"""Select an owned fixture for one qualification phase and restore the original stores."""

from contextlib import ExitStack, contextmanager
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time

from common import (MIN_FREE_BYTES, REPO, STATE, clean_environment, docker_group_command,
                    owned_run, read_json, safe_error, utc_now, write_json)
from fixtures import bundle, original_values, restore_originals, selection_files, selection_values, switch
from owned_process import cancellation_scope, run_session
from read_transport import recover_reader
from target_lease import child_context, target_lease
from target_observation import IntakeNotQuiescentError
from target_safety import InactiveChangingConsumerError, read_intent


@contextmanager
def restore_lease():
    """A nested readiness probe may still be releasing its inherited descriptor."""
    deadline = time.monotonic() + 60
    with ExitStack() as leases:
        while True:
            try:
                leases.enter_context(target_lease(exclusive=True))
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('target lease did not become available for canonical restoration')
                time.sleep(.2)
        yield


def run_controller(script: str, args: list[str], cancellation: dict, **options) -> int:
    inherited, descriptors = child_context()
    command = docker_group_command(['bash', str(REPO / script), *args])
    return run_session(command, cwd=REPO,
                       env={**clean_environment(), **inherited}, pass_fds=descriptors,
                       cancellation=cancellation, **options)


def require_running(cancellation: dict) -> None:
    if cancellation['signal'] is not None:
        raise InterruptedError('fixture session was interrupted')


def restore_when_quiescent() -> None:
    # An interrupted publisher can leave already accepted traffic to commit.
    # Start a fresh protected observation while the owned lease is held.
    # An attempt started before the deadline keeps its own proof deadline.
    deadline = time.monotonic() + 120
    while True:
        try:
            restore_originals()
            return
        except IntakeNotQuiescentError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(1, remaining))
            if time.monotonic() >= deadline:
                raise


def restore(evidence: Path, token: str) -> None:
    # Possible cancellation is latched by the outer scope during recovery.
    # The canonical controller and its fresh readiness probes finish normally.
    recovery = {'signal': None}
    with restore_lease():
        resumed_at = None
        intent = read_intent()
        if intent and intent['status'] == 'pending':
            if intent['requested_selection'].get('token') != token:
                raise ValueError('the pending target does not belong to this fixture session')
            switch(token)
            status = run_controller('scripts/dev-all.sh', ['start'], recovery)
            if status:
                raise RuntimeError(f'canonical pending fixture start could not be resumed (exit {status}); '
                                   'resume this fixture through dev-all before restoring the original stores')
        selection = selection_files()['selection']
        on_originals = selection.get('targets') == 'original'
        if not on_originals and selection.get('token') != token:
            raise ValueError('selected fixture changed during this session')
        try:
            restore_when_quiescent()
        except InactiveChangingConsumerError:
            if on_originals:
                raise
            # Selection has not been published. Resume the owned fixture so
            # the target guard can observe the consumer before switching.
            status = run_controller('scripts/dev-all.sh', ['start'], recovery)
            if status:
                raise RuntimeError(f'canonical fixture restart before restoration failed (exit {status})')
            resumed_at = utc_now()
            restore_when_quiescent()
        if run_controller('scripts/dev-all.sh', ['start'], recovery):
            raise RuntimeError('canonical original-store start failed')
        if selection_values() != original_values():
            raise RuntimeError('canonical original-store selection differs')
        inventory = evidence / 'restored'
        inventory.mkdir(mode=0o700)
        if run_controller('scripts/qualify-full-stack.sh', ['inventory', '--run', str(inventory)], recovery):
            raise RuntimeError('restored inventory failed')
        if resumed_at is not None:
            write_json(evidence / 'resume-fixture.json', {'at': resumed_at, 'fixture': token})


def restore_current(run: Path) -> None:
    from fixture_resume import prepare_applied_fixture

    with cancellation_scope('interrupted fixture recovery') as cancellation, restore_lease():
        receipt = {'at': utc_now(), 'restored': False}

        def record_cancel():
            receipt['cancellation'] = signal.Signals(cancellation['signal']).name
            write_json(run / 'interrupted-recovery.json', receipt)

        cancellation['on_cancel'] = record_cancel
        try:
            token = prepare_applied_fixture(run)
            receipt['fixture'] = token
            restore(run, token)
        except Exception as error:
            receipt['error'] = safe_error(error)
            raise
        else:
            receipt.update(restored=True, inventory=str(run / 'restored' / 'inventory.json'))
        finally:
            write_json(run / 'interrupted-recovery.json', receipt)


def qualify(token: str, run: Path, args: list[str]) -> int:
    if not args or args[0] not in ('access', 'core-projection', 'operations', 'parity',
                                 'reads', 'recovery', 'sample', 'smoke', 'soak', 'traffic'):
        raise ValueError('select one canonical owned fixture workload')
    if any(option == '--run' or option.startswith('--run=') for option in args[1:]):
        raise ValueError('the fixture session owns the qualification evidence path')
    with cancellation_scope('fixture session') as cancellation:
        evidence = None
        disk_problem = None

        def observe_disk():
            nonlocal disk_problem
            if disk_problem is not None:
                return
            try:
                available = shutil.disk_usage(REPO).free
            except OSError as error:
                disk_problem = {'at': utc_now(), 'error': safe_error(error)}
            else:
                if available < MIN_FREE_BYTES:
                    disk_problem = {'at': utc_now(), 'free_bytes': available}

        def disk_cutoff():
            observe_disk()
            if disk_problem:
                write_json(evidence / 'disk-cutoff.json', {**disk_problem, 'reserve_bytes': MIN_FREE_BYTES,
                                                          'fixture': token})
                return 'fixture disk reserve reached' if 'free_bytes' in disk_problem else 'fixture disk reserve unavailable'

        try:
            with target_lease(exclusive=True):
                fixture = bundle(token)
                if fixture['kind'] not in ('1x', '10x') or selection_values() != original_values():
                    raise ValueError('the fixture session requires original stores and an owned lookup fixture')
                intent = read_intent()
                if intent and intent['status'] == 'pending':
                    raise ValueError('finish the existing canonical target transition before qualifying')
                if shutil.disk_usage(REPO).free < MIN_FREE_BYTES:
                    raise ValueError('disk reserve is unavailable before fixture startup')
                if args[0] == 'soak' and (run / 'http-reader').exists():
                    raise ValueError('a fresh soak HTTP reader directory is required')
                require_running(cancellation)
                sessions = run / 'fixture-sessions'
                sessions.mkdir(mode=0o700, exist_ok=True)
                if sessions.resolve().parent != run.resolve() or sessions.is_symlink():
                    raise ValueError('fixture session evidence is outside the owned run')
                evidence = Path(tempfile.mkdtemp(prefix=args[0] + '-', dir=sessions))
                write_json(evidence / 'started.json', {'at': utc_now(), 'fixture': token, 'workload': args[0]})
                switch(token)
                # Record potential reserve loss in memory until the pending start settles.
                status = run_controller('scripts/dev-all.sh', ['start'], {'signal': None}, stop_reason=observe_disk)
                if status:
                    disk_cutoff()
                    raise RuntimeError(f'canonical fixture start failed (exit {status})')
            if (reason := disk_cutoff()):
                raise InterruptedError(reason)
            require_running(cancellation)

            with target_lease():
                selected = read_json(STATE / 'runtime/selected-fixture.json')
                if selected.get('token') != token:
                    raise ValueError('selected fixture changed before workload startup')
                return run_controller('scripts/qualify-full-stack.sh', [*args, '--run', str(run)],
                                      cancellation, stop_reason=disk_cutoff)
        finally:
            if evidence is not None:
                try:
                    if args[0] == 'soak':
                        recover_reader(run)
                finally:
                    receipt = {'at': utc_now(), 'fixture': token, 'restored': False}
                    try:
                        restore(evidence, token)
                    except Exception as error:
                        receipt['error'] = safe_error(error)
                        raise
                    else:
                        receipt.update(restored=True, inventory=str(evidence / 'restored/inventory.json'))
                    finally:
                        if cancellation['signal'] is not None:
                            receipt['cancellation'] = signal.Signals(cancellation['signal']).name
                        write_json(evidence / 'fixture-session.json', receipt)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == 'restore-current':
        try:
            restore_current(owned_run(sys.argv[2]))
        except InterruptedError as error:
            print(str(error), file=sys.stderr)
            raise SystemExit(130)
        raise SystemExit(0)
    if len(sys.argv) < 4:
        raise SystemExit('usage: fixture_session.py TOKEN EVIDENCE_DIR ACTION [OPTIONS] | restore-current EVIDENCE_DIR')
    try:
        raise SystemExit(qualify(sys.argv[1], owned_run(sys.argv[2]), sys.argv[3:]))
    except InterruptedError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(130)
