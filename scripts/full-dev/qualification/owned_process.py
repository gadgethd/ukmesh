"""Linux process-session ownership for local qualification commands."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


_CANCELLATION_SIGNALS = {signal.SIGTERM, signal.SIGINT}


@contextmanager
def cancellation_scope(subject):
    """Latch the first cancellation until mandatory settlement and evidence are saved."""
    state = {'signal': None, 'on_cancel': None}

    def latch(signum, _frame):
        if state['signal'] is None:
            state['signal'] = signum

    blocked = signal.pthread_sigmask(signal.SIG_BLOCK, _CANCELLATION_SIGNALS)
    handlers = {}
    try:
        for sig in _CANCELLATION_SIGNALS:
            handlers[sig] = signal.signal(sig, latch)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
    failed = False
    try:
        yield state
    except InterruptedError:
        raise
    except BaseException:
        failed = True
        raise
    finally:
        # Close the latch-check/handler-restore race. A signal that becomes
        # pending after this block reaches the restored outer/default handler.
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, _CANCELLATION_SIGNALS)
        try:
            if state['signal'] is not None:
                if state['on_cancel'] is not None:
                    state['on_cancel']()
                if not failed:
                    raise InterruptedError(subject + ' canceled by ' + signal.Signals(state['signal']).name)
        finally:
            try:
                for sig, handler in handlers.items():
                    signal.signal(sig, handler)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, blocked)


def session_members(pid):
    result = []
    for path in Path('/proc').iterdir():
        if not path.name.isdecimal():
            continue
        try:
            fields = (path / 'stat').read_text().rsplit(')', 1)[1].split()
            if int(fields[2]) == pid and int(fields[3]) == pid and fields[0] != 'Z':
                result.append(int(fields[19]))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return result


def settle_session(child, ticks, *, graceful_interrupt=0):
    try:
        # The leader is kept unreaped by waitid(WNOWAIT), reserving its PID/PGID.
        steps = ((signal.SIGTERM, .2), (signal.SIGKILL, .5))
        if graceful_interrupt:
            steps = ((signal.SIGINT, graceful_interrupt), *steps)
        for sig, grace in steps:
            leader = Path(f'/proc/{child.pid}/stat')
            if ticks is None or not leader.exists() or int(leader.read_text().rsplit(')', 1)[1].split()[19]) != ticks:
                raise RuntimeError('owned process leader identity unavailable')
            members = session_members(child.pid)
            if not members:
                break
            if any(value < ticks for value in members):
                raise RuntimeError('process group identity differs')
            try:
                os.killpg(child.pid, sig)
            except ProcessLookupError:
                pass
            end = time.monotonic() + grace
            while session_members(child.pid) and time.monotonic() < end:
                time.sleep(.01)
        if session_members(child.pid):
            raise RuntimeError('owned process group not settled')
    finally:
        # Even an identity refusal must reap our direct child. Popen.kill polls
        # its own waitpid state first, so it does not signal an already reaped PID.
        if child.returncode is None:
            child.kill()
        child.wait(timeout=1)


def spawn_session(argv, *, spawn=subprocess.Popen, **options):
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, _CANCELLATION_SIGNALS)
    child = None
    try:
        # Unmask before exec without a preexec_fn in the parent.
        setup = ('import os, signal, sys\n'
                 'signal.pthread_sigmask(signal.SIG_SETMASK, ())\n'
                 'os.execvpe(sys.argv[1], sys.argv[1:], os.environ)\n')
        child = spawn([sys.executable, '-c', setup, *argv], start_new_session=True, **options)
        fields = Path(f'/proc/{child.pid}/stat').read_text().rsplit(')', 1)[1].split()
        if int(fields[2]) != child.pid or int(fields[3]) != child.pid:
            raise RuntimeError('owned process session differs')
        return child, int(fields[19])
    except BaseException:
        if child is not None:
            child.kill()
            child.wait(timeout=1)
        raise
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def run_session(argv, *, cancellation, stop_reason=None, **options):
    """Wait for the invocation and settle its children before returning."""
    if cancellation['signal'] is not None:
        raise InterruptedError('owned process session canceled before spawn')
    child, ticks = spawn_session(argv, **options)
    interrupt = False
    try:
        while os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
            if cancellation['signal'] is not None:
                interrupt = True
                break
            if stop_reason and (reason := stop_reason()):
                interrupt = True
                raise InterruptedError(reason)
            time.sleep(.05)
    finally:
        settle_session(child, ticks, graceful_interrupt=5 if interrupt else 0)
    if stop_reason and (reason := stop_reason()):
        raise InterruptedError(reason)
    return child.returncode
