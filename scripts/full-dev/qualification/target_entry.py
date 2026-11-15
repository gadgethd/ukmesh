#!/usr/bin/env python3
"""Controller lease boundary. Fresh intake checks belong immediately before apply."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys

from common import REPO,clean_environment
from target_lease import target_lease,child_context,validate_fd,ENV
from target_safety import changing,inspect_snapshot,target_values,check_start,mark_complete

FRAME='UKMESH_TARGET_ENTRY'
SCRIPTS={str(REPO/p):scope for p,scope in (
    ('scripts/dev-all.sh',None),('scripts/full-dev/control.sh','ukmesh-full-'),
    ('scripts/companion-dev/control.sh','ukmesh-companions-'),('scripts/qualify-full-stack.sh',None))}


def process(pid):
    root=Path('/proc')/str(pid)
    raw=(root/'stat').read_text();fields=raw[raw.rfind(')')+2:].split()
    return {'parent':int(fields[1]),'ticks':int(fields[19]),
            'argv':[os.fsdecode(v) for v in (root/'cmdline').read_bytes().split(b'\0') if v]}


def entry_frame():
    value=os.environ.get(FRAME)
    if value is None:return None
    frame=json.loads(value)
    if set(frame)!={'pid','ticks','script','args'} or frame['script'] not in SCRIPTS \
            or type(frame['pid']) is not int or type(frame['ticks']) is not int or not isinstance(frame['args'],list):
        raise ValueError('invalid inherited target entry')
    owner=process(frame['pid'])
    expected=[str(Path(__file__).resolve()),'run',frame['script'],*frame['args']]
    if owner['ticks']!=frame['ticks'] or owner['argv'][1:]!=expected:
        raise ValueError('target entry owner identity differs')
    pid=os.getpid();ancestors=set()
    while pid>1 and pid not in ancestors:
        ancestors.add(pid);pid=process(pid)['parent']
    if frame['pid'] not in ancestors:raise ValueError('target entry is not an owned ancestor')
    child_context()  # A PID/flag without the actual inherited FLOCK is insufficient.
    if ENV not in os.environ:raise ValueError('target entry lost its lease')
    return frame


def companion_lock_held():
    path=REPO/'plans/companion-dev/control.lock'
    try:named=path.stat()
    except FileNotFoundError:return False
    pid=os.getppid();seen=set()
    while pid>1 and pid not in seen:
        seen.add(pid);root=Path('/proc')/str(pid)
        try:
            for fd in (root/'fd').iterdir():
                try:
                    info=fd.stat()
                    if (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino):continue
                    lines=(root/'fdinfo'/fd.name).read_text().splitlines()
                    if any(' FLOCK ' in line and ' WRITE ' in line for line in lines):return True
                except (FileNotFoundError,PermissionError):continue
            pid=process(pid)['parent']
        except (FileNotFoundError,PermissionError):return False
    return False


def scope_names(snapshot,script,args=()):
    action=args[0] if args else 'start';service=args[1] if len(args)>1 else ''
    prefix=SCRIPTS[script]
    names={name for name in snapshot['consumers'] if prefix is None or name.startswith(prefix)}
    if action=='companions':names={n for n in names if n.startswith('ukmesh-companions-')}
    if action in ('restart','hot') and service:
        services={service}
        if service in ('corescope','corescope-ingestor'):services={'corescope','corescope-ingestor'}
        if script!=str(REPO/'scripts/full-dev/control.sh') and service.startswith('beacon') and service!='beacon-web':
            services.update(('beacon-migrate','beacon-db','beacon-redis'))
        names={name for name in names if any(name.endswith('-'+item+'-1') for item in services)}
    return names


def guard(scope,action='start',service=''):
    from fixtures import selection_values
    if scope not in ('main','companions'):raise ValueError('unknown apply scope')
    script=str(REPO/('scripts/full-dev/control.sh' if scope=='main' else 'scripts/companion-dev/control.sh'))
    actual=inspect_snapshot();names=scope_names(actual,script,[action,service])
    values=selection_values()
    if changing(actual,target_values(values),names):validate_fd(int(os.environ[ENV]),exclusive=True)
    return check_start(values,scope=names)


@contextmanager
def controller_lease(script,args,*,values_reader=None,snapshot_reader=inspect_snapshot,path=None):
    from fixtures import selection_values
    values_reader=values_reader or selection_values
    mutating=(args[0] if args else 'start') in ('start','companions','restart','hot','stop')
    inherited=ENV in os.environ;need_exclusive=False
    options={} if path is None else {'path':path}
    def inspect():
        if not mutating:return None,False
        values=values_reader();actual=snapshot_reader()
        return values,bool(changing(actual,target_values(values),scope_names(actual,script,args)))
    # The shared read/decision happens before the controller lock. Never
    # upgrade a held descriptor: release it, take EX, and reread both authorities.
    with target_lease(**options):
        values,moved=inspect()
        if not moved:yield values;return
        if inherited:
            validate_fd(int(os.environ[ENV]),exclusive=True,**options)
            yield values;return
        need_exclusive=True
    if need_exclusive:
        with target_lease(exclusive=True,**options):
            values,_=inspect()
            yield values


def run(script,args):
    if script not in SCRIPTS:raise ValueError('unknown controller entry')
    entry_frame()  # If inherited, refuse forged or stale frames before borrowing the lease.
    with controller_lease(script,args) as values:
        inherited,fds=child_context()
        env={**clean_environment(),**inherited,'UKMESH_COMPANION_ENV_CLEAN':'1'}
        if companion_lock_held():env['UKMESH_COMPANION_LOCKED']='1'
        own=process(os.getpid())
        frame={'pid':os.getpid(),'ticks':own['ticks'],'script':script,'args':args}
        env[FRAME]=json.dumps(frame,separators=(',',':'))
        result=subprocess.run(['bash',script,*args],env=env,pass_fds=fds,cwd=REPO)
        if result.returncode==0 and script==str(REPO/'scripts/dev-all.sh') and (not args or args[0]=='start'):
            mark_complete(values)
        return result.returncode


def main():
    action,*args=sys.argv[1:]
    if action=='verify':
        frame=entry_frame()
        return 0 if frame and frame['script']==args[0] and frame['args']==args[1:] else 1
    if action=='run':return run(args[0],args[1:])
    if action=='guard':
        validate_fd(int(os.environ[ENV]));print(json.dumps(guard(*args)));return 0
    raise ValueError('unknown target entry action')


if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as error:
        # Never emit inspected environments, DSNs, argv or raw application state.
        print('Target protection refused: '+str(error),file=sys.stderr);raise SystemExit(2)
