"""One inherited target lease; environment flags alone never prove ownership."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import re
import stat

LOCK=Path(__file__).resolve().parents[3]/'plans/lean-runtime/runtime/target-transition.lock'
ENV='UKMESH_TARGET_LEASE_FD'


def validate_fd(fd,*,exclusive=False,path=LOCK):
    if path.resolve()!=path.absolute():raise ValueError('target lease path must be canonical')
    if type(fd) is not int or fd<3:raise ValueError('invalid inherited target lease')
    info=os.fstat(fd);named=path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(named.st_mode) or info.st_uid!=os.geteuid() \
            or stat.S_IMODE(info.st_mode)!=0o600 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino):
        raise ValueError('inherited target lease identity differs')
    modes=[]
    for line in Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines():
        if line.startswith('lock:'):
            fields=line.split()
            if len(fields)>=6 and fields[2]=='FLOCK' and fields[3]=='ADVISORY' and fields[4] in ('READ','WRITE'):
                modes.append(fields[4])
    if len(modes)!=1 or (exclusive and modes[0]!='WRITE'):raise ValueError('required target lease is not held by this descriptor')
    return modes[0]


def child_context(*,path=LOCK):
    value=os.environ.get(ENV)
    if value is None:return {},()
    if not re.fullmatch(r'[0-9]+',value):raise ValueError('invalid target lease environment')
    fd=int(value);validate_fd(fd,path=path)
    return {ENV:str(fd)},(fd,)


@contextmanager
def target_lease(*,exclusive=False,path=LOCK):
    if path.resolve()!=path.absolute():raise ValueError('target lease path must be canonical')
    inherited=os.environ.get(ENV);owned=inherited is None
    previous=inherited
    if owned:
        path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600:
                raise ValueError('target lease file ownership differs')
            fcntl.flock(fd,(fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)|fcntl.LOCK_NB)
        except BaseException:
            os.close(fd);raise
    else:
        if not re.fullmatch(r'[0-9]+',inherited):raise ValueError('invalid inherited target lease environment')
        fd=int(inherited);validate_fd(fd,exclusive=exclusive,path=path)
    os.environ[ENV]=str(fd)
    try:yield fd
    finally:
        if previous is None:os.environ.pop(ENV,None)
        else:os.environ[ENV]=previous
        if owned:os.close(fd)
