"""Real local flock/FD inheritance only; no runtime, HTTP or database actions."""
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from target_lease import ENV,target_lease,validate_fd,child_context


class TargetLease(unittest.TestCase):
    def test_reentrant_child_mode_never_upgrades_or_releases_parent(self):
        with TemporaryDirectory() as temporary,patch.dict(os.environ,{},clear=True):
            path=Path(temporary)/'lock'
            with target_lease(exclusive=True,path=path) as fd:
                self.assertEqual(validate_fd(fd,exclusive=True,path=path),'WRITE')
                with target_lease(path=path) as borrowed:self.assertEqual(borrowed,fd)
                self.assertEqual(validate_fd(fd,exclusive=True,path=path),'WRITE')
            self.assertNotIn(ENV,os.environ)
            with target_lease(path=path) as fd:
                with self.assertRaises(ValueError):
                    with target_lease(exclusive=True,path=path):pass
                self.assertEqual(validate_fd(fd,path=path),'READ')

    def test_env_alone_unlocked_descriptor_foreign_path_and_symlink_refuse(self):
        with TemporaryDirectory() as temporary,patch.dict(os.environ,{},clear=True):
            path=Path(temporary)/'lock'
            with target_lease(path=path):
                fd=os.open(path,os.O_RDWR)
                try:
                    with patch.dict(os.environ,{ENV:str(fd)}):
                        with self.assertRaises(ValueError):child_context(path=path)
                finally:os.close(fd)
            with patch.dict(os.environ,{ENV:'fake'}):
                with self.assertRaises(ValueError):child_context(path=path)
            alias=Path(temporary)/'alias';alias.symlink_to(path)
            with self.assertRaises(ValueError):
                with target_lease(path=alias):pass
            with target_lease(path=path) as fd:
                wrong=Path(temporary)/'foreign';wrong.touch(mode=0o600)
                with self.assertRaises(ValueError):validate_fd(fd,path=wrong)

    def test_real_descendant_retains_shared_lease_after_parent_scope_until_exit(self):
        with TemporaryDirectory() as temporary,patch.dict(os.environ,{},clear=True):
            path=Path(temporary)/'lock';child=None
            code="from pathlib import Path;import sys,os;from target_lease import validate_fd;validate_fd(int(os.environ['UKMESH_TARGET_LEASE_FD']),path=Path(sys.argv[1]));print('ready',flush=True);sys.stdin.readline()"
            try:
                with target_lease(path=path) as fd:
                    env,handles=child_context(path=path)
                    child=subprocess.Popen([sys.executable,'-c',code,str(path)],env={**env,'PYTHONPATH':str(Path(__file__).parent.resolve())},
                        pass_fds=handles,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                    self.assertEqual(child.stdout.readline(),'ready\n')
                with self.assertRaises(BlockingIOError):
                    with target_lease(exclusive=True,path=path):pass
                child.stdin.write('\n');child.stdin.flush();self.assertEqual(child.wait(timeout=3),0)
                with target_lease(exclusive=True,path=path):pass
            finally:
                if child and child.poll() is None:child.kill();child.wait(timeout=3)
                if child:
                    for stream in (child.stdin,child.stdout,child.stderr):stream.close()


if __name__=='__main__':unittest.main()
