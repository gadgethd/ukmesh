"""Actual shell/clean-env/flock/FD chain with a synthetic controller, no Docker."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from copy import deepcopy
from unittest.mock import patch
from target_lease import target_lease,child_context,validate_fd,ENV
import target_entry


template=(target_entry.REPO/'scripts/dev-all.sh').read_text()
HOOK=template[template.index('entry_env='):template.index('if [[ ${UKMESH_COMPANION_ENV_CLEAN')].replace('"$repo/scripts/dev-all.sh"','"$0"')



class TargetEntry(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name);self.tools=self.root/'scripts/full-dev/qualification';self.tools.mkdir(parents=True)
        for name in ('target_entry.py','target_lease.py'):
            shutil.copyfile(Path(target_entry.__file__).with_name(name),self.tools/name)
        (self.tools/'common.py').write_text("from pathlib import Path\nimport os\nREPO=Path(__file__).resolve().parents[3]\ndef clean_environment():return {'PATH':os.defpath,'HOME':'/tmp','LANG':'C.UTF-8'}\n")
        (self.tools/'fixtures.py').write_text("def selection_values():return {}\n")
        (self.tools/'target_safety.py').write_text("def inspect_snapshot():return {'consumers':{}}\ndef changing(*args):return set()\ndef target_values(v):return v\ndef read_intent(*args):return None\ndef check_start(*args,**kw):return {}\ndef mark_complete(*args):return {}\n")
        self.script=self.root/'scripts/dev-all.sh'
        self.script.write_text('''#!/bin/bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "$0")/.." && pwd)
'''+HOOK+'''
if [[ ${1:-} == --help ]]; then echo offline-help;exit 0;fi
if [[ ${UKMESH_COMPANION_LOCKED:-} != 1 ]]; then
 mkdir -p "$repo/plans/companion-dev"
 exec flock --nonblock --close "$repo/plans/companion-dev/control.lock" env UKMESH_COMPANION_LOCKED=1 bash "$0" "$@"
fi
bash "$repo/scripts/full-dev/control.sh" "$@"
''')
        self.nested=self.root/'scripts/full-dev/control.sh'
        self.nested.write_text('''#!/bin/bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "$0")/../.." && pwd)
'''+HOOK+'''
python3 - "$repo" <<'CHILD'
import json,sys,os
sys.path.insert(0,sys.argv[1]+'/scripts/full-dev/qualification')
from target_lease import validate_fd,ENV
print(json.dumps({'mode':validate_fd(int(os.environ[ENV])), 'keys':sorted(os.environ),
                  'locked':os.environ.get('UKMESH_COMPANION_LOCKED')}))
CHILD
''')

    def test_real_reentrant_clean_shell_inherits_actual_fd_and_controller_lock(self):
        poison=self.root/'poison';poison.mkdir();(poison/'json.py').write_text("raise RuntimeError('caller import environment survived')\n")
        env={**os.environ,'PYTHONPATH':str(poison),'UNRELATED_SECRET':'not-in-child','UKMESH_COMPANION_LOCKED':'1','UKMESH_COMPANION_ENV_CLEAN':'1'}
        result=subprocess.run(['bash',str(self.script),'start'],env=env,capture_output=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        data=json.loads(result.stdout);self.assertEqual(data['mode'],'READ');self.assertEqual(data['locked'],'1')
        self.assertNotIn('UNRELATED_SECRET',data['keys']);self.assertNotIn('PYTHONPATH',data['keys'])

    def test_offline_help_does_not_inspect_or_require_selected_app_stores(self):
        with (self.tools/'target_safety.py').open('a') as stream:
            stream.write("def inspect_snapshot():raise RuntimeError('offline no applications')\n")
        (self.tools/'fixtures.py').write_text("def selection_values():raise RuntimeError('no selected stores for help')\n")
        result=subprocess.run(['bash',str(self.script),'--help'],env=os.environ,capture_output=True,timeout=3)
        self.assertEqual(result.returncode,0,result.stderr.decode());self.assertEqual(result.stdout,b'offline-help\n')

    def test_external_same_store_restart_shares_publisher_but_change_refuses(self):
        lock=self.root/'lease';publisher=None
        values={'UKMESH_DATABASE':'old','OWNER_DATABASE':'owner','BEACON_DATABASE':'beacon','CORESCOPE_DATA_DIR':'/core'}
        actual={'consumers':{'ukmesh-full-backend-1':{'refs':{'ukmesh':'old'}}}}
        try:
            with target_lease(path=lock):
                env,handles=child_context(path=lock)
                publisher=subprocess.Popen([os.sys.executable,'-c',"print('ready',flush=True);input()"],
                    env=env,pass_fds=handles,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                self.assertEqual(publisher.stdout.readline(),'ready\n')
            # External operation, independent of the publisher's process tree.
            script=str(target_entry.REPO/'scripts/full-dev/control.sh')
            with target_entry.controller_lease(script,['restart','backend'],values_reader=lambda:values,
                    snapshot_reader=lambda:actual,path=lock):
                self.assertEqual(validate_fd(int(os.environ[ENV]),path=lock),'READ')
            with self.assertRaises(BlockingIOError):
                with target_entry.controller_lease(script,['restart','backend'],values_reader=lambda:{**values,'UKMESH_DATABASE':'new'},
                        snapshot_reader=lambda:actual,path=lock):self.fail('changed stores admitted')
        finally:
            if publisher:
                if publisher.poll() is None:publisher.communicate('\n',timeout=3)
                for stream in (publisher.stdin,publisher.stdout,publisher.stderr):stream.close()

    def test_exclusive_reacquisition_rereads_and_restart_scopes_actual_dependencies(self):
        lock=self.root/'lease';reads=[]
        values={'UKMESH_DATABASE':'new','OWNER_DATABASE':'owner','BEACON_DATABASE':'beacon','CORESCOPE_DATA_DIR':'/core'}
        actual={'consumers':{'ukmesh-full-backend-1':{'refs':{'ukmesh':'old'}}}}
        script=str(target_entry.REPO/'scripts/full-dev/control.sh')
        def selected():reads.append(validate_fd(int(os.environ[ENV]),path=lock));return values
        with target_entry.controller_lease(script,['restart','backend'],values_reader=selected,
                snapshot_reader=lambda:actual,path=lock):
            self.assertEqual(validate_fd(int(os.environ[ENV]),path=lock),'WRITE')
        self.assertEqual(reads,['READ','WRITE'])
        names=['ukmesh-full-backend-1','ukmesh-full-tagger-worker-1','ukmesh-companions-beacon-1',
               'ukmesh-companions-beacon-migrate-1','ukmesh-companions-beacon-db-1','ukmesh-companions-beacon-redis-1',
               'ukmesh-companions-corescope-1','ukmesh-companions-corescope-ingestor-1']
        snap={'consumers':dict.fromkeys(names,{})}
        self.assertEqual(target_entry.scope_names(snap,script,['restart','backend']),{'ukmesh-full-backend-1'})
        root=str(target_entry.REPO/'scripts/dev-all.sh')
        self.assertEqual(target_entry.scope_names(snap,root,['restart','beacon']),set(names[2:6]))
        self.assertEqual(target_entry.scope_names(snap,root,['restart','corescope']),set(names[6:]))

    def test_env_frame_or_fd_without_live_ancestor_never_bypasses_entry(self):
        env={**os.environ,'UKMESH_TARGET_ENTRY':json.dumps({'pid':os.getpid(),'ticks':0,'script':str(self.script),'args':['start']})}
        result=subprocess.run(['bash',str(self.script),'start'],env=env,capture_output=True,timeout=3)
        self.assertEqual(result.returncode,2);self.assertEqual(result.stdout,b'')
        env={**os.environ,'UKMESH_TARGET_LEASE_FD':'99999'}
        result=subprocess.run(['bash',str(self.script),'start'],env=env,capture_output=True,timeout=3)
        self.assertEqual(result.returncode,2);self.assertEqual(result.stdout,b'')


if __name__=='__main__':unittest.main()
