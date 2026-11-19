"""Unchanged local wiring keeps its inode; changed wiring advances its revision."""
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest
import yaml


class WiringPublication(unittest.TestCase):
    def test_local_substitutions_preserve_policy_and_only_replace_changed_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tool = root / 'scripts/full-dev/wiring.py'
            tool.parent.mkdir(parents=True)
            shutil.copyfile(Path(__file__).with_name('wiring.py'), tool)
            (root / 'logging').mkdir()
            (root / 'logging/prometheus.yml').write_text('targets: [mosquitto:9001, backend:9091]\n')
            (root / 'rf-coverage').mkdir()
            policy = {'precision': {'chunk_mib': 128, 'resolution': 30},
                      'gpu': {'mode': 'remote', 'remote': {'broker_addr': '127.0.0.1:8081'}},
                      'remote_worker': {'token': ''}}
            (root / 'rf-coverage/config.ukmesh.yaml').write_text(yaml.safe_dump(policy))
            for name in ('app', 'website'):
                (root / ('nginx.' + name + '.conf')).write_text('real_ip $http_cf_connecting_ip;\n')
            subprocess.run(['python3', str(tool)], check=True)
            state = root / 'plans/lean-runtime/runtime'
            first = {path.name: (path.stat().st_ino, path.read_text()) for path in state.iterdir()}
            subprocess.run(['python3', str(tool)], check=True)
            self.assertEqual({path.name: (path.stat().st_ino, path.read_text()) for path in state.iterdir()}, first)
            (root / 'nginx.website.conf').write_text('changed $http_cf_connecting_ip;\n')
            subprocess.run(['python3', str(tool)], check=True)
            for name, (inode, _) in first.items():
                if name == 'nginx.website.conf':
                    self.assertNotEqual((state / name).stat().st_ino, inode)
                else:
                    self.assertEqual((state / name).stat().st_ino, inode)
            self.assertEqual((state / 'nginx.website.conf').read_text(), 'changed $http_x_real_ip;\n')
            self.assertIn('ukmesh-sample-mqtt:9001', (state / 'prometheus.yml').read_text())
            rendered = yaml.safe_load((state / 'hopreach.yaml').read_text())
            self.assertEqual(rendered['precision'], policy['precision'])
            self.assertEqual(rendered['gpu'], policy['gpu'])
            self.assertEqual(rendered['remote_worker']['token'], '')
            self.assertEqual(stat.S_IMODE((state / 'hopreach.yaml').stat().st_mode), 0o600)
            self.assertEqual(yaml.safe_load((root / 'rf-coverage/config.ukmesh.yaml').read_text()), policy)
            token = state / 'hopreach-worker.token'
            token.write_text('ab' * 32 + '\n')
            token.chmod(0o640)
            denied = subprocess.run(['python3', str(tool)], capture_output=True, text=True)
            self.assertNotEqual(denied.returncode, 0)
            self.assertNotIn('ab' * 32, denied.stderr)
            self.assertEqual(yaml.safe_load((state / 'hopreach.yaml').read_text()), rendered)
            token.chmod(0o600)
            subprocess.run(['python3', str(tool)], check=True)
            self.assertEqual(yaml.safe_load((state / 'hopreach.yaml').read_text())['remote_worker']['token'], 'ab' * 32)
            self.assertEqual(stat.S_IMODE((state / 'hopreach.yaml').stat().st_mode), 0o600)


if __name__ == '__main__':
    unittest.main()
