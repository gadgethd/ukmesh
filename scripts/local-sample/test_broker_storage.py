"""Static storage ownership; graceful preservation is verified by the native proof."""
from pathlib import Path
import unittest
import importlib.util
import yaml

ROOT=Path(__file__).resolve().parents[2]


def settings():
    values={}
    for line in (ROOT/'scripts/local-sample/mosquitto-test.conf').read_text().splitlines():
        line=line.strip()
        if not line or line.startswith('#'):continue
        key,_,value=line.partition(' ')
        values.setdefault(key,[]).append(value)
    return values


class BrokerStorage(unittest.TestCase):
    def test_shared_broker_is_persistent_and_keeps_queue_listener_bounds(self):
        values=settings()
        for key,want in {'persistence':['true'],'persistence_location':['/mosquitto/data/'],
                         'autosave_interval':['30'],'max_queued_messages':['10000'],
                         'max_queued_bytes':['16777216'],'listener':['1883','9001'],
                         'protocol':['mqtt','websockets']}.items():self.assertEqual(values[key],want,key)

    def test_existing_finite_initializer_owns_saved_storage_without_deleting_it(self):
        cfg=yaml.safe_load((ROOT/'scripts/local-sample/compose.yml').read_text())
        init,broker=cfg['services']['mqtt-log-init'],cfg['services']['mqtt']
        self.assertEqual(init['restart'],'no');self.assertEqual(init['network_mode'],'none')
        self.assertEqual(init['user'],'0:0')
        mount='sample-mqtt-data:/mosquitto/data'
        self.assertIn(mount,init['volumes']);self.assertIn(mount,broker['volumes'])
        self.assertIn('sample-mqtt-data',cfg['volumes'])
        command=init['command'][0]
        self.assertIn('chown 1883:1883 /mosquitto/data',command)
        self.assertIn('chmod 0750 /mosquitto/data',command)
        self.assertNotIn('rm ',command);self.assertNotIn('> /mosquitto/data',command)
        self.assertEqual(broker['user'],'1883:1883')
        self.assertEqual(broker['entrypoint'],['/bin/sh','/dev-tools/mosquitto-lifecycle.sh'])
        self.assertIn('sample-mqtt-control:/mosquitto/control',broker['volumes'])
        self.assertEqual(broker['depends_on']['mqtt-log-init']['condition'],'service_completed_successfully')
        self.assertEqual(broker['mem_limit'],broker['memswap_limit'])


class BrokerEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('broker_persistence_proof',Path(__file__).with_name('broker-persistence-proof.py'))
        cls.proof=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.proof)

    def inspected(self,bindings):
        return {'NetworkSettings':{'Ports':{'1883/tcp':bindings}}}

    def test_each_fresh_inspection_owns_its_current_endpoint_and_evidence(self):
        first=[{'HostIp':'127.0.0.1','HostPort':'32792'}]
        second=[{'HostIp':'127.0.0.1','HostPort':'32793'}]
        self.assertEqual(self.proof.endpoint(self.inspected(first)),('mqtt://127.0.0.1:32792',first))
        self.assertEqual(self.proof.endpoint(self.inspected(second)),('mqtt://127.0.0.1:32793',second))
        self.assertEqual(first[0]['HostPort'],'32792')

    def test_missing_multiple_external_or_malformed_bindings_refuse(self):
        for bindings in (None,[],[{'HostIp':'0.0.0.0','HostPort':'32792'}],
                         [{'HostIp':'127.0.0.1','HostPort':'32792'}]*2):
            with self.subTest(bindings=bindings),self.assertRaises(ValueError):self.proof.endpoint(self.inspected(bindings))
        for port in ('0','80','1024','65536','032792','32792\n',32792,None):
            with self.subTest(port=port),self.assertRaises(ValueError):
                self.proof.endpoint(self.inspected([{'HostIp':'127.0.0.1','HostPort':port}]))


if __name__=='__main__':unittest.main()
