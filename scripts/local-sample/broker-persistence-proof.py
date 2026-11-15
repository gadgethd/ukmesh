#!/usr/bin/env python3
"""Explicit isolated broker restart proof. Never touches retained broker/volumes."""
from pathlib import Path
import argparse,datetime,hashlib,json,re,sys,uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'full-dev/qualification'))
from common import REPO,command,docker,write_json


def endpoint(inspected: dict) -> tuple[str, list[dict]]:
    """Decode the fresh owned-container binding; never cache an assigned port."""
    binding=inspected['NetworkSettings']['Ports']['1883/tcp']
    if not isinstance(binding,list) or len(binding)!=1 or binding[0].get('HostIp')!='127.0.0.1':
        raise ValueError('one owned loopback broker binding is required')
    port=binding[0].get('HostPort')
    if not isinstance(port,str) or not re.fullmatch(r'[1-9][0-9]{0,4}',port) or not 1024<int(port)<=65535:
        raise ValueError('broker host port is invalid')
    return 'mqtt://127.0.0.1:'+port,binding


def run(output: Path):
    token=uuid.uuid4().hex;project='ukmesh-broker-proof-'+token;name=project+'-mqtt'
    if not output.resolve().is_relative_to(REPO/'plans/lean-runtime/final-evidence') or output.exists():
        raise ValueError('a fresh private final-evidence directory is required')
    output.mkdir(mode=0o700,parents=True)
    tool=Path(__file__).resolve()
    paths=[tool,tool.with_name('broker-persistence-client.mjs'),tool.with_name('compose.yml'),
           tool.with_name('mosquitto-test.conf'),tool.with_name('mosquitto-lifecycle.sh')]
    hashes=lambda:{str(p.relative_to(REPO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    def original_state():
        value=json.loads(docker(['inspect','--format','{{json .}}','ukmesh-sample-mqtt']).stdout)
        return {'id':value['Id'],'started_at':value['State']['StartedAt'],'restarts':value['RestartCount']}
    before=hashes();original=original_state()
    override=output/'override.yml'
    override.write_text('services:\n  mqtt:\n    container_name: '+name+'\n    ports: !override ["127.0.0.1::1883"]\n')
    compose=['compose','--project-name',project,'--env-file','/dev/null','-f',str(Path(__file__).with_name('compose.yml')),'-f',str(override)]
    receipt={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'project':project,'source_before':before,'passed':False,'checks':{},'errors':[],'scope':'Synthetic isolated broker graceful restart only; no power-loss/per-PUBACK-fsync or qualification claim'}
    image=None;admitted=False;helper_label='ukmesh.broker-proof-owner='+token
    try:
        for args in (['ps','-aq'],['volume','ls','-q'],['network','ls','-q']):
            if docker([*args,'--filter','label=com.docker.compose.project='+project]).stdout.strip():raise ValueError('proof project already exists')
        for kind,names in (('container',[name]),('volume',[project+'_'+v for v in ('sample-mqtt-data','sample-mqtt-log','sample-mqtt-control')]),('network',[project+'_default'])):
            for resource in names:
                if docker([kind,'inspect',resource],check=False).returncode==0:raise ValueError('proof resource name already exists')
        admitted=True
        config=json.loads(docker([*compose,'config','--format','json']).stdout)
        image=config['services']['mqtt']['image']
        image_info=json.loads(docker(['image','inspect',image]).stdout)[0]
        assert '/mosquitto/data' in image_info['Config']['Volumes']
        # Exact legacy image default data volume, broker UID; --rm removes its anonymous volume.
        legacy=docker(['run','--rm','--label',helper_label,'--network','none','--memory','32m','--memory-swap','32m','--cpus','.1','--pids-limit','16','--cap-drop','ALL','--security-opt','no-new-privileges','--user','1883:1883','--entrypoint','/bin/sh',image,'-ec','test -d /mosquitto/data; test -w /mosquitto/data; printf owned > /mosquitto/data/ukmesh-proof; test "$(cat /mosquitto/data/ukmesh-proof)" = owned'],timeout=30)
        receipt['checks']['legacy_image_default_data_writable']=legacy.returncode==0
        docker([*compose,'up','-d','--wait','--wait-timeout','30','mqtt'],timeout=60)
        owned=json.loads(docker(['inspect',name]).stdout)[0]
        assert owned['Config']['Labels']['com.docker.compose.project']==project and owned['Config']['Labels']['com.docker.compose.service']=='mqtt'
        data=next(m for m in owned['Mounts'] if m['Destination']=='/mosquitto/data');volume=data['Name']
        assert data['Type']=='volume' and volume==project+'_sample-mqtt-data'
        meta=json.loads(docker(['volume','inspect',volume]).stdout)[0]
        assert meta['Labels']['com.docker.compose.project']==project
        receipt.update(broker_id=owned['Id'],data_volume=volume,image_id=owned['Image'])
        address,binding=endpoint(owned);receipt['initial_binding']=binding
        client=Path(__file__).with_name('broker-persistence-client.mjs')
        prepare=command(['node',str(client),'prepare',address,token],timeout=20)
        receipt['prepare']=json.loads(prepare.stdout)
        assert receipt['prepare']=={'passed':True,'queued_qos1':3,'retained_published':1,'all_clients_closed':True}
        docker([*compose,'stop','--timeout','20','mqtt'],timeout=30)
        stopped=json.loads(docker(['inspect',name]).stdout)[0]
        assert not stopped['State']['Running'] and stopped['State']['ExitCode']==0
        def saved():
            result=docker(['run','--rm','--label',helper_label,'--network','none','--memory','32m','--memory-swap','32m','--cpus','.1','--pids-limit','16','--cap-drop','ALL','--user','1883:1883','-v',volume+':/proof:ro','--entrypoint','/bin/sh',image,'-ec','test -s /proof/mosquitto.db; stat -c "%u:%g:%a" /proof; sha256sum /proof/mosquitto.db'],timeout=20).stdout.decode().splitlines()
            assert result[0]=='1883:1883:750'
            return result[1].split()[0]
        saved_sha=saved();receipt['saved_database_sha256']=saved_sha
        docker([*compose,'run','--rm','--no-deps','mqtt-log-init'],timeout=30)
        assert saved()==saved_sha
        receipt['checks']['initializer_preserves_saved_bytes']=True
        docker([*compose,'up','-d','--wait','--no-deps','--wait-timeout','30','mqtt'],timeout=60)
        after_restart=json.loads(docker(['inspect',name]).stdout)[0]
        assert after_restart['Id']==owned['Id'] and after_restart['State']['Running']
        address,binding=endpoint(after_restart);receipt['restarted_binding']=binding
        receipt['verify']=json.loads(command(['node',str(client),'verify',address,token],timeout=20).stdout)
        assert receipt['verify']=={'passed':True,'session_present':True,'queued_qos1_restored':3,'retained_restored':1,'all_clients_closed':True}
        receipt['checks']['graceful_restart_preserves_session_queue_and_retained']=True
    except Exception as error:
        receipt['errors'].append(type(error).__name__)
        if hasattr(error,'stderr') and error.stderr:
            # Container/test output is synthetic. No original data, env or row material is printed.
            (output/'failure.stderr').write_bytes(error.stderr)
    finally:
        # Every removable resource belongs to this freshly generated project; never adopt names.
        try:
            if not admitted:raise ValueError('project was never admitted; refusing cleanup adoption')
            helpers=docker(['ps','-aq','--filter','label='+helper_label]).stdout.decode().split()
            for ident in helpers:
                actual=json.loads(docker(['inspect',ident]).stdout)[0]
                if actual['Config']['Labels'].get('ukmesh.broker-proof-owner')!=token:raise ValueError('helper ownership differs')
                docker(['rm','-f','-v',ident])
            receipt['checks']['owned_helpers_removed']=not docker(['ps','-aq','--filter','label='+helper_label]).stdout.strip()
            ids=docker(['ps','-aq','--filter','label=com.docker.compose.project='+project]).stdout.decode().split()
            for ident in ids:
                actual=json.loads(docker(['inspect',ident]).stdout)[0]
                if actual['Config']['Labels']['com.docker.compose.project']!=project:raise ValueError('cleanup ownership differs')
            docker([*compose,'down','--volumes','--timeout','20','--remove-orphans'],timeout=45)
            receipt['checks']['owned_containers_removed']=not docker(['ps','-aq','--filter','label=com.docker.compose.project='+project]).stdout.strip()
            receipt['checks']['owned_volumes_removed']=not docker(['volume','ls','-q','--filter','label=com.docker.compose.project='+project]).stdout.strip()
            receipt['checks']['owned_networks_removed']=not docker(['network','ls','-q','--filter','label=com.docker.compose.project='+project]).stdout.strip()
        except Exception as error:receipt['errors'].append('cleanup:'+type(error).__name__)
        try:
            receipt['source_after']=hashes();receipt['checks']['source_stable']=receipt['source_before']==receipt['source_after']
        except Exception as error:
            receipt['errors'].append('source:'+type(error).__name__);receipt['checks']['source_stable']=False
        receipt['original_broker_before']=original
        try:
            receipt['original_broker_after']=original_state()
            receipt['checks']['original_broker_not_restarted']=receipt['original_broker_after']==original
        except Exception as error:
            receipt['errors'].append('original-inspection:'+type(error).__name__);receipt['checks']['original_broker_not_restarted']=False
        receipt['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        required={'legacy_image_default_data_writable','initializer_preserves_saved_bytes','graceful_restart_preserves_session_queue_and_retained','owned_containers_removed','owned_volumes_removed','owned_helpers_removed','owned_networks_removed','source_stable','original_broker_not_restarted'}
        receipt['passed']=not receipt['errors'] and set(receipt['checks'])==required and all(receipt['checks'].values())
        write_json(output/'result.json',receipt)
    return receipt


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute',action='store_true');parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not args.execute:parser.error('isolated native proof requires explicit --execute')
    result=run(args.output.resolve());print(json.dumps({'passed':result['passed'],'path':str(args.output/'result.json'),'errors':result['errors']}));sys.exit(0 if result['passed'] else 1)
