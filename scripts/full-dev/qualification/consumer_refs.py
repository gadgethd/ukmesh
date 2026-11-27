"""Pure role/ref projection. No fixture lifecycle, credentials or IO ownership."""
from pathlib import Path
from urllib.parse import parse_qs,unquote,urlsplit

CORE={'ukmesh-companions-corescope-1','ukmesh-companions-corescope-ingestor-1'}
BACKEND_SERVICES={'ukmesh-full-backend-1','ukmesh-full-mqtt-ingest-1'}
BACKEND_ROLES={'MESHCORE_INGEST_DATABASE_URL':'meshcore_ingest','MESHCORE_WORKER_DATABASE_URL':'meshcore_worker'}
REQUIRED={
    **{name:{'DATABASE_URL','MESHCORE_HTTP_DATABASE_URL','MESHCORE_INGEST_DATABASE_URL','MESHCORE_WORKER_DATABASE_URL','OWNER_DATABASE_URL'}
       for name in BACKEND_SERVICES},
    'ukmesh-full-db-migrate-1':{'DATABASE_URL','MESHCORE_MIGRATE_DATABASE_URL'},
    'ukmesh-full-postgres-exporter-1':{'DATA_SOURCE_NAME'},
    'ukmesh-companions-beacon-1':{'POSTGRES_DSN','BEACON_MAINTENANCE_DSN'},
    'ukmesh-companions-beacon-migrate-1':{'POSTGRES_DSN'},
}


def database_setting(key):
    return 'DATABASE_URL' in key or key.endswith('_DSN') or key=='DATA_SOURCE_NAME'


def describe_consumer(name,item,*,identity=False,finite=False):
    environment={}
    for value in item['Config'].get('Env',[]):
        key,separator,setting=value.partition('=')
        if not separator or key in environment:raise ValueError('ambiguous consumer environment')
        environment[key]=setting
    required=REQUIRED.get(name,{'DATABASE_URL','MESHCORE_WORKER_DATABASE_URL'}
        if name.endswith('-worker-1') or name=='ukmesh-full-synthetic-monitor-1' else set())
    if any(not environment.get(key) for key in required):raise ValueError(name+' lacks an explicit required database session target')
    refs,checks={},[]
    for key,value in environment.items():
        if not value or not database_setting(key):continue
        parsed=urlsplit(value)
        family='beacon' if name.startswith('ukmesh-companions-beacon') else ('owner' if key=='OWNER_DATABASE_URL' else 'ukmesh')
        hosts={'beacon-db','ukmesh-companions-beacon-db-1'} if family=='beacon' else {'ukmesh-sample-db'}
        if name=='ukmesh-companions-beacon-migrate-1':role='beacon_dev'
        elif family=='beacon':role='beacon_maint' if key=='BEACON_MAINTENANCE_DSN' else 'beacon_runtime'
        elif family=='owner':role='meshcore_owner'
        elif name=='ukmesh-full-db-migrate-1':role='meshcore'
        elif name=='ukmesh-full-postgres-exporter-1':role='meshcore_monitor'
        elif name in BACKEND_SERVICES:role=BACKEND_ROLES.get(key,'meshcore_api')
        else:role='meshcore_worker'
        database=unquote(parsed.path).removeprefix('/')
        if parsed.scheme not in ('postgres','postgresql') or parsed.hostname not in hosts or parsed.port!=5432 \
                or unquote(parsed.username or '')!=role or parsed.fragment or not database or '/' in database \
                or parse_qs(parsed.query,keep_blank_values=True) not in ({},{'sslmode':['disable']}):
            raise ValueError(name+' '+key+' differs from its selected store or intended session role')
        if family in refs and refs[family]!=database:raise ValueError('one consumer has conflicting selected store refs')
        refs[family]=database
        checks.append({'consumer':name,'setting':key,'target':family,'role':role})
    if name in CORE:
        mounts=[str(Path(m['Source']).resolve()) for m in item['Mounts'] if m['Type']=='bind' and m['Destination']=='/data']
        if len(mounts)!=1:raise ValueError('Core consumer data directory is ambiguous')
        refs['corescope']=mounts[0]
    state=item.get('State',{})
    result={'name':name,'refs':refs,'container_id':item.get('Id'),'running':state.get('Running'),
            'status':state.get('Status'),'started_at':state.get('StartedAt'),'pid':state.get('Pid'),
            'image_id':item.get('Image'),'checks':checks,'finite':finite,'exit_code':state.get('ExitCode')}
    if identity:
        expected='ukmesh-companions' if name.startswith('ukmesh-companions-') else 'ukmesh-full'
        if item['Config'].get('Labels',{}).get('com.docker.compose.project')!=expected \
                or not result['container_id'] or type(result['running']) is not bool or not result['image_id'] \
                or (result['running'] and (type(result['pid']) is not int or result['pid']<=0 or not result['started_at'])):
            raise ValueError('consumer runtime ownership is unknown')
    return result


def requested_refs(record,targets):
    if not set(record['refs']).issubset(targets):raise ValueError('requested target family missing')
    return {family:targets[family] for family in record['refs']}


def validate_consumers(containers,targets,data_directory):
    wanted={**targets,'corescope':str(data_directory.resolve())};checks=[];cores=set()
    for name,item in containers.items():
        if not name.startswith(('ukmesh-full-','ukmesh-companions-')):continue
        record=describe_consumer(name,item)
        if record['refs']!=requested_refs(record,wanted):
            raise ValueError(name+(' does not mount the selected CoreScope store' if name in CORE else ' differs from its selected store'))
        checks+=record['checks']
        if name in CORE:cores.add(name)
    if cores!=CORE:raise ValueError('both Core consumers must use the selected data directory')
    if not checks:raise ValueError('runtime has no PostgreSQL consumer target evidence')
    return checks
