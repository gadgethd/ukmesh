"""Owned intent state for selections, partial application and complete closure."""
from copy import deepcopy
from consumer_refs import requested_refs


def new_intent(token,records,targets,*,old_targets):
    if not token or not records:raise ValueError('owned intent and actual consumers required')
    if any(row['refs']!=requested_refs(row,old_targets) for row in records.values() if row['refs']):
        raise ValueError('actual consumers differ from the known previous target')
    return {'version':1,'token':token,'status':'pending','requested_targets':deepcopy(targets),'old_targets':deepcopy(old_targets),
            'consumers':{name:{'old':deepcopy(row['refs']),'requested':requested_refs(row,targets),
                               'observed_container_id':row['container_id']}
                         for name,row in records.items() if row['refs']}}


def assess_intent(intent,records,targets):
    if intent.get('version')!=1 or intent.get('status')!='pending' or intent['requested_targets']!=targets:
        raise ValueError('resume requires the exact pending target intent')
    categories={};inactive_same=[];blocked=[]
    for name,expected in intent['consumers'].items():
        actual=records.get(name)
        if actual is None:raise ValueError('unknown consumer; resume previous retained store')
        if actual['refs']==expected['requested']:kind='requested'
        elif actual['refs']==expected['old']:kind='old'
        else:raise ValueError('foreign consumer refs; pending intent cannot adopt them')
        categories[name]=kind
        if not actual['running'] and not actual.get('finite',False):
            if kind=='requested':inactive_same.append(name)
            else:blocked.append(name)
    extra={n for n,row in records.items() if row['refs']} - set(intent['consumers'])
    if extra:raise ValueError('unowned consumer appeared during transition')
    return {'categories':categories,'mixed':len(set(categories.values()))>1,
            'same_store_inactive_resume':inactive_same,'blocked_changing_inactive':blocked,
            'may_observe':not blocked,'complete':False}


def complete_intent(intent,records,required_inventory):
    """A partial start never becomes complete merely by updating selected settings."""
    assessed=assess_intent(intent,records,intent['requested_targets'])
    expected=set(required_inventory['required'])
    present=set(required_inventory['present'])
    if required_inventory.get('passed') is not True or not expected or present!=expected \
            or any(kind!='requested' for kind in assessed['categories'].values()) \
            or assessed['same_store_inactive_resume'] or assessed['blocked_changing_inactive'] \
            or any(row.get('finite') and (row.get('status')!='exited' or row.get('exit_code')!=0) for row in records.values()):
        raise ValueError('complete service inventory and requested refs are required')
    result=deepcopy(intent);result['status']='applied'
    result['applied_consumers']={name:{'refs':deepcopy(row['refs']),'container_id':row['container_id']}
                                 for name,row in records.items() if row['refs']}
    return result
