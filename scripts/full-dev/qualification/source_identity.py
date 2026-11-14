"""Bind ignored baseline archives and current sources to their actual bytes."""
from pathlib import Path
import json
import re

from common import REPO, STATE, command, read_json

BASELINE=STATE/'runtime/lookup-baseline.json'


def identify(directory: Path, revision: str) -> dict:
    result=command(['node','--max-old-space-size=128','--import',str(REPO/'backend/node_modules/tsx/dist/loader.mjs'),
                    str(REPO/'backend/src/tools/lookupSource.ts'),str(directory),revision],timeout=60)
    return json.loads(result.stdout)


def expected_sources() -> dict:
    seal=read_json(BASELINE)
    keys=('source_root','revision','sha256','files')
    if seal.get('git_verified') is not True or not re.fullmatch(r'[a-f0-9]{40}',seal.get('revision','')) \
            or not re.fullmatch(r'[a-f0-9]{64}',seal.get('sha256','')) or not isinstance(seal.get('files'),int) or seal['files']<1:
        raise ValueError('lookup baseline requires its Git-verified source seal')
    baseline={key:seal[key] for key in keys}
    path=Path(baseline['source_root']).resolve()
    if not path.is_relative_to(REPO/'plans') or identify(path,baseline['revision'])!=baseline:
        raise ValueError('sealed lookup baseline bytes or ownership differ')
    revision=command(['git','rev-parse','HEAD']).stdout.decode().strip()
    return {'baseline':baseline,'candidate':identify(REPO/'backend/src',revision)}


def validate_measurements(runs: list[dict], expected: dict) -> None:
    for row in runs:
        label=row.get('label')
        if label not in expected or row.get('measured_source')!=expected[label]:
            raise ValueError('lookup measurements used an unsealed or stale source tree')
        for mode in row.get('modes',[]):
            if mode.get('source_identity')!=expected[label] or mode.get('source_stable') is not True:
                raise ValueError('lookup invocation source drift was not excluded')
