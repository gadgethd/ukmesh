"""Require every declared runtime service to have a work and lifecycle oracle."""
from pathlib import Path
import yaml


class ComposeLoader(yaml.SafeLoader):
    pass


def compose_tag(loader, suffix, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_scalar(node)


ComposeLoader.add_multi_constructor('!', compose_tag)


def coverage(root: Path, manifest: dict) -> dict:
    definitions = {}
    for path in ('docker-compose.yml', 'scripts/full-dev/compose.yml'):
        definitions.update(yaml.load((root / path).read_text(), Loader=ComposeLoader)['services'])
    expected = set(definitions)
    expected.update(yaml.safe_load((root / 'scripts/companion-dev/compose.yml').read_text())['services'])
    sample = yaml.safe_load((root / 'scripts/local-sample/compose.yml').read_text())['services']
    expected.update('sample-' + name if name in ('db', 'redis', 'mqtt') else name for name in sample)
    exclusions = manifest.get('excluded', {})
    entries = manifest['services']
    names = [entry['name'] for entry in entries]
    checks = {
        'unique_names': len(set(names)) == len(names),
        'unique_containers': len({entry['container'] for entry in entries}) == len(entries),
        'all_declared_services_accounted_for': expected == set(names) | set(exclusions),
        'exclusions_are_explicit': all(isinstance(reason, str) and reason.strip() for reason in exclusions.values()),
        'every_service_required_with_work': all(entry.get('required') is True and entry.get('work') for entry in entries),
        'no_required_service_excluded': not set(names).intersection(exclusions),
    }
    return {'passed': all(checks.values()), 'checks': checks,
            'missing': sorted(expected - set(names) - set(exclusions)),
            'unknown': sorted((set(names) | set(exclusions)) - expected)}
