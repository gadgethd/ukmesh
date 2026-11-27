#!/usr/bin/env python3
"""Render and control the LAN addon against the canonical DEV networks."""

import hashlib
import ipaddress
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'qualification'))

from common import STATE, command, docker, read_json, write_json
from target_lease import target_lease
from target_safety import require_originals

RUNTIME = STATE / 'runtime' / 'lan'
ROUTES = HERE / 'site' / 'routes.json'
PRIVATE_LANS = tuple(ipaddress.ip_network(network) for network in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))


def tool_headers(routes, address):
    ports = {route['id']: route['port'] for route in routes}
    site, core, health, beacon = (f'http://{address}:{ports[key]}' for key in ('website', 'corescope', 'health', 'beacon-web'))
    substitutions = {'SITE': site, 'CORE': core, 'HEALTH': health, 'BEACON': beacon,
                     'CORE_PORT': str(ports['corescope']), 'HEALTH_PORT': str(ports['health']),
                     'BEACON_PORT': str(ports['beacon-web'])}
    maps = (HERE / 'tool-headers.conf').read_text()
    for name, value in substitutions.items():
        maps = maps.replace(f'@{name}@', value)
    security = '    proxy_hide_header Content-Security-Policy;\n'
    embedded_ancestors = f'    add_header Content-Security-Policy "frame-ancestors \'self\' {site}" always;\n'
    return maps, {
        'website': security + '    add_header Content-Security-Policy $embedded_site_policy always;\n',
        'corescope': embedded_ancestors,
        'beacon-web': embedded_ancestors,
        'health': security + '    proxy_hide_header X-Frame-Options;\n'
                  '    add_header Content-Security-Policy $embedded_health_policy always;\n'
                  '    add_header X-Frame-Options $embedded_health_frame_options always;\n',
    }


def bind_address(value, local):
    address = ipaddress.IPv4Address(value)
    if str(address) != '127.0.0.1' and (
            not any(address in network for network in PRIVATE_LANS) or str(address) not in local):
        raise ValueError('select a private IPv4 address assigned to the DEV host')
    return str(address)


def assigned_addresses():
    links = json.loads(command(['ip', '-j', '-4', 'address', 'show']).stdout)
    return {row['local'] for link in links for row in link.get('addr_info', [])}


def local_bind(value):
    return bind_address(value, set() if value == '127.0.0.1' else assigned_addresses())


def render(routes, address):
    if {route['port'] for route in routes} != set(range(8081, 8090)) or len(routes) != 9:
        raise ValueError('LAN routes must cover the nine existing service ports once')
    tool_maps, tool_listeners = tool_headers(routes, address)
    base = (HERE / 'nginx.conf').read_text() + '\n' + tool_maps
    proxy = (HERE / 'lan-proxy.inc').read_text().replace(
        'location / {\n', 'location / {\n    proxy_set_header Cookie $lan_proxy_cookie;\n', 1)
    listeners = []
    ports = []
    for route in routes:
        port, upstream = route['port'], route['upstream']
        operator = route['access'] == 'operator'
        host = '127.0.0.1' if operator else address
        ports.append(f'{host}:{port}:{port}')
        restricted = ''
        if host != '127.0.0.1':
            restricted = '''    location ~* ^/(?:api/(?:v[0-9]+/)?)?(?:admin|internal|metrics|operations|observer-registrations|local-api)(?:/|$) {
        return 404;
    }
'''
            for path in route.get('operator_paths', []):
                restricted += f'    location = {path} {{ return 404; }}\n'
            operator_methods = route.get('operator_methods', [])
            if operator_methods:
                restricted += '    set $method_and_path "$request_method $uri";\n'
            for method, path in operator_methods:
                restricted += f'    if ($method_and_path = "{method} {path}") {{ return 404; }}\n'
        listeners.append(
            f'server {{\n    listen {port};\n    set $upstream {upstream};\n'
            f'{restricted}{tool_listeners.get(route["id"], "")}{proxy}}}\n'
        )
    config = base + '\n' + '\n'.join(listeners)
    revision = hashlib.sha256(config.encode()).hexdigest()
    override = {'services': {
        'site': {'ports': [f'{address}:8080:8080'], 'environment': {'LAN_BIND_ADDRESS': address},
                 'labels': {'ukmesh.routes.revision': hashlib.sha256(ROUTES.read_bytes()).hexdigest()}},
        'gateway': {'ports': ports, 'volumes': [f'{RUNTIME / f"gateway-{revision}.conf"}:/etc/nginx/conf.d/default.conf:ro']},
    }}
    return config, override


def compose(args, *, include_override=True):
    files = ['-f', str(HERE / 'compose.yml')]
    override = RUNTIME / 'compose.json'
    if include_override and override.is_file():
        files.extend(['-f', str(override)])
        ui = command(['python3', str(HERE.parent / 'ui/control.py'), 'file', 'lan']).stdout.decode().strip()
        if ui:
            files.extend(['-f', ui])
    result = docker(['compose', '--project-name', 'ukmesh-lan', '--env-file', '/dev/null', *files, *args],
                    timeout=100, check=False)
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    return result.returncode


def active(action, arguments):
    selected = RUNTIME / 'active.json'
    if action == 'prepare':
        address = local_bind(arguments[0])
    else:
        if not selected.is_file():
            raise ValueError('prepare LAN bindings before starting the addon')
        address = local_bind(read_json(selected)['address'])
    config, override = render(read_json(ROUTES), address)
    gateway = Path(override['services']['gateway']['volumes'][0].split(':', 1)[0])
    compose_file = RUNTIME / 'compose.json'
    if action == 'up':
        if not gateway.is_file() or gateway.read_text() != config or read_json(compose_file) != override:
            raise ValueError('LAN bindings changed during preparation')
        return compose(['up', '-d', '--wait', '--wait-timeout', '45'])
    RUNTIME.mkdir(mode=0o700, parents=True, exist_ok=True)
    gateway.write_text(config)
    write_json(compose_file, override)
    write_json(selected, {'address': address})
    return 0


def main(action, *arguments):
    selected = RUNTIME / 'active.json'
    if action == 'origins':
        if selected.is_file():
            address = local_bind(read_json(selected)['address'])
            routes = read_json(ROUTES)
            for role in ('app', 'health'):
                print(','.join(f'http://{address}:{route["port"]}' for route in routes if route.get('origin') == role))
        return 0
    if action in ('prepare', 'up'):
        with target_lease():
            require_originals()
            return active(action, arguments)
    if action == 'refresh':
        with target_lease():
            require_originals()
            active('prepare', (read_json(selected)['address'],))
            return active('up', ())
    if action == 'stop':
        with target_lease():
            status = compose(['stop'], include_override=False)
            if status == 0:
                selected.unlink(missing_ok=True)
            return status
    if action == 'status':
        return compose(['ps', '-a'])
    raise ValueError('expected LAN prepare, up, refresh, stop or status')


if __name__ == '__main__':
    try:
        raise SystemExit(main(*sys.argv[1:]))
    except (OSError, ValueError) as error:
        print(f'LAN control refused: {error}', file=sys.stderr)
        raise SystemExit(2)
