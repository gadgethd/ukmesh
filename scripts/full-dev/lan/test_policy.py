import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import control
from control import ROUTES, bind_address, render
import fixtures
import target_safety


class LanPolicy(unittest.TestCase):
    def test_hosts_and_operator_ports_are_explicit(self):
        address = bind_address('192.168.2.22', {'192.168.2.22'})
        routes = json.loads(ROUTES.read_text())
        config, override = render(routes, address)
        self.assertEqual(override['services']['site']['ports'], ['192.168.2.22:8080:8080'])
        ports = dict((int(item.split(':')[1]), item.split(':')[0])
                     for item in override['services']['gateway']['ports'])
        self.assertEqual({port for port, host in ports.items() if host == '127.0.0.1'}, {8085, 8086, 8088})
        self.assertEqual({port for port, host in ports.items() if host == address}, {8081, 8082, 8083, 8084, 8087, 8089})
        self.assertIn('location = /api/backup { return 404; }', config)
        self.assertIn('"PUT /api/config/geo-filter"', config)
        self.assertNotIn('"GET /api/config/geo-filter"', config)
        loopback, _ = render(routes, '127.0.0.1')
        self.assertNotIn('return 404;', loopback)

    def test_gateway_revision_changes_with_operator_policy(self):
        routes = json.loads(ROUTES.read_text())
        _, before = render(routes, '192.168.2.22')
        routes[1]['operator_paths'].append('/api/diagnostics')
        _, after = render(routes, '192.168.2.22')
        self.assertNotEqual(before['services']['gateway']['volumes'], after['services']['gateway']['volumes'])

    def test_tools_only_allow_the_bound_website_as_parent(self):
        config, _ = render(json.loads(ROUTES.read_text()), '192.168.2.22')
        self.assertIn('frame-src http://192.168.2.22:8082 http://192.168.2.22:8089 http://192.168.2.22:8084', config)
        self.assertIn("frame-ancestors 'self' http://192.168.2.22:8083", config)
        self.assertIn('frame-ancestors http://192.168.2.22:8083$health_csp_end', config)
        self.assertIn('8082 $core_cookies;', config)
        self.assertIn('8089 $health_cookies;', config)
        self.assertIn('8084 "";', config)
        self.assertNotIn('frame-ancestors http://192.168.2.22:8087', config)

    def test_up_rechecks_all_original_targets_before_exposing_prepared_ports(self):
        with tempfile.TemporaryDirectory() as runtime, patch.object(control, 'RUNTIME', Path(runtime)), \
                patch.object(control, 'target_lease') as lease, patch.object(control, 'compose') as compose, \
                patch.object(fixtures, 'selection_values', return_value=fixtures.original_values()) as selection, \
                patch.object(target_safety, 'read_intent', return_value=None), \
                patch.object(target_safety, 'inspect_snapshot', return_value={'consumers': {}}):
            self.assertEqual(control.main('prepare', '127.0.0.1'), 0)
            selection.return_value = {**fixtures.original_values(), 'BEACON_DATABASE': 'fixture'}
            with self.assertRaisesRegex(ValueError, 'original DEV stores'):
                control.main('up')
            compose.assert_not_called()
            self.assertEqual(lease.call_count, 2)

    def test_up_rejects_unapplied_originals_or_changed_rendered_config(self):
        with tempfile.TemporaryDirectory() as runtime, patch.object(control, 'RUNTIME', Path(runtime)), \
                patch.object(control, 'target_lease'), patch.object(control, 'compose') as compose, \
                patch.object(fixtures, 'selection_values', return_value=fixtures.original_values()), \
                patch.object(target_safety, 'read_intent', return_value=None) as intent, \
                patch.object(target_safety, 'inspect_snapshot', return_value={'consumers': {}}):
            self.assertEqual(control.main('prepare', '127.0.0.1'), 0)
            intent.return_value = {'status': 'pending'}
            with self.assertRaisesRegex(ValueError, 'apply the original'):
                control.main('up')
            intent.return_value = None
            next(Path(runtime).glob('gateway-*.conf')).write_text('changed')
            with self.assertRaisesRegex(ValueError, 'bindings changed'):
                control.main('up')
            compose.assert_not_called()

    def test_explicit_address_must_be_private_and_local(self):
        self.assertEqual(bind_address('127.0.0.1', set()), '127.0.0.1')
        for address, local in (('0.0.0.0', {'0.0.0.0'}), ('8.8.8.8', {'8.8.8.8'}),
                               ('192.168.2.23', {'192.168.2.22'})):
            with self.subTest(address=address), self.assertRaises(ValueError):
                bind_address(address, local)
