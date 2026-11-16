"""Synthetic authenticated-role evidence; real validators, no service calls."""
import copy
import unittest

from role_evidence import validate_database_identity


def role_context(scale='1x'):
    return {'targets': {'ukmesh': f'lean_{scale}_role_fixture',
                        'owner': f'lean_owner_{scale}_role_fixture'}}


def database_identity(context, *, operations=False):
    """Fixture metadata only: deliberately not an authenticated-session oracle."""
    expected = {role: {'user': 'meshcore_' + ('api' if role == 'http' else role),
                       'database': context['targets']['owner' if role == 'owner' else 'ukmesh']}
                for role in ('http', 'worker', 'ingest', 'owner')}
    used = ('http', 'worker') if operations else ('http',)
    phases = {phase: {role: {'current_user': expected[role]['user'],
                            'session_user': expected[role]['user'],
                            'database': expected[role]['database'],
                            'backend_pid': 100 + phase_index * 10 + role_index,
                            'transaction_read_only': 'on',
                            'default_transaction_read_only': 'on',
                            'superuser': False, 'bypass_rls': False}
                      for role_index, role in enumerate(used)}
              for phase_index, phase in enumerate(('setup', 'after'))}
    return {'version': 1, 'scope': 'application-role-readonly', 'expected': expected,
            **phases, 'profiles': {name: {'max': maximum}
                                 for name, maximum in (('http', 8), ('worker', 2), ('ingest', 2))},
            'ownerPool': {'configuredMax': 2, 'queriedByBenchmark': False}}


class AuthenticatedRoleEvidence(unittest.TestCase):
    def setUp(self):
        self.context = role_context()

    def test_current_read_and_operations_proofs_allow_independent_pool_checkouts(self):
        for operations in (False, True):
            value = database_identity(self.context, operations=operations)
            self.assertNotEqual(value['setup']['http']['backend_pid'], value['after']['http']['backend_pid'])
            self.assertIsNone(validate_database_identity(value, self.context, operations=operations))

    def test_legacy_configuration_only_missing_and_wrongtyped_versions_cannot_qualify(self):
        valid = database_identity(self.context)
        for value in (None, {}, {'passed': True, 'expected': valid['expected']},
                      {**valid, 'scope': 'legacy-inspector-readonly'},
                      *({**valid, 'version': version} for version in (True, 1.0, '1', 2))):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_database_identity(value, self.context)

    def test_consistent_administrator_claims_cannot_replace_canonical_application_roles(self):
        for role in ('http', 'worker', 'ingest', 'owner'):
            value = database_identity(self.context, operations=True)
            value['expected'][role]['user'] = 'meshcore'
            for phase in ('setup', 'after'):
                if role in value[phase]:
                    value[phase][role]['current_user'] = value[phase][role]['session_user'] = 'meshcore'
            with self.subTest(role=role), self.assertRaises(ValueError):
                validate_database_identity(value, self.context, operations=True)

    def test_every_used_session_in_both_phases_requires_authenticated_readonly_identity(self):
        faults = {'current_user': 'meshcore', 'session_user': 'meshcore', 'database': 'meshcore',
                  'backend_pid': 0, 'transaction_read_only': 'off', 'default_transaction_read_only': 'off',
                  'superuser': True, 'bypass_rls': True}
        for phase in ('setup', 'after'):
            for role in ('http', 'worker'):
                for key, replacement in faults.items():
                    value = database_identity(self.context, operations=True)
                    value[phase][role][key] = replacement
                    with self.subTest(phase=phase, role=role, key=key), self.assertRaises(ValueError):
                        validate_database_identity(value, self.context, operations=True)
        for pid in (True, -1, 1.5, '123'):
            value = database_identity(self.context)
            value['after']['http']['backend_pid'] = pid
            with self.subTest(pid=pid), self.assertRaises(ValueError):
                validate_database_identity(value, self.context)

    def test_selected_main_and_owner_stores_bind_even_when_all_claimed_sessions_agree(self):
        value = database_identity(self.context, operations=True)
        for target in ('ukmesh', 'owner'):
            context = copy.deepcopy(self.context)
            context['targets'][target] = 'meshcore'
            with self.subTest(target=target), self.assertRaises(ValueError):
                validate_database_identity(value, context, operations=True)
        for context in ({}, {'targets': {}}, {'targets': {'ukmesh': self.context['targets']['ukmesh']}}):
            with self.subTest(context=context), self.assertRaises(ValueError):
                validate_database_identity(value, context, operations=True)

    def test_read_and_operations_proofs_require_exact_observed_role_sets(self):
        for operations in (False, True):
            for phase in ('setup', 'after'):
                value = database_identity(self.context, operations=operations)
                if operations:
                    value[phase].pop('worker')
                else:
                    value[phase]['worker'] = copy.deepcopy(value[phase]['http'])
                with self.subTest(operations=operations, phase=phase), self.assertRaises(ValueError):
                    validate_database_identity(value, self.context, operations=operations)

    def test_pool_bounds_and_unqueried_owner_are_evidence_not_green_flags(self):
        for name, wrong in (('http', 2), ('worker', 8), ('ingest', 8), ('http', True)):
            value = database_identity(self.context)
            value['profiles'][name]['max'] = wrong
            with self.subTest(name=name, max=wrong), self.assertRaises(ValueError):
                validate_database_identity(value, self.context)
        for owner in ({'configuredMax': 8, 'queriedByBenchmark': False},
                      {'configuredMax': 2, 'queriedByBenchmark': True},
                      {'configuredMax': True, 'queriedByBenchmark': False}):
            value = database_identity(self.context)
            value['ownerPool'] = owner
            with self.subTest(owner=owner), self.assertRaises(ValueError):
                validate_database_identity(value, self.context)
        value = database_identity(self.context)
        value['expected'].pop('owner')
        with self.assertRaises(ValueError):
            validate_database_identity(value, self.context)


if __name__ == '__main__':
    unittest.main()
