import unittest
from unittest.mock import call, patch

import control


class ReleaseTest(unittest.TestCase):
    @patch.object(control, 'write_json')
    @patch.object(control, 'switch')
    @patch.object(control, 'require_originals')
    @patch.object(control, 'verify')
    def test_publishes_pointer_after_switching_all_services(self, verify, require_originals, switch, save):
        control.apply('redesign', 'classic')
        verify.assert_has_calls([call('redesign'), call('classic')])
        require_originals.assert_called_once()
        self.assertEqual(switch.call_args_list, [call('redesign', entry) for entry in control.SERVICES])
        save.assert_called_once_with(control.ACTIVE, {'current': 'redesign', 'previous': 'classic'})

    @patch.object(control, 'write_json')
    @patch.object(control, 'switch')
    @patch.object(control, 'require_originals')
    @patch.object(control, 'verify')
    def test_partial_switch_restores_services_in_reverse_order(self, verify, require_originals, switch, save):
        first, second = control.SERVICES[:2]
        switch.side_effect = [None, ValueError('failed health check'), None, None]
        with self.assertRaisesRegex(RuntimeError, 'prior release restored'):
            control.apply('redesign', 'classic')
        self.assertEqual(switch.call_args_list, [
            call('redesign', first), call('redesign', second), call('classic', second), call('classic', first),
        ])
        save.assert_not_called()


if __name__ == '__main__':
    unittest.main()
