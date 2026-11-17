"""Alert work proof belongs to its actual successful forward, within its deadline."""
import unittest
from unittest.mock import patch
import work

class AlertWorkProof(unittest.TestCase):
    def invoke(self, counts, clock):
        from types import SimpleNamespace
        with patch.object(work.uuid, 'uuid4', return_value=SimpleNamespace(hex='a'*32)), \
             patch.object(work, 'request', side_effect=[{'cluster': {'status': 'ready'}}, None, counts]), \
             patch.object(work.time, 'monotonic', side_effect=clock), \
             patch.object(work.time, 'sleep'):
            return work.check('alertmanager')

    def test_owned_forward_remains_proven_after_another_alert_arrives(self):
        self.assertEqual(self.invoke({'lastAlertProbe':'b'*32,'alertProbes':['a'*32,'b'*32]},[0,1,2]),
                         {'native_forwarding':True})

    def test_a_last_marker_without_successful_history_is_not_delivery(self):
        with self.assertRaisesRegex(AssertionError, 'did not reach'):
            self.invoke({'lastAlertProbe':'a'*32,'alertProbes':['b'*32]},[0,1,2,26])

    def test_a_slow_final_response_cannot_pass_after_the_deadline(self):
        with self.assertRaisesRegex(AssertionError, 'did not reach'):
            self.invoke({'lastAlertProbe':'a'*32,'alertProbes':['a'*32]},[0,1,26,27])

if __name__=='__main__': unittest.main()
