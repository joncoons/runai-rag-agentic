import unittest
from application.web_acquisition import AcquisitionPolicy,validate_url,AcquisitionError

class OperatorScopeTests(unittest.TestCase):
 def test_nonstandard_port_is_preserved_only_with_explicit_operator_scope(self):
  policy=AcquisitionPolicy(allowed_hosts=('127.0.0.1',),private_hosts=('127.0.0.1',),private_ports=(8089,))
  self.assertEqual(validate_url('http://127.0.0.1:8089/page',policy),'http://127.0.0.1:8089/page')
  for untrusted in (AcquisitionPolicy(allowed_hosts=('127.0.0.1',)),AcquisitionPolicy(allowed_hosts=('127.0.0.1',),private_hosts=('127.0.0.1',))):
   with self.assertRaises(AcquisitionError):validate_url('http://127.0.0.1:8089/page',untrusted)
 def test_public_default_port_normalization_remains_stable(self):
  policy=AcquisitionPolicy(allowed_hosts=('example.com',))
  self.assertEqual(validate_url('https://example.com:443/page#section',policy),'https://example.com/page')
