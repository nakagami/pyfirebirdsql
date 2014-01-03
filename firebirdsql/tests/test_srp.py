import unittest
from firebirdsql import srp


# backwards compatibility:
if not hasattr(unittest, "skip"):
    def _empty(func):
        pass
    def _skip(message):
        return _empty
    unittest.skip = _skip


class TestSrp(unittest.TestCase):
    @unittest.skip("working")
    def test_srp(self):
        user = b'sysdba'
        password = b'masterkey'
    
        # Client send A to Server
        A, a = srp.client_seed(user, password)
    
        # Server send B, salt to Client
        salt = srp.get_salt()
        v = srp.get_verifier(user, password, salt)
        B, b = srp.server_seed(v)
    
        # Client send M to Server
        M, clientKey = srp.client_proof(user, password, salt, A, B, a)
    
        # Server send serverProof to Client
        serverProof, serverKey = srp.server_proof(user, salt, A, B, M, b, v)
    
        # Client can verify by serverProof
        srp.verify_server_proof(clientKey, A, M, serverProof)
    
        # Client and Server has same key
        self.assertEqual(clientKey, serverKey)

