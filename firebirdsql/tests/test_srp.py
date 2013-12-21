import unittest
from firebirdsql.tests import base
from firebirdsql import srp

class TestSrp(base.TestBase):
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
        verify_server_proof(clientKey, A, M, serverProof)
    
        # Client and Server has same key
        self.assertEqual(clientKey == serverKey)
    
