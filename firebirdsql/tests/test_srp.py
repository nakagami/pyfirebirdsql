import unittest
from firebirdsql import srp
from firebirdsql.tests.base import *

class TestSrp(unittest.TestCase):
    def test_srp(self):
        user = b'sysdba'
        password = b'masterkey'
    
        # Client send A to Server
        A, a = srp.client_seed(user, password)
    
        # Server send B, salt to Client
        salt = srp.get_salt()
        v = srp.get_verifier(user, password, salt)
        B, b = srp.server_seed(v)
    
        serverKey = srp.server_session(user, password, salt, A, B, b)

        # Client send M to Server
        M, clientKey = srp.client_proof(user, password, salt, A, B, a)
    
        # Client and Server has same key
        self.assertEqual(clientKey, serverKey)

