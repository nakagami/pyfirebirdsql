import unittest
from firebirdsql import srp
from firebirdsql.tests.base import *

class TestSrp(TestBase):
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

    def test_legacy_auth(self):
        self.connection = firebirdsql.connect(
                auth_plugin_list=("Legacy_Auth",), 
                wire_crypt=False,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)

        self.connection.close()

