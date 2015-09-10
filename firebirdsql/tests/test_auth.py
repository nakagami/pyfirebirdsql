import unittest
from firebirdsql import srp
from firebirdsql.tests.base import *

class TestAuth(TestBase):
    def test_srp_key_exchange(self):
        user = b'sysdba'
        password = b'masterkey'
    
        # Client send A to Server
        A, a = srp.client_seed()
    
        # Server send B, salt to Client
        salt = srp.get_salt()
        v = srp.get_verifier(user, password, salt)
        B, b = srp.server_seed(v)
    
        serverKey = srp.server_session(user, password, salt, A, B, b)

        # Client send M to Server
        M, clientKey = srp.client_proof(user, password, salt, A, B, a)
    
        # Client and Server has same key
        self.assertEqual(clientKey, serverKey)

    def test_srp_wireencrypt(self):
        self.connection = firebirdsql.connect(
                auth_plugin_name="Srp", 
                wire_crypt=True,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)
        # FB3
        # self.assertEqual(self.connection.accept_plugin_name, 'Srp')
        self.connection.close()

    def test_srp_no_wirecrypt(self):
        self.connection = firebirdsql.connect(
                auth_plugin_name="Srp", 
                wire_crypt=False,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)
        self.connection.close()

    def test_legacy_auth_wirecrypt(self):
        self.connection = firebirdsql.connect(
                auth_plugin_name="Legacy_Auth", 
                wire_crypt=True,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)
        # FB3
        # self.assertEqual(self.connection.accept_plugin_name, '')
        self.connection.close()

    def test_legacy_auth_no_wirecrypt(self):
        self.connection = firebirdsql.connect(
                auth_plugin_name="Legacy_Auth", 
                wire_crypt=False,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)
        self.connection.close()

