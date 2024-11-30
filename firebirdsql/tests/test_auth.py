import unittest
import hashlib
from firebirdsql import srp
from firebirdsql.tests.base import *    # noqa


class TestAuth(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        cur = self.connection.cursor()

        cur.execute(
            "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database"
        )
        self.server_version = tuple(
            [int(n) for n in cur.fetchone()[0].split('.')]
        )

    def test_srp_key_exchange(self):
        user = b'sysdba'
        password = b'masterkey'
        hash_algo = hashlib.sha256

        # Client send A to Server
        A, a = srp.client_seed()

        # Server send B, salt to Client
        salt = srp.get_salt()
        v = srp.get_verifier(user, password, salt)
        B, b = srp.server_seed(v)

        serverKey = srp.server_session(user, password, salt, A, B, b)

        # Client send M to Server
        M, clientKey = srp.client_proof(user, password, salt, A, B, a, hash_algo)

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
        if self.server_version[0] >= 3:
            self.assertEqual(self.connection.accept_plugin_name, b'Srp')
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

    @unittest.skip("Fail on Github action")
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
        if self.server_version[0] >= 3:
           self.assertEqual(self.connection.accept_plugin_name, b'')
        self.connection.close()

    @unittest.skip("Fail on Github action")
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

    def test_unauthorized(self):
        with self.assertRaises(firebirdsql.OperationalError):
            self.connection = firebirdsql.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user="notexisting",
                    password="wrongpassword",
                    page_size=self.page_size)
