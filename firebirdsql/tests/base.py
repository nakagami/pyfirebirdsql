import os
import unittest
import tempfile

import firebirdsql

# backwards compatibility:
if not hasattr(unittest, "skip"):
    def _empty(func):
        pass

    def _skip(message):
        return _empty

    unittest.skip = _skip


class TestBase(unittest.TestCase):
    auth_plugin_name = 'Srp'
    wire_crypt = True
    host = 'localhost'
    port = 3050
    user = os.environ.get("ISC_USER", "sysdba")
    password = os.environ.get("ISC_PASSWORD", "masterkey")
    page_size = 2 << 13

    def setUp(self):
        self.database = tempfile.mktemp()
        self.connection = firebirdsql.create_database(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
                timeout=1)

    def tearDown(self):
        self.connection.close()
