import firebirdsql
import unittest
import tempfile

class TestBase(unittest.TestCase):
    host='localhost'
    port=3050
    user='sysdba'
    password='masterkey'
    page_size=2<<13

    def setUp(self):
        self.database=tempfile.mktemp()
        self.connection = firebirdsql.create_database(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)

    def tearDown(self):
        self.connection.close()
