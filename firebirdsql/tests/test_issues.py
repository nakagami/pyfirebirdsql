import firebirdsql
import unittest
import tempfile

class TestIssues(unittest.TestCase):
    def setUp(self):
        self.connection = firebirdsql.create_database(
                host='localhost',
                port=3050,
                database=tempfile.mktemp(),
                user='sysdba',
                password='masterkey',
                page_size=2<<13)

    def test_issue_39(self):
        """
        .description attribute should be None when .execute has not run yet
        """
        cur = self.connection.cursor()
        self.assertEqual(None, cur.description)


    def test_issue_40(self):
        cur = self.connection.cursor()

    def tearDown(self):
        self.connection.close()

