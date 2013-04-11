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
        cur.execute("SELECT RDB$INDEX_NAME FROM RDB$INDICES WHERE RDB$INDEX_NAME LIKE 'RDB$INDEX_%'")
        self.assertNotEqual(None, cur.fetchone())
        cur.close()
        cur = self.connection.cursor()
        cur.execute("SELECT RDB$INDEX_NAME FROM RDB$INDICES WHERE RDB$INDEX_NAME LIKE ?", ('RDB$INDEX_%', ))
        self.assertNotEqual(None, cur.fetchone())
        cur.close()

    def test_issue_41(self):
        self.connection.cursor().execute('''
              CREATE TABLE ISSUE41
              (
                  a       integer,
                  b       varchar(20)
              )
        ''')
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("insert into ISSUE41 (a, b) values (32767, 'FOO')")
        cur.execute("insert into ISSUE41 (a, b) values (32768, 'BAR')")
        cur.execute('''select count(*) from ISSUE41 WHERE A=?''',(32768, ))
        self.assertEqual(cur.fetchone()[0], 1)
        cur.close()

    def tearDown(self):
        self.connection.close()

