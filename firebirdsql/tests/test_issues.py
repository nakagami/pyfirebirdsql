import firebirdsql
import unittest
import tempfile
from firebirdsql.tests import base

class TestIssues(base.TestBase):
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
              CREATE TABLE issue_41
              (
                  a       INTEGER,
                  b       VARCHAR(20)
              )
        ''')
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("INSERT INTO issue_41 (a, b) VALUES (32767, 'FOO')")
        cur.execute("INSERT INTO issue_41 (a, b) VALUES (32768, 'BAR')")
        cur.close()
        cur = self.connection.cursor()
        cur.execute('''SELECT b FROM issue_41 WHERE a=?''',(32767, ))
        self.assertEqual(cur.fetchone()[0], 'FOO')
        cur.close()
        cur = self.connection.cursor()
        cur.execute('''SELECT b from issue_41 WHERE a=?''',(32768, ))
        self.assertEqual(cur.fetchone()[0], 'BAR')
        cur.close()

    def tearDown(self):
        self.connection.close()

