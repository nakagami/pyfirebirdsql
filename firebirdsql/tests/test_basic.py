import firebirdsql
import unittest
import tempfile
from firebirdsql.tests import base

class TestBasic(base.TestBase):
    def test_prep(self):
        cur = self.connection.cursor()
        prep = cur.prep("select * from foo where c=?", explain_plan=True)
        self.assertEqual(prep.sql, 'select * from foo where c=?')
        self.assertEqual(prep.statement_type, 1)
        self.assertEqual(prep.n_output_params, 10)

        cur.execute(prep, ('C parameter', ))
        cur.close()

