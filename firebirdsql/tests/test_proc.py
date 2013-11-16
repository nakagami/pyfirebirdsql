from __future__ import with_statement
import sys
import unittest
import tempfile
import datetime
from decimal import Decimal
import firebirdsql
from firebirdsql.tests import base
from firebirdsql.consts import *

class TestProc(base.TestBase):
    def test_call_proc(self):
        cur = self.connection.cursor()
        cur.callproc("foo_proc")
        self.assertEqual(cur.fetchone()[0], 1, 'ABC')
        cur.close()

    def test_insert_returning(self):
        cur = self.connection.cursor()
        cur.execute("insert into foo(a, b) values (1, 'b') returning e")
        self.assertEqual(cur.fetchone()[0], datetime.date(1967, 8, 11))
        cur.close()

    def test_prep_insert_returning(self):
        cur = self.connection.cursor()
        prep = cur.prep("insert into foo(a, b) values (?, 'b') returning e")
        cur.execute(prep, (2, ))
        self.assertEqual(cur.fetchone()[0], datetime.date(1967, 8, 11))
        cur.close()

