from __future__ import with_statement
import sys
import unittest
import tempfile
import datetime
from decimal import Decimal
import firebirdsql
from firebirdsql.tests.base import *
from firebirdsql.consts import *


class TestTimeZone(TestBase):
    def setUp(self):
        self.database=tempfile.mktemp()
        self.connection = firebirdsql.create_database(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
                tz_name='Asia/Tokyo')

    @unittest.skip("FB 4")
    def test_time_zone(self):
        """
        For FB4
        """
        cur = self.connection.cursor()
        cur.execute('''
            CREATE TABLE tz_test (
                a INTEGER NOT NULL,
                b TIME WITH TIME ZONE DEFAULT '12:34:56',
                c TIMESTAMP WITH TIME ZONE DEFAULT '1967-08-11 23:45:01',
                PRIMARY KEY (a)
            )
        ''')
        cur.close()
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("insert into tz_test (a) values (1)")

        cur = self.connection.cursor()
        cur.execute("select * from tz_test")
        for a, b, c in cur.fetchall():
            pass

        self.connection.close()

