from __future__ import with_statement
import sys
import unittest
import tempfile
import datetime
import pytz

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
                timezone='Asia/Tokyo')

    def test_timezone(self):
        """
        For FB4
        """
        cur = self.connection.cursor()
        cur.execute('''
            CREATE TABLE tz_test (
                id INTEGER NOT NULL,
                t TIME WITH TIME ZONE DEFAULT '12:34:56',
                ts TIMESTAMP WITH TIME ZONE DEFAULT '1967-08-11 23:45:01',
                PRIMARY KEY (id)
            )
        ''')
        cur.close()
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("insert into tz_test (id) values (1)")

        tzinfo = pytz.timezone('Asia/Seoul')
        cur.execute(
            "insert into tz_test (id, t, ts) values (2, ?, ?)", [
                datetime.time(12, 34, 56, tzinfo=tzinfo),
                datetime.datetime(1967, 8, 11, 23, 45, 1, tzinfo=tzinfo)
            ]
        )

        tzinfo = pytz.timezone('UTC')
        cur.execute(
            "insert into tz_test (id, t, ts) values (3, ?, ?)", [
                datetime.time(3, 34, 56, tzinfo=tzinfo),
                datetime.datetime(1967, 8, 11, 14, 45, 1, tzinfo=tzinfo)
            ]
        )

        cur = self.connection.cursor()
        cur.execute("select t, ts from tz_test order by id")
        r1, r2, r3 = cur.fetchall()
        self.assertEqual(r1, r2)
        self.assertEqual(r2, r3)
        self.connection.close()

