from __future__ import with_statement
import tempfile
import datetime

import firebirdsql
from firebirdsql.tests.base import *    # noqa
from firebirdsql.consts import *        # noqa


class TestTimeZone(TestBase):
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
                timezone='Asia/Tokyo')
        cur = self.connection.cursor()

        cur.execute(
            "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database"
        )
        self.server_version = tuple(
            [int(n) for n in cur.fetchone()[0].split('.')]
        )
        cur.close()

    def test_timezone(self):
        """
        TimeZone data tests
        """
        # Firebird4 only
        if self.server_version[0] < 4:
            self.connection.close()
            return
        try:
            firebirdsql.tz_utils.get_tzinfo_by_name('GMT')
        except Exception:
            self.connection.close()
            return


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

        tzinfo = firebirdsql.tz_utils.get_tzinfo_by_name('Asia/Seoul')
        cur.execute(
            "insert into tz_test (id, t, ts) values (2, ?, ?)", [
                datetime.time(12, 34, 56, tzinfo=tzinfo),
                datetime.datetime(1967, 8, 11, 23, 45, 1, tzinfo=tzinfo)
            ]
        )

        tzinfo = firebirdsql.tz_utils.get_tzinfo_by_name('UTC')
        cur.execute(
            "insert into tz_test (id, t, ts) values (3, ?, ?)", [
                datetime.time(3, 34, 56, tzinfo=tzinfo),
                datetime.datetime(1967, 8, 11, 14, 45, 1, tzinfo=tzinfo)
            ]
        )

        expected = [
            (
                1,
                datetime.time(12, 34, 56, tzinfo=firebirdsql.tz_utils.get_tzinfo_by_name('Asia/Tokyo')),
                datetime.datetime(1967, 8, 11, 23, 45, 1, tzinfo=firebirdsql.tz_utils.get_tzinfo_by_name('Asia/Tokyo')),
            ),
            (
                2,
                datetime.time(12, 34, 56, tzinfo=firebirdsql.tz_utils.get_tzinfo_by_name('Asia/Seoul')),
                datetime.datetime(1967, 8, 11, 23, 45, 1, tzinfo=firebirdsql.tz_utils.get_tzinfo_by_name('Asia/Seoul')),
            ),
            (
                3,
                datetime.time(3, 34, 56, tzinfo=firebirdsql.tz_utils.get_tzinfo_by_name('UTC')),
                datetime.datetime(1967, 8, 11, 14, 45, 1, tzinfo=firebirdsql.tz_utils.get_tzinfo_by_name('UTC')),
            ),
        ]
        cur = self.connection.cursor()
        cur.execute("select id, t, ts from tz_test order by id")
        self.assertEqual(cur.fetchall(), expected)

        self.connection.close()
