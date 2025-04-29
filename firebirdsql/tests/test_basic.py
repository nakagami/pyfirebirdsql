from __future__ import with_statement
import sys
import datetime
from decimal import Decimal
import firebirdsql
from firebirdsql.tests.base import *    # noqa
from firebirdsql.consts import *        # noqa


class TestBasic(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        cur = self.connection.cursor()

        cur.execute(
            "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database"
        )
        self.server_version = tuple(
            [int(n) for n in cur.fetchone()[0].split('.')]
        )

        cur.execute('''
            CREATE TABLE foo (
                a INTEGER NOT NULL,
                b VARCHAR(30) NOT NULL UNIQUE,
                c VARCHAR(1024),
                d DECIMAL(16,3) DEFAULT -0.123,
                e DATE DEFAULT '1967-08-11',
                f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
                g TIME DEFAULT '23:45:01',
                h BLOB SUB_TYPE 1,
                i DOUBLE PRECISION DEFAULT 0.0,
                j FLOAT DEFAULT 0.0,
                k BIGINT DEFAULT 123456789999999999,
                PRIMARY KEY (a),
                CONSTRAINT CHECK_A CHECK (a <> 0)
            )
        ''')
        cur.execute('''
            CREATE TABLE bar_empty (
                k INTEGER NOT NULL,
                abcdefghijklmnopqrstuvwxyz INTEGER
            )
        ''')
        self.connection.commit()

    def test_basic(self):
        conn = self.connection

        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(cur.fetchone(), None)
        cur.close()

        cur = conn.cursor()
        cur.execute("select a as alias_name from foo")
        assert cur.description[0][0] == 'ALIAS_NAME'
        cur.close()

        # 3 records insert
        conn.cursor().execute("insert into foo(a, b, c,h) values (1, 'a', 'b','This is a memo')")
        conn.cursor().execute("""insert into foo(a, b, c, e, g, i, j)
            values (2, 'A', 'B', '1999-01-25', '00:00:01', 0.1, 0.1)""")
        conn.cursor().execute("""insert into foo(a, b, c, e, g, i, j, k)
            values (3, 'X', 'Y', '2001-07-05', '00:01:02', 0.2, 0.2, 123456789000000000)""")

        # 1 record insert and rollback to savepoint
        cur = conn.cursor()
        conn.savepoint('abcdefghijklmnopqrstuvwxyz')
        cur.execute("""insert into foo(a, b, c, e, g, i, j)
            values (4, 'x', 'y', '1967-05-08', '00:01:03', 0.3, 0.3)""")
        conn.rollback(savepoint='abcdefghijklmnopqrstuvwxyz')

        conn.cursor().execute("update foo set c='Hajime' where a=1")
        conn.cursor().execute("update foo set c=? where a=2", ('Nakagami', ))
        conn.commit()

        # select rowcount
        cur = conn.cursor()
        cur.execute("select * from foo where c=?", ('Nakagami', ))
        self.assertEqual(cur.rowcount, 1)
        self.assertEqual(len(cur.fetchall()), 1)
        cur.close()
        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(cur.rowcount, 3)
        self.assertEqual(len(cur.fetchall()), 3)
        cur.close()

        # update rowcount
        cur = conn.cursor()
        cur.execute("update foo set c=? where a=2", ('Nakagami', ))
        self.assertEqual(cur.rowcount, 1)
        conn.commit()

        cur = conn.cursor()
        cur.execute("select * from foo")
        assert not cur.fetchone() is None
        assert not cur.fetchone() is None
        assert not cur.fetchone() is None
        assert cur.fetchone() is None
        cur.close()

        cur = conn.cursor()
        cur.execute("select * from foo order by a")
        self.assertEqual(len(cur.fetchmany(2)), 2)
        cur.close()

        cur = conn.cursor()
        cur.execute("""insert into foo(a, b, c, e, g, i, j)
            values (5, 'c', 'c', '2014-02-19', '00:01:05', 0.5, 0.5)""")
        self.assertEqual(cur.fetchone(), None)
        cur.close()

        cur = conn.cursor()
        cur.execute("""insert into foo(a, b, c, e, g, i, j)
            values (6, 'd', 'd', '2014-02-19', '00:01:06', 0.6, 0.6)""")
        self.assertEqual(cur.fetchmany(), [])
        cur.close()

        cur = conn.cursor()
        cur.execute("select * from foo")
        conn.commit()
        try:
            cur.fetchall()
        except firebirdsql.OperationalError:
            pass

        cur = conn.cursor()
        try:
            conn.cursor().execute("insert into foo(a, b, c) values (1, 'a', 'b')")
        except firebirdsql.IntegrityError:
            pass
        try:
            conn.cursor().execute("bad sql")
        except firebirdsql.OperationalError:
            e = sys.exc_info()[1]
            self.assertEqual(e.sql_code, -104)

        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(
            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'],
            [d[0] for d in cur.description]
        )
        self.assertEqual(['a', 'A', 'X', 'c', 'd'], [r[1] for r in cur.fetchall()])

        cur.execute("select * from foo")
        self.assertEqual(
            ['a', 'A', 'X', 'c', 'd'],
            [r['B'] for r in cur.fetchallmap()]
        )

        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual({
            'A': 1,
            'B': 'a',
            'C': 'Hajime',
            'D': Decimal('-0.123'),
            'E': datetime.date(1967, 8, 11),
            'F': datetime.datetime(1967, 8, 11, 23, 45, 1),
            'G': datetime.time(23, 45, 1),
            'H': 'This is a memo',
            'I': 0.0,
            'J': 0.0,
            'K': 123456789999999999},
            dict(cur.fetchonemap())
        )

        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(
            ['a', 'A', 'X', 'c', 'd'],
            [r['B'] for r in cur.itermap()]
        )

        cur = conn.cursor()
        cur.execute("select a from foo where k=?", [123456789000000000])
        self.assertEqual(cur.fetchone()[0], 3)

        cur = conn.cursor()
        cur.execute("select * from bar_empty")
        self.assertEqual([], [r for r in cur.fetchonemap().items()])

        cur = conn.cursor()
        cur.execute("select * from foo")
        rs = [r for r in cur]
        self.assertEqual(rs[0][:3], (1, 'a', 'Hajime'))
        self.assertEqual(rs[1][:3], (2, 'A', 'Nakagami'))
        self.assertEqual(rs[2][:3], (3, 'X', 'Y'))

        cur = conn.cursor()
        cur.execute("select rdb$field_name from rdb$relation_fields where rdb$field_name='ABCDEFGHIJKLMNOPQRSTUVWXYZ'")
        v = cur.fetchone()[0]
        self.assertEqual(v.strip(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')

        conn.close()

        with firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            database=self.database,
            port=self.port,
            user=self.user,
            password=self.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("update foo set c='Toshihide' where a=1")

        conn = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            database=self.database,
            port=self.port,
            user=self.user,
            password=self.password)
        conn.begin()

        requests = [
            isc_info_ods_version,
            isc_info_ods_minor_version,
            isc_info_base_level,
            isc_info_db_id,
            isc_info_implementation,
            isc_info_firebird_version,
            isc_info_user_names,
            isc_info_read_idx_count,
            isc_info_creation_date,
        ]
        self.assertEqual(9, len(conn.db_info(requests)))

        requests = [
            isc_info_tra_id,
            isc_info_tra_oldest_interesting,
            isc_info_tra_oldest_snapshot,
            isc_info_tra_oldest_active,
            isc_info_tra_isolation,
            isc_info_tra_access,
            isc_info_tra_lock_timeout,
        ]
        self.assertEqual(7, len(conn.trans_info(requests)))

        conn.set_isolation_level(firebirdsql.ISOLATION_LEVEL_SERIALIZABLE)
        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(
            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'],
            [d[0] for d in cur.description]
        )
        self.assertEqual(
            ['a', 'A', 'X', 'c', 'd'],
            [r[1] for r in cur.fetchall()]
        )
        cur.close()

    def test_autocommit(self):
        # autocommit
        cur = self.connection.cursor()
        cur.execute("select count(*) from foo")
        self.assertEqual(cur.fetchone()[0], 0)
        cur.close()

        self.connection.set_autocommit(True)
        cur = self.connection.cursor()
        cur.execute("insert into foo(a, b, c) values (1, 'A', 'a')")
        cur.close()
        cur = self.connection.cursor()
        cur.execute("select count(*) from foo")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.close()

        self.connection.set_autocommit(False)
        cur = self.connection.cursor()
        cur.execute("insert into foo(a, b, c) values (2, 'B', 'b')")
        self.connection.rollback()
        cur.close()
        cur = self.connection.cursor()
        cur.execute("select count(*) from foo")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.close()

        self.connection.set_autocommit(True)
        cur = self.connection.cursor()
        cur.execute("insert into foo(a, b, c) values (3, 'C', 'c')")
        self.connection.rollback()
        cur.close()
        cur = self.connection.cursor()
        cur.execute("select count(*) from foo")
        self.assertEqual(cur.fetchone()[0], 2)
        cur.close()

    def test_prep(self):
        cur = self.connection.cursor()
        prep = cur.prep("select * from foo where c=?", explain_plan=True)
        self.assertEqual(prep.sql, 'select * from foo where c=?')
        self.assertEqual(prep.stmt.stmt_type, 1)
        self.assertEqual(prep.n_output_params, 11)

        cur.execute(prep, ('C parameter', ))
        self.assertEqual(0, len(cur.fetchall()))
        cur.close()

    def test_error(self):
        cur = self.connection.cursor()
        try:
            # table foo is already exists.
            cur.execute("CREATE TABLE foo (a INTEGER)")
        except firebirdsql.OperationalError:
            pass

    def test_null_parameter(self):
        cur = self.connection.cursor()
        cur.execute("insert into foo(a, b, c) values (1, 'B', ?)", (None,))
        cur.execute("select count(*) from foo where c is null")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.close()

    def test_execute_immediate(self):
        self.connection.execute_immediate(
            "insert into foo(a, b) values (1, 'B')")

    def test_blob(self):
        def _test_length(cur, ln):
            cur.execute("update blob0_test set b = ?",  (b'x' * ln, ))
            cur.execute("select * from blob0_test")
            self.assertEqual(cur.fetchone()[0], b'x' * ln)
            cur.execute("update blob1_test set b = ?",  ('x' * ln, ))
            cur.execute("select * from blob1_test")
            self.assertEqual(cur.fetchone()[0], 'x' * ln)

        cur = self.connection.cursor()
        cur.execute("CREATE TABLE blob0_test (b BLOB SUB_TYPE 0)")  # BINARY
        cur.execute("CREATE TABLE blob1_test (b BLOB SUB_TYPE 1)")  # TEXT
        cur.close()
        self.connection.commit()
        cur = self.connection.cursor()
        cur.execute("insert into blob0_test(b) values ('abc')")
        cur.execute("insert into blob1_test(b) values ('abc')")
        cur.close()

        cur = self.connection.cursor()
        cur.execute("select * from blob0_test")
        self.assertEqual(cur.fetchone()[0], b'abc')
        cur.execute("select * from blob1_test")
        self.assertEqual(cur.fetchone()[0], 'abc')

        _test_length(cur, 1)
        _test_length(cur, 2)
        _test_length(cur, 3)
        _test_length(cur, MAX_CHAR_LENGTH)
        _test_length(cur, MAX_CHAR_LENGTH+1)
        _test_length(cur, MAX_CHAR_LENGTH+2)
        _test_length(cur, MAX_CHAR_LENGTH+3)

        self.connection.close()

    def test_boolean(self):
        if self.server_version[0] < 3:
            self.connection.close()
            return

        cur = self.connection.cursor()
        cur.execute("CREATE TABLE boolean_test (b BOOLEAN)")
        cur.close()
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("insert into boolean_test(b) values (true)")
        cur.execute("insert into boolean_test(b) values (false)")
        cur.close()

        cur = self.connection.cursor()
        cur.execute("select * from boolean_test where b is true")
        self.assertEqual(cur.fetchone()[0], True)
        cur.close()

        cur = self.connection.cursor()
        cur.execute("select * from boolean_test where b is false")
        self.assertEqual(cur.fetchone()[0], False)
        cur.close()

        cur = self.connection.cursor()
        cur.execute("select * from boolean_test where b = ?", (True,))
        self.assertEqual(cur.fetchone()[0], True)

        cur.execute("select * from boolean_test where b = ?", (False, ))
        self.assertEqual(cur.fetchone()[0], False)
        cur.close()

        self.connection.close()

    def test_ping(self):
        if self.server_version[0] >= 3:
            self.assertEqual(self.connection.ping(), True)
        self.connection.close()

    def test_decfloat(self):
        if self.server_version[0] < 4:
            self.connection.close()
            return

        cur = self.connection.cursor()
        cur.execute('''
            CREATE TABLE dec_test (
                d DECIMAL(20, 2),
                df64 DECFLOAT(16),
                df128 DECFLOAT(34),
                s varchar(32))
        ''')
        cur.close()
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("insert into dec_test(d, df64, df128, s) values (0.0, 0.0, 0.0, '0.0')")
        cur.execute("insert into dec_test(d, df64, df128, s) values (1.1, 1.1, 1.1, '1.1')")
        cur.execute("insert into dec_test(d, df64, df128, s) values (20.2, 20.2, 20.2, '20.2')")
        cur.execute("insert into dec_test(d, df64, df128, s) values (-1.1, -1.1, -1.1, '-1.1')")
        cur.execute("insert into dec_test(d, df64, df128, s) values (-20.2, -20.2, -20.2, '-20.2')")
        cur.close()

        cur = self.connection.cursor()
        cur.execute("select * from dec_test")
        for d, df64, df128, s in cur.fetchall():
            val = Decimal(s)
            self.assertEqual(d, val)
            self.assertEqual(df64, val)
            self.assertEqual(df128, val)
        cur.close()

        self.connection.close()
