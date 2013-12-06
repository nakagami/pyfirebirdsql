from __future__ import with_statement
import sys
import unittest
import tempfile
import datetime
from decimal import Decimal
import firebirdsql
from firebirdsql.tests import base
from firebirdsql.consts import *

def b(s):
    if sys.version_info[0] == 3:
        return bytes(s, 'utf8')
    return s

# backwards compatibility:
if not hasattr(unittest, "skip"):
    def _empty(func):
        pass
    def _skip(message):
        return _empty
    unittest.skip = _skip


class TestBasic(base.TestBase):
    def setUp(self):
        base.TestBase.setUp(self)
        cur = self.connection.cursor()
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
        conn.cursor().execute("""insert into foo(a, b, c, e, g, i, j) 
            values (3, 'X', 'Y', '2001-07-05', '00:01:02', 0.2, 0.2)""")


        # 1 record insert and rollback to savepoint
        cur = conn.cursor()
        conn.savepoint('abcdefghijklmnopqrstuvwxyz')
        cur.execute("""insert into foo(a, b, c, e, g, i, j) 
            values (4, 'x', 'y', '1967-05-08', '00:01:03', 0.3, 0.3)""")
        conn.rollback(savepoint='abcdefghijklmnopqrstuvwxyz')

        conn.cursor().execute("update foo set c='Hajime' where a=1")
        conn.cursor().execute("update foo set c=? where a=2", ('Nakagami', ))
        conn.commit()

        cur = conn.cursor()
        cur.execute("select * from foo where c=?", ('Nakagami', ))
        len(cur.fetchall()) == 1
        cur.close()

        cur = conn.cursor()
        cur.execute("select * from foo")
        assert not cur.fetchone() is None
        assert not cur.fetchone() is None
        assert not cur.fetchone() is None
        assert cur.fetchone() is None
        cur.close()

        cur = conn.cursor()
        cur.execute("select * from foo")
        conn.commit()
        try:
            list(cur.fetchall())
        except firebirdsql.OperationalError:
            e = sys.exc_info()[1]
            self.assertTrue(
                e.sql_code == -504           # FB2.1 cursor is not open
                or 335544332 in e.gds_codes) # FB2.5 invalid transaction handle

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
        self.assertEqual(['A','B','C','D','E','F','G','H','I','J'],
                        [d[0] for d in cur.description])
        self.assertEqual(['a','A','X'], [r[1] for r in cur.fetchall()])

        cur.execute("select * from foo")
        self.assertEqual(['a','A','X'], [r['B'] for r in cur.fetchallmap()])

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
            'J': 0.0},
            dict(cur.fetchonemap())
        )

        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(['a','A','X'], [r['B'] for r in cur.itermap()])

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

        with firebirdsql.connect(host=self.host,
                                    database=self.database,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute("update foo set c='Toshihide' where a=1")

        conn = firebirdsql.connect(host=self.host,
                                    database=self.database,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password)
        conn.begin()

        requests = [isc_info_ods_version,
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
   
        requests = [isc_info_tra_id, 
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
        self.assertEqual(['A','B','C','D','E','F','G','H','I','J'],
                        [d[0] for d in cur.description])
        self.assertEqual(['a','A','X'], [r[1] for r in cur.fetchall()])

    def test_prep(self):
        cur = self.connection.cursor()
        prep = cur.prep("select * from foo where c=?", explain_plan=True)
        self.assertEqual(prep.sql, 'select * from foo where c=?')
        self.assertEqual(prep.statement_type, 1)
        self.assertEqual(prep.n_output_params, 10)

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

    def test_execute_immediate(self):
        self.connection.execute_immediate(
            "insert into foo(a, b) values (1, 'B')")

    def test_blob(self):
        cur = self.connection.cursor()
        cur.execute("CREATE TABLE blob0_test (b BLOB SUB_TYPE 0)") # BINARY
        cur.execute("CREATE TABLE blob1_test (b BLOB SUB_TYPE 1)") # TEXT
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

        cur.execute("update blob0_test set b = ?",  (b'x' * 0xffff, ))
        cur.execute("select * from blob0_test")
        self.assertEqual(cur.fetchone()[0], b'x' * 0xffff)

        cur.execute("update blob1_test set b = ?",  ('x' * 0xffff, ))
        cur.execute("select * from blob1_test")
        self.assertEqual(cur.fetchone()[0], 'x' * 0xffff)

        self.connection.close()

    @unittest.skip("FB 3")
    def test_boolean(self):
        """
        For FB3
        """
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
        prep = cur.prep("select * from boolean_test where b = ?")
        cur.execute(prep, (True, ))
        self.assertEqual(cur.fetchone()[0], True)
        cur.execute(prep, (False, ))
        self.assertEqual(cur.fetchone()[0], False)
        cur.close()

        self.connection.close()

