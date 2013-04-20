from __future__ import with_statement
import sys
import unittest
import tempfile
import firebirdsql
from firebirdsql.tests import base
from firebirdsql.consts import *

class TestBasic(base.TestBase):
    def test_basic(self):
        conn = self.connection

        cur = conn.cursor()
        cur.execute("select * from foo")
        self.assertEqual(cur.fetchone(), None)
        cur.close()

        cur = conn.cursor()

        self.assertEqual(None, cur.callproc("foo_proc"))
        cur.close()

        cur = conn.cursor()
        try:
            rs = cur.execute("select out1, out2 from foo_proc")
            if rs is None:  # FB1.5
                print('foo_proc not selectable')
            else:
                for r in rs:
                    print(r)
        except firebirdsql.OperationalError:    # FB2
            print('foo_proc not selectable')
        finally:
            cur.close()

        cur = conn.cursor()
        cur.callproc("bar_proc", (1, "ABC"))
        rs = cur.fetchallmap()
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['OUT1'], 1)
        self.assertEqual(rs[0]['OUT2'], 'ABC')
   
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

        cur = conn.cursor()
        cur.execute("select out1, out2 from baz_proc(?)", (1, ))
        rs = cur.fetchall()
        self.assertEqual(len(rs), 1)
        self.assertEqual([1, 'a'], rs[0])
        cur.close()

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
            for r in cur.fetchall():
                print(r)
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
        for c in cur.fetchall():
            print(c)

        print('fetchallmap()')
        cur.execute("select * from foo")
        for r in cur.fetchallmap():
            print(r)
            for key in r:
                print (key, r[key])

        print('fetchonemap()')
        cur = conn.cursor()
        cur.execute("select * from foo")
        for k,v in cur.fetchonemap().items():
            print(k, v)

        print('itermap()')
        cur = conn.cursor()
        cur.execute("select * from foo")
        for r in cur.itermap():
            print(r['a'], r['b'], r['c'])
   
        print('fetchonemap() empty record')
        cur = conn.cursor()
        cur.execute("select * from bar_empty")
        for k,v in cur.fetchonemap().items():
            print(k, v)

        print('cursor iteration')
        cur = conn.cursor()
        cur.execute("select * from foo")
        for (a, b, c, d, e, f, g, h, i, j) in cur:
            print(a, b, c)

        print('long field name from system table')
        cur = conn.cursor()
        cur.execute("select rdb$field_name from rdb$relation_fields where rdb$field_name='ABCDEFGHIJKLMNOPQRSTUVWXYZ'")
        v = cur.fetchone()[0]
        assert v.strip() == 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

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

        print('db_info:')
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
        print(conn.db_info(requests))
   
        print('trans_info:')
        requests = [isc_info_tra_id, 
                    isc_info_tra_oldest_interesting,
                    isc_info_tra_oldest_snapshot,
                    isc_info_tra_oldest_active,
                    isc_info_tra_isolation,
                    isc_info_tra_access,
                    isc_info_tra_lock_timeout,
        ]
        print(conn.trans_info(requests))
    
        conn.set_isolation_level(firebirdsql.ISOLATION_LEVEL_SERIALIZABLE)
        cur = conn.cursor()
        cur.execute("select * from foo")
        print(cur.description)
        for c in cur.fetchall():
            print(c)

    def test_prep(self):
        cur = self.connection.cursor()
        prep = cur.prep("select * from foo where c=?", explain_plan=True)
        self.assertEqual(prep.sql, 'select * from foo where c=?')
        self.assertEqual(prep.statement_type, 1)
        self.assertEqual(prep.n_output_params, 10)

        cur.execute(prep, ('C parameter', ))
        print(cur.fetchall())
        cur.close()

