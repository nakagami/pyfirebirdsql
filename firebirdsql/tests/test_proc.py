from __future__ import with_statement
import datetime
import firebirdsql
from firebirdsql.tests.base import *    # noqa
from firebirdsql.consts import *        # noqa


class TestProc(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        cur = self.connection.cursor()
        cur.execute('''
            CREATE TABLE foo_table (
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
            CREATE PROCEDURE foo_proc
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                out1 = 1;
                out2 = 'ABC';
              END
        ''')
        cur.execute('''
            CREATE PROCEDURE bar_proc (param_a INTEGER, param_b VARCHAR(30))
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                out1 = param_a;
                out2 = param_b;
              END
        ''')
        cur.execute('''
            CREATE PROCEDURE baz_proc(param_a INTEGER)
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                SELECT a, b FROM foo_table
                  WHERE a= :param_a
                  INTO :out1, :out2;
                SUSPEND;
              END
        ''')
        self.connection.commit()

        # 3 records insert
        cur.execute("""
            insert into foo_table(a, b, c,h)
                values (1, 'a', 'b','This is a memo')""")
        cur.execute("""
            insert into foo_table(a, b, c, e, g, i, j)
                values (2, 'A', 'B', '1999-01-25', '00:00:01', 0.1, 0.1)""")
        cur.execute("""
            insert into foo_table(a, b, c, e, g, i, j)
                values (3, 'X', 'Y', '2001-07-05', '00:01:02', 0.2, 0.2)""")
        self.connection.commit()

    def test_call_proc(self):
        cur = self.connection.cursor()
        r = cur.callproc("foo_proc")
        self.assertEqual(cur.fetchone(), r)
        cur.close()

        cur = self.connection.cursor()
        try:
            rs = cur.execute("select out1, out2 from foo_proc")
            if rs is None:
                # foo_proc not selectable with Firebird 1.5
                pass
            else:
                pass
        except firebirdsql.OperationalError:
            # foo_proc not selectable with Firebird 2.x
            pass
        finally:
            cur.close()

        cur = self.connection.cursor()
        cur.callproc("bar_proc", (1, "ABC"))
        rs = cur.fetchallmap()
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['OUT1'], 1)
        self.assertEqual(rs[0]['OUT2'], 'ABC')
        cur.close()

        cur = self.connection.cursor()
        cur.execute("select out1, out2 from baz_proc(?)", (1, ))
        rs = cur.fetchall()
        self.assertEqual(len(rs), 1)
        self.assertEqual((1, 'a'), rs[0])
        cur.close()

    def test_insert_returning(self):
        cur = self.connection.cursor()
        cur.execute("insert into foo_table(a, b) values (4, 'b') returning e")
        self.assertEqual(cur.rowcount, 1)
        self.assertEqual(cur.fetchone()[0], datetime.date(1967, 8, 11))
        cur.close()

    def test_prep_insert_returning(self):
        cur = self.connection.cursor()
        prep = cur.prep("insert into foo_table(a, b) values (?, 'b') returning e")
        cur.execute(prep, (5, ))
        self.assertEqual(cur.fetchone()[0], datetime.date(1967, 8, 11))
        cur.close()
