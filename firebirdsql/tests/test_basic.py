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

    def test_returning(self):
        # Test RETURNING clause with DML statements
        cur = self.connection.cursor()
        
        # INSERT with RETURNING
        cur.execute(
            "insert into foo(a, b, c) values (1, 'A', 'test') RETURNING a, b, c"
        )
        result = cur.fetchone()
        self.assertEqual(result, (1, 'A', 'test'))
        cur.close()
        
        # INSERT with RETURNING - partial columns
        cur = self.connection.cursor()
        cur.execute(
            "insert into foo(a, b, c) values (2, 'B', 'test2') RETURNING a, b"
        )
        result = cur.fetchone()
        self.assertEqual(result, (2, 'B'))
        cur.close()
        
        self.connection.commit()
        
        # UPDATE with RETURNING
        cur = self.connection.cursor()
        cur.execute(
            "update foo set c='updated' where a=1 RETURNING a, b, c"
        )
        result = cur.fetchone()
        self.assertEqual(result, (1, 'A', 'updated'))
        cur.close()
        
        # UPDATE with RETURNING using parameters
        cur = self.connection.cursor()
        cur.execute(
            "update foo set c=? where a=? RETURNING a, c",
            ('param_updated', 2)
        )
        result = cur.fetchone()
        self.assertEqual(result, (2, 'param_updated'))
        cur.close()
        
        self.connection.commit()
        
        # DELETE with RETURNING
        cur = self.connection.cursor()
        cur.execute("delete from foo where a=1 RETURNING a, b, c")
        result = cur.fetchone()
        self.assertEqual(result, (1, 'A', 'updated'))
        cur.close()
        
        # Verify the record was deleted
        cur = self.connection.cursor()
        cur.execute("select count(*) from foo where a=1")
        self.assertEqual(cur.fetchone()[0], 0)
        cur.close()
        
        self.connection.commit()

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

    def test_selectable_proc_exception_propagation(self):
        """
        When a server-side exception occurs after at least one row has been
        returned from a selectable procedure, the actual Firebird error message
        should be propagated to the caller instead of an opaque internal error.
        """
        cur = self.connection.cursor()
        cur.execute("CREATE EXCEPTION EX_PROC_ERR 'proc error message'")
        cur.execute("""
            CREATE PROCEDURE PROC_WITH_EXCEPTION
            RETURNS (N INTEGER)
            AS
            BEGIN
              N = 1;
              SUSPEND;
              EXCEPTION EX_PROC_ERR 'proc error message';
            END""")
        self.connection.commit()

        # The server-side exception may surface at execute() or fetchone() time
        # depending on how the server batches op_fetch_response / op_response.
        try:
            cur.execute("SELECT * FROM PROC_WITH_EXCEPTION")
            row = cur.fetchone()
            self.assertEqual(row[0], 1)
            cur.fetchone()  # should raise the server exception
            self.fail("Expected a server exception to be raised")
        except AssertionError:
            raise
        except Exception as e:
            err_msg = str(e)
            self.assertNotIn("opFetchResponse:Internal Error", err_msg,
                "error should not be the generic internal error fallback")
            self.assertIn("proc error message", err_msg,
                "error should contain the actual Firebird exception message")

    def test_statement_timeout(self):
        if self.server_version[0] < 4:
            self.connection.close()
            return

        cur = self.connection.cursor()
        # Fast query with timeout should succeed
        cur.execute("SELECT 1 FROM RDB$DATABASE", timeout=5.0)
        self.assertEqual(cur.fetchone()[0], 1)

        # Slow query with short timeout should fail with OperationalError
        with self.assertRaises(firebirdsql.OperationalError):
            cur.execute(
                "EXECUTE BLOCK AS DECLARE VARIABLE I INT = 0; BEGIN WHILE (I < 50000000) DO I = I + 1; END",
                timeout=0.2
            )
        self.connection.rollback()

        # Cursor-level statement timeout
        cur2 = self.connection.cursor(timeout=0.2)
        self.assertEqual(cur2.timeout, 0.2)
        with self.assertRaises(firebirdsql.OperationalError):
            cur2.execute("EXECUTE BLOCK AS DECLARE VARIABLE I INT = 0; BEGIN WHILE (I < 50000000) DO I = I + 1; END")
        self.connection.rollback()
        cur2.close()

        # Connection-level statement timeout
        conn2 = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            database=self.database,
            port=self.port,
            user=self.user,
            password=self.password,
            statement_timeout=0.2,
        )
        cur3 = conn2.cursor()
        with self.assertRaises(firebirdsql.OperationalError):
            cur3.execute("EXECUTE BLOCK AS DECLARE VARIABLE I INT = 0; BEGIN WHILE (I < 50000000) DO I = I + 1; END")
        conn2.rollback()
        conn2.close()

    def test_executemany_batch(self):
        if self.server_version[0] < 4:
            self.connection.close()
            return

        cur = self.connection.cursor()
        cur.execute("""
            CREATE TABLE batch_tbl (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                val DECIMAL(10, 2),
                dt DATE,
                tm TIME,
                ts TIMESTAMP,
                flag BOOLEAN
            )
        """)
        self.connection.commit()

        # Insert 150 records using Batch Execution API
        records = [
            (
                i,
                f"Item_{i}",
                Decimal(f"{i}.95"),
                datetime.date(2025, 1, 1),
                datetime.time(12, 0, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 0),
                i % 2 == 0,
            )
            for i in range(150)
        ]
        cur.executemany(
            "INSERT INTO batch_tbl (id, name, val, dt, tm, ts, flag) VALUES (?, ?, ?, ?, ?, ?, ?)",
            records
        )
        self.assertEqual(cur.rowcount, 150)
        self.connection.commit()

        cur.execute("SELECT COUNT(*) FROM batch_tbl")
        self.assertEqual(cur.fetchone()[0], 150)

        # Verify selected data matches
        cur.execute("SELECT name, val, flag FROM batch_tbl WHERE id = 10")
        row = cur.fetchone()
        self.assertEqual(row, ("Item_10", Decimal("10.95"), True))

        # Insert record with NULLs
        cur.executemany(
            "INSERT INTO batch_tbl (id, name, val, dt, tm, ts, flag) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(150, None, None, None, None, None, None)]
        )
        self.assertEqual(cur.rowcount, 1)
        self.connection.commit()

        cur.execute("SELECT name, val, flag FROM batch_tbl WHERE id = 150")
        self.assertEqual(cur.fetchone(), (None, None, None))

        # Batch UPDATE
        update_data = [(f"Updated_{i}", i) for i in range(10)]
        cur.executemany("UPDATE batch_tbl SET name = ? WHERE id = ?", update_data)
        self.assertEqual(cur.rowcount, 10)
        self.connection.commit()

        cur.execute("SELECT name FROM batch_tbl WHERE id = 5")
        self.assertEqual(cur.fetchone()[0], "Updated_5")

        # Batch DELETE
        delete_data = [(i,) for i in range(10)]
        cur.executemany("DELETE FROM batch_tbl WHERE id = ?", delete_data)
        self.assertEqual(cur.rowcount, 10)
        self.connection.commit()

        cur.execute("SELECT COUNT(*) FROM batch_tbl")
        self.assertEqual(cur.fetchone()[0], 141)

        # Batch IntegrityError (duplicate primary key)
        with self.assertRaises(firebirdsql.IntegrityError):
            cur.executemany(
                "INSERT INTO batch_tbl (id, name) VALUES (?, ?)",
                [(100, "dup")]
            )
        self.connection.rollback()

        # Verify _can_use_batch returns False for protocol < 16
        old_ver = self.connection.accept_version
        try:
            self.connection.accept_version = 15
            prep_stmt = cur.prep("INSERT INTO batch_tbl (id, name) VALUES (?, ?)")
            self.assertFalse(cur._can_use_batch(prep_stmt.stmt))
        finally:
            self.connection.accept_version = old_ver

        # Fallback to loop when batch cannot be used
        orig_can_use_batch = cur._can_use_batch
        try:
            cur._can_use_batch = lambda *args, **kwargs: False
            fallback_data = [(200 + i, f"Fallback_{i}") for i in range(5)]
            cur.executemany("INSERT INTO batch_tbl (id, name) VALUES (?, ?)", fallback_data)
            self.assertEqual(cur.rowcount, 5)
            self.connection.commit()
            cur.execute("SELECT COUNT(*) FROM batch_tbl WHERE id >= 200")
            self.assertEqual(cur.fetchone()[0], 5)
        finally:
            cur._can_use_batch = orig_can_use_batch

        # Empty seq_of_params
        cur.executemany("INSERT INTO batch_tbl (id, name) VALUES (?, ?)", [])
        self.assertEqual(cur.rowcount, 0)
        cur.close()

    def test_batch_edge_cases(self):
        if self.server_version[0] < 4:
            self.connection.close()
            return

        cur = self.connection.cursor()
        cur.execute("""
            CREATE TABLE batch_edge (
                id INT PRIMARY KEY,
                s VARCHAR(5),
                amount DECIMAL(10, 2),
                dt DATE,
                flag BOOLEAN,
                ts TIMESTAMP,
                si SMALLINT,
                after_si INTEGER
            )
        """)
        self.connection.commit()

        # 1. Fresh cursor executemany followed by fetchone should return None (not raise AttributeError)
        cur_fresh = self.connection.cursor()
        cur_fresh.executemany("INSERT INTO batch_edge (id, s) VALUES (?, ?)", [(1, "a")])
        self.assertIsNone(cur_fresh.fetchone())
        self.assertEqual(cur_fresh.fetchall(), [])

        # 2. Previous SELECT cursor: executemany clears stale results
        cur_stale = self.connection.cursor()
        cur_stale.execute("SELECT id FROM batch_edge")
        cur_stale.executemany("INSERT INTO batch_edge (id, s) VALUES (?, ?)", [(2, "b")])
        self.assertIsNone(cur_stale.fetchone())
        self.assertEqual(cur_stale.fetchall(), [])

        # 3. DATE string / BOOLEAN string fallback to ordinary execution and succeed
        cur.executemany(
            "INSERT INTO batch_edge (id, dt, flag) VALUES (?, ?, ?)",
            [(3, "2025-01-01", "false"), (4, "2025-01-02", "true")]
        )
        self.connection.commit()
        cur.execute("SELECT id, dt, flag FROM batch_edge WHERE id IN (3, 4) ORDER BY id")
        self.assertEqual(cur.fetchall(), [
            (3, datetime.date(2025, 1, 1), False),
            (4, datetime.date(2025, 1, 2), True),
        ])

        cur.executemany(
            "INSERT INTO batch_edge (id, amount) VALUES (?, ?)",
            [(5, Decimal("1.239"))]
        )
        self.connection.commit()
        cur.execute("SELECT amount FROM batch_edge WHERE id = 5")
        self.assertEqual(cur.fetchone()[0], Decimal("1.24"))

        integer_stmt = cur.prep("INSERT INTO batch_edge (id) VALUES (?)")
        self.assertFalse(
            cur._is_safe_batch_row(integer_stmt.stmt.bind_xsqlda, [Decimal("1.5")])
        )
        integer_stmt.close()

        cur.executemany(
            "INSERT INTO batch_edge (id, si, after_si) VALUES (?, ?, ?)",
            [(6, -123, 456), (7, 123, 789)]
        )
        self.connection.commit()
        cur.execute("SELECT id, si, after_si FROM batch_edge WHERE id IN (6, 7) ORDER BY id")
        self.assertEqual(cur.fetchall(), [(6, -123, 456), (7, 123, 789)])

        def generated_rows():
            yield (8, "8")
            verify = self.connection.cursor()
            verify.execute("SELECT COUNT(*) FROM batch_edge WHERE id = 8")
            self.assertEqual(verify.fetchone()[0], 1)
            verify.close()
            yield (9, "9")

        cur.executemany("INSERT INTO batch_edge (id, s) VALUES (?, ?)", generated_rows())
        self.connection.commit()
        cur.execute("SELECT id, s FROM batch_edge WHERE id IN (8, 9) ORDER BY id")
        self.assertEqual(cur.fetchall(), [(8, "8"), (9, "9")])

        # 4. Overlong VARCHAR does not silently truncate (raises error)
        with self.assertRaises(firebirdsql.DatabaseError):
            cur.executemany("INSERT INTO batch_edge (id, s) VALUES (?, ?)", [(10, "abcdef")])
        self.connection.rollback()

        # 5. Out of range SMALLINT does not silently wrap (raises error)
        with self.assertRaises(firebirdsql.DatabaseError):
            cur.executemany("INSERT INTO batch_edge (id, si) VALUES (?, ?)", [(20, 70000)])
        self.connection.rollback()

        # 6. Timezone-aware timestamp on plain TIMESTAMP field falls back or safely handles without corrupting protocol
        aware_dt = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        cur.executemany("INSERT INTO batch_edge (id, ts) VALUES (?, ?)", [(30, aware_dt)])
        self.connection.commit()
        cur.execute("SELECT id FROM batch_edge WHERE id = 30")
        self.assertEqual(cur.fetchone()[0], 30)

        # 7. statement_timeout cursor disables batching
        cur_timeout = self.connection.cursor(timeout=5)
        prep = cur_timeout.prep("INSERT INTO batch_edge (id, s) VALUES (?, ?)")
        self.assertFalse(cur_timeout._can_use_batch(prep.stmt))

        # 8. Bounded batch execution: splits batches and accumulates affected count
        cur.batch_flush_threshold = 50
        batch_exec_calls = 0
        orig_batch_exec = self.connection._op_batch_exec

        def counting_batch_exec(*args, **kwargs):
            nonlocal batch_exec_calls
            batch_exec_calls += 1
            return orig_batch_exec(*args, **kwargs)

        self.connection._op_batch_exec = counting_batch_exec
        try:
            chunked_rows = [(2000 + i, f"c_{i}") for i in range(20)]
            cur.executemany("INSERT INTO batch_edge (id, s) VALUES (?, ?)", chunked_rows)
            self.assertEqual(cur.rowcount, 20)
            self.assertGreater(batch_exec_calls, 1)
            self.connection.commit()
            cur.execute("SELECT COUNT(*) FROM batch_edge WHERE id >= 2000")
            self.assertEqual(cur.fetchone()[0], 20)
        finally:
            self.connection._op_batch_exec = orig_batch_exec
            del cur.batch_flush_threshold

        cur.close()

    def test_fetch_memoryview(self):
        cur = self.connection.cursor()
        cur.execute("""
            CREATE TABLE fetch_mv_test (
                id INT PRIMARY KEY,
                s VARCHAR(50),
                val DOUBLE PRECISION,
                d DATE,
                t TIME,
                ts TIMESTAMP,
                n DECIMAL(12, 2)
            )
        """)
        self.connection.commit()

        # Insert 600 rows (exceeds DEFAULT_FETCH_COUNT=400 to test multi-batch fetch)
        test_rows = [
            (
                i,
                f"str_{i:04d}",
                float(i) * 1.25,
                datetime.date(2025, 1, 1 + (i % 28)),
                datetime.time(i % 24, i % 60, i % 60),
                datetime.datetime(2025, 1, 1, i % 24, i % 60, i % 60),
                Decimal(f"{i}.75"),
            )
            for i in range(600)
        ]
        # Include some rows with NULL values
        test_rows[10] = (10, None, None, None, None, None, None)
        test_rows[500] = (500, None, 500.0, None, None, None, None)

        cur.executemany(
            "INSERT INTO fetch_mv_test VALUES (?, ?, ?, ?, ?, ?, ?)",
            test_rows
        )
        self.connection.commit()

        # Test fetchall
        cur.execute("SELECT id, s, val, d, t, ts, n FROM fetch_mv_test ORDER BY id")
        rows = cur.fetchall()
        self.assertEqual(len(rows), 600)

        # Check first, middle, last, and NULL rows
        self.assertEqual(rows[0][0], 0)
        self.assertEqual(rows[0][1], "str_0000")
        self.assertAlmostEqual(rows[0][2], 0.0)
        self.assertEqual(rows[0][3], datetime.date(2025, 1, 1))
        self.assertEqual(rows[0][6], Decimal("0.75"))

        self.assertEqual(rows[10], (10, None, None, None, None, None, None))
        self.assertEqual(rows[500][1], None)
        self.assertAlmostEqual(rows[500][2], 500.0)

        self.assertEqual(rows[599][0], 599)
        self.assertEqual(rows[599][1], "str_0599")
        self.assertAlmostEqual(rows[599][2], 599 * 1.25)
        self.assertEqual(rows[599][6], Decimal("599.75"))

        # Test fetchone and fetchmany
        cur.execute("SELECT id, s FROM fetch_mv_test ORDER BY id")
        self.assertEqual(cur.fetchone(), (0, "str_0000"))
        batch = cur.fetchmany(5)
        self.assertEqual(len(batch), 5)
        self.assertEqual(batch[0], (1, "str_0001"))
        self.assertEqual(batch[4], (5, "str_0005"))

        # Test cursor iteration
        cur.execute("SELECT id FROM fetch_mv_test WHERE id < 10 ORDER BY id")
        ids = [row[0] for row in cur]
        self.assertEqual(ids, list(range(10)))

        cur.close()

    def test_fetch_conversion_exception_not_masked(self):
        conn = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            timeout=1,
        )
        cur = conn.cursor()
        prep = cur.prep("SELECT 1 FROM rdb$database")
        orig_val = prep.stmt.xsqlda[0].value
        try:
            prep.stmt.xsqlda[0].value = lambda raw: int("not_a_number")
            with self.assertRaises(ValueError):
                cur.execute(prep)
        finally:
            prep.stmt.xsqlda[0].value = orig_val
            conn.sock.close()
