import tempfile
import asyncio
import unittest
import firebirdsql
from firebirdsql.tests import base


class AsyncTestCase(base.TestBase):

    def setUp(self):
        self.database = tempfile.mktemp()
        conn = firebirdsql.create_database(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            page_size=self.page_size)
        conn.close()

    def tearDown(self):
        pass

    def test_aio_large_blob(self):
        data = bytes(i * 7 % 256 for i in range(300000))
        conn = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        cur = conn.cursor()
        cur.execute("CREATE TABLE large_blob (b BLOB SUB_TYPE BINARY)")
        conn.commit()
        conn.close()

        async def _test():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            cur = conn.cursor()
            await cur.execute("INSERT INTO large_blob VALUES (?)", (data,))
            await conn.commit()
            await cur.execute("SELECT b FROM large_blob")
            self.assertEqual((await cur.fetchone())[0], data)
            await conn.close()
        asyncio.run(_test())

    def test_aio_rowcount_and_errors(self):
        async def _test():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            cur = conn.cursor()
            await cur.execute("CREATE TABLE aio_rc (id INTEGER NOT NULL PRIMARY KEY)")
            await conn.commit()
            for i in range(3):
                await cur.execute("INSERT INTO aio_rc VALUES (?)", (i,))
                self.assertEqual(cur.rowcount, 1)
            await cur.execute("UPDATE aio_rc SET id = id + 10 WHERE id > 0")
            self.assertEqual(cur.rowcount, 2)
            with self.assertRaises(firebirdsql.IntegrityError):
                await cur.execute("INSERT INTO aio_rc VALUES (0)")
            with self.assertRaises(firebirdsql.DatabaseError):
                await cur.execute("SELECT 1 / 0 FROM aio_rc")
                await cur.fetchall()
            await cur.execute("SELECT id FROM aio_rc ORDER BY id")
            self.assertEqual(await cur.fetchall(), [(0,), (11,), (12,)])
            await conn.close()
        asyncio.run(_test())

    def test_aio_reexecute_prepared_select(self):
        async def _test():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            cur = conn.cursor()
            await cur.execute("CREATE TABLE aio_re (id INTEGER NOT NULL PRIMARY KEY)")
            await conn.commit()
            for i in range(1, 4):
                await cur.execute("INSERT INTO aio_re VALUES (?)", (i,))
            await conn.commit()
            ps = await cur.prep("SELECT id FROM aio_re WHERE id <= ? ORDER BY id")
            await cur.execute(ps, (1,))
            self.assertEqual(await cur.fetchall(), [(1,)])
            await cur.execute(ps, (2,))         # all rows read before
            self.assertEqual(await cur.fetchall(), [(1,), (2,)])
            await cur.execute(ps, (3,))
            self.assertEqual(await cur.fetchone(), (1,))
            await cur.execute(ps, (3,))         # only one row read before
            self.assertEqual(await cur.fetchall(), [(1,), (2,), (3,)])
            await conn.commit()                 # closes the cursor on the server
            await cur.execute(ps, (2,))
            self.assertEqual(await cur.fetchall(), [(1,), (2,)])
            await conn.rollback()
            await cur.execute(ps, (1,))
            self.assertEqual(await cur.fetchall(), [(1,)])
            await ps.close()
            await cur.execute(ps, (2,))         # executed again after close()
            self.assertEqual(await cur.fetchall(), [(1,), (2,)])

            # the first fetch fails but the cursor is open on the server
            ps2 = await cur.prep("SELECT 1 / (id - CAST(? AS INTEGER)) FROM aio_re ORDER BY id")
            with self.assertRaises(firebirdsql.DatabaseError):
                await cur.execute(ps2, (1,))
            await cur.execute(ps2, (0,))
            self.assertEqual(await cur.fetchall(), [(1,), (0,), (0,)])

            # a long text parameter is sent as a blob before the close
            ps3 = await cur.prep(
                "SELECT id FROM aio_re WHERE CHAR_LENGTH(CAST(? AS BLOB SUB_TYPE TEXT)) > id ORDER BY id")
            long_text = 'x' * 40000
            for _ in range(3):
                await cur.execute(ps3, (long_text,))
                self.assertEqual(await cur.fetchone(), (1,))
            await cur.execute("SELECT COUNT(*) FROM aio_re")
            self.assertEqual(await cur.fetchone(), (3,))
            await conn.close()
        asyncio.run(_test())

    def test_aio_connect(self):
        async def _test_select():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
            )
            cur = conn.cursor()

            await cur.execute("SELECT 42 FROM rdb$database")
            self.assertEqual(cur.rowcount, 1)
            result = await cur.fetchall()
            self.assertEqual(result, [(42,)])

            await cur.execute("SELECT 42 FROM rdb$database")
            rows = [row async for row in cur]
            self.assertEqual(rows, [(42,)])
        asyncio.run(_test_select())

    def test_aio_wire_compress(self):
        conn = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        cur = conn.cursor()
        cur.execute("SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database")
        major = int(cur.fetchone()[0].split('.')[0])
        conn.close()
        if major < 3:
            self.skipTest("wire compression needs Firebird 3+")

        async def _test_select():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                wire_compress=True,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            cur = conn.cursor()
            await cur.execute("SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_COMPRESSED') FROM RDB$DATABASE")
            self.assertEqual(await cur.fetchall(), [('TRUE',)])
            await cur.execute("SELECT RDB$FIELD_NAME FROM RDB$FIELDS")
            self.assertGreater(len(await cur.fetchall()), 100)
            await conn.close()
        asyncio.run(_test_select())

    def test_aio_connect_with_loop(self):
        loop = asyncio.new_event_loop()

        async def _test_select():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
                loop=loop,
            )
            cur = conn.cursor()
            await cur.execute("SELECT 42 FROM rdb$database")
            result = await cur.fetchall()
            self.assertEqual(result, [(42, ), ])
            await cur.close()
            await conn.close()
        loop.run_until_complete(_test_select())
        loop.close()

    def test_create_pool(self):
        async def _test_select(loop):
            pool = await firebirdsql.aio.create_pool(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
                loop=loop,
            )
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 42 FROM rdb$database")
                    self.assertEqual(
                        cur.description,
                        [('CONSTANT', 496, 11, 4, 11, 0, False)],
                    )
                    (r,) = await cur.fetchone()
                    self.assertEqual(r, 42)
            pool.close()
            await pool.wait_closed()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_test_select(loop))
        loop.close()

    def test_insert_returning(self):
        async def _test_insert_returning(loop):
            pool = await firebirdsql.aio.create_pool(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
                loop=loop,
            )
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("CREATE TABLE foo (a INTEGER)")
                    await conn.commit()
                    await cur.execute("INSERT INTO foo (a) VALUES(?) returning a", [1])
                    res = await cur.fetchall()
                    self.assertEqual(res, [(1,)])
                    await conn.commit()
            pool.close()
            await pool.wait_closed()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_test_insert_returning(loop))
        loop.close()

    def test_many_columns_query(self):
        """Test for PR #132: Fix TimeoutError with queries having many columns.
        
        When a query's SQLDA description spans multiple packets, parse_xsqlda
        needs to paginate by calling _op_response. In async mode, this must use
        await _async_op_response() to avoid blocking the event loop.
        """
        async def _test_many_columns(loop):
            pool = await firebirdsql.aio.create_pool(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
                loop=loop,
            )
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Create a table with many columns to trigger SQLDA pagination
                    columns = ', '.join([f'col{i} INTEGER' for i in range(100)])
                    await cur.execute(f"CREATE TABLE many_cols ({columns})")
                    await conn.commit()
                    
                    # Insert test data
                    values = ', '.join(['?' for _ in range(100)])
                    await cur.execute(f"INSERT INTO many_cols VALUES ({values})", list(range(100)))
                    await conn.commit()
                    
                    # This SELECT should trigger async_parse_xsqlda when preparing
                    await cur.execute("SELECT * FROM many_cols")
                    result = await cur.fetchone()
                    
                    # Verify we got all 100 columns back
                    self.assertEqual(len(result), 100)
                    self.assertEqual(result, tuple(range(100)))
                    await conn.commit()
            pool.close()
            await pool.wait_closed()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_test_many_columns(loop))

    def test_aio_fetch_memoryview(self):
        async def _test():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size,
            )
            cur = conn.cursor()
            await cur.execute("CREATE TABLE t_aio_mv (id INT, name VARCHAR(50), val DOUBLE PRECISION)")
            await conn.commit()

            # Insert 700 rows to ensure multi-batch fetching
            for i in range(700):
                val = float(i) * 1.5 if i % 2 == 0 else None
                name = f"row_{i}" if i % 3 != 0 else None
                await cur.execute("INSERT INTO t_aio_mv (id, name, val) VALUES (?, ?, ?)", (i, name, val))
            await conn.commit()

            await cur.execute("SELECT id, name, val FROM t_aio_mv ORDER BY id")
            rows = await cur.fetchall()
            self.assertEqual(len(rows), 700)
            self.assertEqual(rows[0], (0, None, 0.0))
            self.assertEqual(rows[1], (1, "row_1", None))
            self.assertEqual(rows[2], (2, "row_2", 3.0))

            await cur.close()
            await conn.close()

        asyncio.run(_test())


if __name__ == "__main__":
    unittest.main()
