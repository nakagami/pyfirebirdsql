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
        cur.execute("INSERT INTO large_blob VALUES (?)", (data,))
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
            await cur.execute("SELECT b FROM large_blob")
            self.assertEqual((await cur.fetchone())[0], data)
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
