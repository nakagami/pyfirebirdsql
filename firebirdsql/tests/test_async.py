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
        loop.close()


if __name__ == "__main__":
    unittest.main()
