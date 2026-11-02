import datetime
from decimal import Decimal
import unittest
import firebirdsql
from firebirdsql.tests.base import TestBase
from firebirdsql.array import (
    Array, ArrayDimension, ArrayMeta,
    generate_sdl, shrink_dimensions,
    present_index_ranges, format_array_output,
    is_ragged,
)
from firebirdsql.consts import (
    SQL_TYPE_LONG, SQL_TYPE_SHORT, SQL_TYPE_INT64,
    SQL_TYPE_FLOAT, SQL_TYPE_DOUBLE, SQL_TYPE_TEXT,
    SQL_TYPE_VARYING, SQL_TYPE_BOOLEAN, SQL_TYPE_DATE,
    SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP,
    blr_type_long, blr_type_short, blr_type_int64,
    blr_type_float, blr_type_double, blr_type_text,
    blr_type_varying, blr_type_boolean, blr_type_date,
    blr_type_time, blr_type_timestamp,
)


class TestArrayUnit(unittest.TestCase):
    def test_ragged(self):
        self.assertFalse(is_ragged([1, 2, 3]))
        self.assertFalse(is_ragged([[1, 2], [3, 4]]))
        self.assertFalse(is_ragged([[[1, 2]], [[3, 4]]]))
        self.assertTrue(is_ragged([[1, 2], [3]]))
        self.assertTrue(is_ragged([[1, 2], 3]))
        self.assertTrue(is_ragged([1, [2, 3]]))

    def test_shrink_dimensions(self):
        dims = [ArrayDimension(1, 3), ArrayDimension(1, 4)]
        shrunk = shrink_dimensions(dims, 12)
        self.assertEqual(shrunk[0].length, 3)
        self.assertEqual(shrunk[1].length, 4)

        shrunk = shrink_dimensions(dims, 8)
        self.assertEqual(shrunk[0].length, 2)
        self.assertEqual(shrunk[1].length, 4)

        with self.assertRaises(firebirdsql.OperationalError):
            shrink_dimensions(dims, 13)

    def test_present_index_ranges(self):
        declared = [ArrayDimension(1, 3)]
        actual = [ArrayDimension(1, 2)]
        ranges = present_index_ranges(declared, actual)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start, 0)
        self.assertEqual(ranges[0].end, 1)

    def test_format_array_output(self):
        meta = ArrayMeta(
            table_name='T',
            field_name='F',
            type_id=SQL_TYPE_LONG,
            blr_type_id=blr_type_long,
            scale=0,
            length=4,
            field_bytes=4,
            sub_type=0,
            field_source='RDB$1',
            dimensions=[ArrayDimension(1, 2), ArrayDimension(1, 3)],
        )
        flat = [1, 2, 3, 4, 5, 6]
        res = format_array_output(meta, flat)
        self.assertEqual(res, [[1, 2, 3], [4, 5, 6]])

    def test_generate_sdl_int_1d(self):
        meta = ArrayMeta(
            table_name="T",
            field_name="INTS",
            type_id=SQL_TYPE_LONG,
            blr_type_id=blr_type_long,
            scale=0,
            length=4,
            field_bytes=4,
            sub_type=0,
            dimensions=[ArrayDimension(1, 5)],
        )
        sdl = generate_sdl(meta, meta.dimensions, for_get=False)
        want = bytes([
            1, 6, 1, 8, 0,
            2, 1, ord('T'),
            4, 4, ord('I'), ord('N'), ord('T'), ord('S'),
            35, 0, 9, 5,
            36, 1, 8, 0, 1, 7, 0,
            255,
        ])
        self.assertEqual(sdl, want)

    def test_generate_sdl_lower_bound_non_one(self):
        meta = ArrayMeta(
            table_name="T",
            field_name="G",
            type_id=SQL_TYPE_INT64,
            blr_type_id=blr_type_int64,
            scale=0,
            length=8,
            field_bytes=8,
            sub_type=0,
            dimensions=[ArrayDimension(0, 3)],
        )
        sdl = generate_sdl(meta, meta.dimensions, for_get=False)
        want = bytes([
            1, 6, 1, 16, 0,
            2, 1, ord('T'),
            4, 1, ord('G'),
            34, 0, 9, 0, 9, 2,
            36, 1, 8, 0, 1, 7, 0,
            255,
        ])
        self.assertEqual(sdl, want)

    def test_generate_sdl_varying_length_word(self):
        meta = ArrayMeta(
            table_name="T",
            field_name="S",
            type_id=SQL_TYPE_VARYING,
            blr_type_id=blr_type_varying,
            scale=0,
            length=10,
            field_bytes=40,
            sub_type=0,
            dimensions=[ArrayDimension(1, 4)],
        )
        put_sdl = generate_sdl(meta, meta.dimensions, for_get=False)
        get_sdl = generate_sdl(meta, meta.dimensions, for_get=True)
        # Both put and get SDL carry field bytes (40)
        self.assertEqual(put_sdl[4:6], bytes([40, 0]))
        self.assertEqual(get_sdl[4:6], bytes([40, 0]))


class TestArrayIntegration(TestBase):
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
            CREATE TABLE test_arr_basic (
                id INTEGER NOT NULL PRIMARY KEY,
                arr_int INTEGER [1:5],
                arr_small SMALLINT [1:3],
                arr_big BIGINT [1:4],
                arr_vchar VARCHAR(20) [1:3],
                arr_char CHAR(10) [1:3],
                arr_flt FLOAT [1:3],
                arr_dbl DOUBLE PRECISION [1:3],
                arr_num NUMERIC(9, 2) [1:3],
                arr_dt DATE [1:3],
                arr_tm TIME [1:3],
                arr_ts TIMESTAMP [1:3]
            )
        ''')

        if self.server_version[0] >= 3:
            cur.execute('''
                CREATE TABLE test_arr_bool (
                    id INTEGER NOT NULL PRIMARY KEY,
                    arr_bool BOOLEAN [1:4]
                )
            ''')

        if self.server_version[0] >= 4:
            cur.execute('''
                CREATE TABLE test_arr_fb4 (
                    id INTEGER NOT NULL PRIMARY KEY,
                    arr_i128 INT128 [1:3]
                )
            ''')

        cur.execute('''
            CREATE TABLE "MyTable" (
                id INTEGER NOT NULL PRIMARY KEY,
                "myArr" INTEGER [1:3]
            )
        ''')

        cur.execute('''
            CREATE TABLE test_arr_multi (
                id INTEGER NOT NULL PRIMARY KEY,
                arr_2d INTEGER [1:2, 1:3],
                arr_3d INTEGER [1:2, 1:2, 1:2]
            )
        ''')

        cur.execute('''
            CREATE TABLE test_arr_bounds (
                id INTEGER NOT NULL PRIMARY KEY,
                arr_zero INTEGER [0:3],
                arr_neg INTEGER [-2:2]
            )
        ''')

        self.connection.commit()

    def test_array_basic_types(self):
        cur = self.connection.cursor()
        d1 = datetime.date(2020, 1, 1)
        d2 = datetime.date(2021, 6, 15)
        d3 = datetime.date(2022, 12, 31)

        t1 = datetime.time(8, 30, 0)
        t2 = datetime.time(12, 0, 0)
        t3 = datetime.time(23, 59, 59)

        ts1 = datetime.datetime(2020, 1, 1, 8, 30, 0)
        ts2 = datetime.datetime(2021, 6, 15, 12, 0, 0)
        ts3 = datetime.datetime(2022, 12, 31, 23, 59, 59)

        cur.execute('''
            INSERT INTO test_arr_basic (
                id, arr_int, arr_small, arr_big, arr_vchar, arr_char,
                arr_flt, arr_dbl, arr_num, arr_dt, arr_tm, arr_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            1,
            [10, 20, 30, 40, 50],
            [1, 2, 3],
            [10000000000, 20000000000, 30000000000, 40000000000],
            ['apple', 'banana', 'cherry'],
            ['red', 'green', 'blue'],
            [1.5, 2.5, 3.5],
            [10.125, 20.25, 30.5],
            [Decimal('12.34'), Decimal('56.78'), Decimal('90.12')],
            [d1, d2, d3],
            [t1, t2, t3],
            [ts1, ts2, ts3],
        ))
        self.connection.commit()

        cur.execute('SELECT * FROM test_arr_basic WHERE id = 1')
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], [10, 20, 30, 40, 50])
        self.assertEqual(row[2], [1, 2, 3])
        self.assertEqual(row[3], [10000000000, 20000000000, 30000000000, 40000000000])
        self.assertEqual(row[4], ['apple', 'banana', 'cherry'])
        self.assertEqual(row[5], ['red', 'green', 'blue'])
        self.assertAlmostEqual(row[6][0], 1.5, places=5)
        self.assertAlmostEqual(row[6][1], 2.5, places=5)
        self.assertAlmostEqual(row[6][2], 3.5, places=5)
        self.assertEqual(row[7], [10.125, 20.25, 30.5])
        self.assertEqual(row[8], [Decimal('12.34'), Decimal('56.78'), Decimal('90.12')])
        self.assertEqual(row[9], [d1, d2, d3])
        self.assertEqual(row[10], [t1, t2, t3])
        self.assertEqual(row[11], [ts1, ts2, ts3])

    def test_array_null_handling(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (2, None))
        self.connection.commit()

        cur.execute('SELECT id, arr_int, arr_vchar FROM test_arr_basic WHERE id = 2')
        row = cur.fetchone()
        self.assertEqual(row[0], 2)
        self.assertIsNone(row[1])
        self.assertIsNone(row[2])

    def test_array_boolean(self):
        if self.server_version[0] < 3:
            return
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_bool (id, arr_bool) VALUES (?, ?)', (1, [True, False, True, False]))
        self.connection.commit()

        cur.execute('SELECT id, arr_bool FROM test_arr_bool WHERE id = 1')
        row = cur.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], [True, False, True, False])

    def test_array_multi_dimensional(self):
        cur = self.connection.cursor()
        arr_2d = [[1, 2, 3], [4, 5, 6]]
        arr_3d = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
        cur.execute('INSERT INTO test_arr_multi (id, arr_2d, arr_3d) VALUES (?, ?, ?)', (1, arr_2d, arr_3d))
        self.connection.commit()

        cur.execute('SELECT id, arr_2d, arr_3d FROM test_arr_multi WHERE id = 1')
        row = cur.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], arr_2d)
        self.assertEqual(row[2], arr_3d)

    def test_array_update(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (20, [1, 2, 3, 4, 5]))
        self.connection.commit()

        cur.execute('UPDATE test_arr_basic SET arr_int = ? WHERE id = ?', ([10, 20, 30, 40, 50], 20))
        self.connection.commit()

        cur.execute('SELECT arr_int FROM test_arr_basic WHERE id = 20')
        row = cur.fetchone()
        self.assertEqual(row[0], [10, 20, 30, 40, 50])

    def test_array_returning(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?) RETURNING id, arr_int', (30, [100, 200, 300, 400, 500]))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 30)
        self.assertEqual(row[1], [100, 200, 300, 400, 500])
        self.connection.commit()

        cur.execute('UPDATE test_arr_basic SET arr_int = ? WHERE id = ? RETURNING arr_int', ([1, 2, 3, 4, 5], 30))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], [1, 2, 3, 4, 5])
        self.connection.commit()

    def test_array_executemany(self):
        cur = self.connection.cursor()
        data = [
            (40, [1, 2, 3, 4, 5]),
            (41, [6, 7, 8, 9, 10]),
        ]
        cur.executemany('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', data)
        self.connection.commit()

        cur.execute('SELECT id, arr_int FROM test_arr_basic WHERE id IN (40, 41) ORDER BY id')
        rows = cur.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1], [1, 2, 3, 4, 5])
        self.assertEqual(rows[1][1], [6, 7, 8, 9, 10])

    def test_array_utf8(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_basic (id, arr_vchar, arr_char) VALUES (?, ?, ?)', (
            50,
            ['こんにちは', '世界', 'テスト'],
            ['東京', '大阪', '京都'],
        ))
        self.connection.commit()

        cur.execute('SELECT arr_vchar, arr_char FROM test_arr_basic WHERE id = 50')
        row = cur.fetchone()
        self.assertEqual(row[0], ['こんにちは', '世界', 'テスト'])
        self.assertEqual(row[1], ['東京', '大阪', '京都'])

    def test_array_quoted_identifier(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO "MyTable" (id, "myArr") VALUES (?, ?)', (1, [100, 200, 300]))
        self.connection.commit()

        cur.execute('SELECT "myArr" FROM "MyTable" WHERE id = 1')
        row = cur.fetchone()
        self.assertEqual(row[0], [100, 200, 300])

    def test_array_int128(self):
        if self.server_version[0] < 4:
            return
        cur = self.connection.cursor()
        big1 = 123456789012345678901234567890
        big2 = 987654321098765432109876543210
        big3 = -555555555555555555555555555555
        cur.execute('INSERT INTO test_arr_fb4 (id, arr_i128) VALUES (?, ?)', (1, [big1, big2, big3]))
        self.connection.commit()

        cur.execute('SELECT arr_i128 FROM test_arr_fb4 WHERE id = 1')
        row = cur.fetchone()
        self.assertEqual(row[0], [big1, big2, big3])

    def test_array_partial_elements(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (60, [1, 2, 3]))
        self.connection.commit()

        cur.execute('SELECT arr_int FROM test_arr_basic WHERE id = 60')
        row = cur.fetchone()
        self.assertEqual(row[0], [1, 2, 3, 0, 0])

    def test_array_custom_bounds(self):
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_bounds (id, arr_zero, arr_neg) VALUES (?, ?, ?)', (
            1,
            [100, 101, 102, 103],
            [-20, -10, 0, 10, 20],
        ))
        self.connection.commit()

        cur.execute('SELECT id, arr_zero, arr_neg FROM test_arr_bounds WHERE id = 1')
        row = cur.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], [100, 101, 102, 103])
        self.assertEqual(row[2], [-20, -10, 0, 10, 20])

    def test_array_wrapper_object(self):
        cur = self.connection.cursor()
        arr = Array([100, 200, 300, 400, 500])
        cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (10, arr))
        self.connection.commit()

        cur.execute('SELECT id, arr_int FROM test_arr_basic WHERE id = 10')
        row = cur.fetchone()
        self.assertEqual(row[0], 10)
        self.assertEqual(row[1], [100, 200, 300, 400, 500])

    def test_array_error_cases(self):
        cur = self.connection.cursor()
        # Ragged multi-dimensional array
        with self.assertRaises(firebirdsql.OperationalError):
            cur.execute('INSERT INTO test_arr_multi (id, arr_2d) VALUES (?, ?)', (99, [[1, 2], [3]]))

        # NULL element inside array
        with self.assertRaises(firebirdsql.OperationalError):
            cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (99, [1, None, 3, 4, 5]))

    def test_array_prepared_insert_reexecute(self):
        # array parameters are uploaded before the execute, the row count
        # request goes out with it
        cur = self.connection.cursor()
        ps = cur.prep('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)')
        for i in range(5):
            cur.execute(ps, (200 + i, [i, i + 1, i + 2, i + 3, i + 4]))
            self.assertEqual(cur.rowcount, 1)
        ps_upd = cur.prep('UPDATE test_arr_basic SET arr_int = ? WHERE id >= ?')
        cur.execute(ps_upd, ([9, 9, 9, 9, 9], 203))
        self.assertEqual(cur.rowcount, 2)
        cur.execute(ps_upd, ([8, 8, 8, 8, 8], 204))
        self.assertEqual(cur.rowcount, 1)
        self.connection.commit()

        cur.execute('SELECT id, arr_int FROM test_arr_basic WHERE id >= 200 ORDER BY id')
        self.assertEqual(cur.fetchall(), [
            (200, [0, 1, 2, 3, 4]),
            (201, [1, 2, 3, 4, 5]),
            (202, [2, 3, 4, 5, 6]),
            (203, [9, 9, 9, 9, 9]),
            (204, [8, 8, 8, 8, 8]),
        ])

    def test_array_prepared_select_reexecute(self):
        # array columns are read after the cursor of the previous execution
        # was closed in the same round trip as the execute
        cur = self.connection.cursor()
        for i in range(3):
            cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (210 + i, [i] * 5))
        self.connection.commit()
        ps = cur.prep('SELECT id, arr_int FROM test_arr_basic WHERE id >= ? ORDER BY id')
        cur.execute(ps, (210,))
        self.assertEqual(cur.fetchone(), (210, [0] * 5))
        cur.execute(ps, (211,))     # only one row read before
        self.assertEqual(cur.fetchall(), [(211, [1] * 5), (212, [2] * 5)])
        cur.execute(ps, (210,))     # all rows read before
        self.assertEqual(cur.fetchall(), [(210, [0] * 5), (211, [1] * 5), (212, [2] * 5)])
        ps.close()

    def test_array_param_error_on_reexecuted_select(self):
        # an array parameter that cannot be sent raises before any request
        # goes out, even when the cursor of the previous execution has to
        # be closed first: the connection stays in step
        cur = self.connection.cursor()
        cur.execute('INSERT INTO test_arr_basic (id, arr_int) VALUES (?, ?)', (220, [1, 2, 3, 4, 5]))
        self.connection.commit()
        ps = cur.prep('SELECT id, arr_int FROM test_arr_basic WHERE id = ?')
        cur.execute(ps, (220,))
        self.assertEqual(cur.fetchall(), [(220, [1, 2, 3, 4, 5])])
        with self.assertRaises(firebirdsql.OperationalError):
            cur.execute(ps, ([1, 2, 3],))   # an array for an INTEGER parameter
        cur.execute(ps, (220,))
        self.assertEqual(cur.fetchall(), [(220, [1, 2, 3, 4, 5])])

        # the same for a prepared INSERT
        ps_ins = cur.prep('INSERT INTO test_arr_multi (id, arr_2d) VALUES (?, ?)')
        with self.assertRaises(firebirdsql.OperationalError):
            cur.execute(ps_ins, (221, [[1, 2], [3]]))   # ragged
        cur.execute(ps_ins, (221, [[1, 2, 3], [4, 5, 6]]))
        self.assertEqual(cur.rowcount, 1)
        self.connection.commit()
        cur.execute('SELECT arr_2d FROM test_arr_multi WHERE id = 221')
        self.assertEqual(cur.fetchone(), ([[1, 2, 3], [4, 5, 6]],))
