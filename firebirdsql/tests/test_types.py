import datetime
import decimal
import unittest
import firebirdsql
from firebirdsql.consts import *


class TestDBAPITypes(unittest.TestCase):
    def test_dbapi_type_objects_python_types(self):
        # Bidirectional equality for Python types
        self.assertEqual(firebirdsql.STRING, str)
        self.assertEqual(str, firebirdsql.STRING)
        self.assertNotEqual(firebirdsql.STRING, int)
        self.assertNotEqual(int, firebirdsql.STRING)

        self.assertEqual(firebirdsql.NUMBER, int)
        self.assertEqual(int, firebirdsql.NUMBER)
        self.assertEqual(firebirdsql.NUMBER, float)
        self.assertEqual(float, firebirdsql.NUMBER)
        self.assertEqual(firebirdsql.NUMBER, decimal.Decimal)
        self.assertEqual(decimal.Decimal, firebirdsql.NUMBER)
        self.assertNotEqual(firebirdsql.NUMBER, str)

        self.assertEqual(firebirdsql.DATETIME, datetime.datetime)
        self.assertEqual(datetime.datetime, firebirdsql.DATETIME)
        self.assertEqual(firebirdsql.DATETIME, datetime.date)
        self.assertEqual(firebirdsql.DATETIME, datetime.time)

        self.assertEqual(firebirdsql.DATE, datetime.date)
        self.assertEqual(datetime.date, firebirdsql.DATE)

        self.assertEqual(firebirdsql.TIME, datetime.time)
        self.assertEqual(datetime.time, firebirdsql.TIME)

        self.assertEqual(firebirdsql.TIMESTAMP, datetime.datetime)
        self.assertEqual(datetime.datetime, firebirdsql.TIMESTAMP)

        self.assertEqual(firebirdsql.BINARY, bytes)
        self.assertEqual(bytes, firebirdsql.BINARY)
        self.assertEqual(firebirdsql.BINARY, bytearray)
        self.assertEqual(firebirdsql.BINARY, memoryview)

    def test_dbapi_type_objects_sql_constants(self):
        # Bidirectional equality for Firebird SQL types
        self.assertEqual(firebirdsql.STRING, SQL_TYPE_TEXT)
        self.assertEqual(SQL_TYPE_VARYING, firebirdsql.STRING)
        self.assertEqual(firebirdsql.STRING, SQL_TYPE_TEXT + 1)
        self.assertEqual(firebirdsql.STRING, SQL_TYPE_VARYING + 1)

        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_SHORT)
        self.assertEqual(SQL_TYPE_LONG, firebirdsql.NUMBER)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_INT64)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_INT128)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_FLOAT)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_DOUBLE)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_DEC64)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_DEC128)
        self.assertEqual(firebirdsql.NUMBER, SQL_TYPE_BOOLEAN)

        self.assertEqual(firebirdsql.DATETIME, SQL_TYPE_DATE)
        self.assertEqual(firebirdsql.DATETIME, SQL_TYPE_TIME)
        self.assertEqual(firebirdsql.DATETIME, SQL_TYPE_TIMESTAMP)
        self.assertEqual(firebirdsql.DATETIME, SQL_TYPE_TIMESTAMP_TZ)
        self.assertEqual(firebirdsql.DATETIME, SQL_TYPE_TIME_TZ)

        self.assertEqual(firebirdsql.DATE, SQL_TYPE_DATE)
        self.assertEqual(firebirdsql.TIME, SQL_TYPE_TIME)
        self.assertEqual(firebirdsql.TIME, SQL_TYPE_TIME_TZ)
        self.assertEqual(firebirdsql.TIMESTAMP, SQL_TYPE_TIMESTAMP)
        self.assertEqual(firebirdsql.TIMESTAMP, SQL_TYPE_TIMESTAMP_TZ)

        self.assertEqual(firebirdsql.BINARY, SQL_TYPE_BLOB)
        self.assertEqual(firebirdsql.BINARY, SQL_TYPE_ARRAY)
        self.assertEqual(firebirdsql.BINARY, SQL_TYPE_QUAD)

    def test_dbapi_type_objects_hashable(self):
        # Type objects can be used as dict keys (common in ORMs)
        type_mapping = {
            firebirdsql.STRING: "string",
            firebirdsql.NUMBER: "number",
            firebirdsql.DATETIME: "datetime",
            firebirdsql.BINARY: "binary",
            firebirdsql.ROWID: "rowid",
        }
        self.assertEqual(type_mapping[firebirdsql.STRING], "string")
        self.assertEqual(type_mapping[firebirdsql.NUMBER], "number")
        self.assertEqual(type_mapping[firebirdsql.DATETIME], "datetime")
        self.assertEqual(type_mapping[firebirdsql.BINARY], "binary")
        self.assertEqual(type_mapping[firebirdsql.ROWID], "rowid")

    def test_dbapi_type_objects_cursor_description(self):
        # Simulates checking cursor.description[i][1] == TYPE_OBJECT
        simulated_description = [
            ("ID", SQL_TYPE_LONG, None, None, None, None, None),
            ("NAME", SQL_TYPE_VARYING, None, None, None, None, None),
            ("CREATED_AT", SQL_TYPE_TIMESTAMP, None, None, None, None, None),
            ("DATA", SQL_TYPE_BLOB, None, None, None, None, None),
        ]
        self.assertEqual(simulated_description[0][1], firebirdsql.NUMBER)
        self.assertEqual(simulated_description[1][1], firebirdsql.STRING)
        self.assertEqual(simulated_description[2][1], firebirdsql.DATETIME)
        self.assertEqual(simulated_description[2][1], firebirdsql.TIMESTAMP)
        self.assertEqual(simulated_description[3][1], firebirdsql.BINARY)


if __name__ == "__main__":
    unittest.main()
