"""
Text columns the server does not transliterate to the connection charset.

A CHARACTER SET NONE column (the default in a database created without
DEFAULT CHARACTER SET) is described with its byte length, e.g. sqllen 40 for
VARCHAR(40) CHARACTER SET NONE, even under a UTF8 connection.

Firebird 6 sizes a charset-less blr_varying/blr_text output item in characters
of the connection charset, so a 40-byte item holds only 40 / 4 = 10 UTF8
characters and fetching a longer value failed with::

    arithmetic exception, numeric overflow, or string truncation
    string right truncation
    expected length 10, actual 15

Firebird 2.5 to 5.0 treat the same item as 40 bytes. The driver now declares
untransliterated text items as char length * connection bytes per character
(see firebirdsql.xsqlvar.widen_untransliterated_text). These tests pass on
every server version; before the fix the UTF8 cases failed on Firebird 6.
"""
import unittest

import firebirdsql
from firebirdsql.consts import SQL_TYPE_TEXT, SQL_TYPE_VARYING, SQL_TYPE_LONG
from firebirdsql.tests.base import *    # noqa
from firebirdsql.xsqlvar import (
    XSQLVAR, calc_blr, charset_bytes_per_char, connection_bytes_per_char,
    widen_untransliterated_text,
)


def _var(sqltype, subtype, sqllen):
    x = XSQLVAR(lambda b: b.decode())
    x.sqltype, x.sqlsubtype, x.sqllen, x.sqlscale = sqltype, subtype, sqllen, 0
    return x


class TestWidenUntransliteratedText(unittest.TestCase):
    """Unit tests: no server needed."""

    def test_widening_rules(self):
        NONE, OCTETS, ASCII, FSS, UTF8, SJIS, WIN1252 = 0, 1, 2, 3, 4, 5, 53
        cases = [
            # (description, sqltype, charset id, sqllen, connection width, message length)
            ('NONE varchar under UTF8', SQL_TYPE_VARYING, NONE, 40, 4, 160),
            ('NONE char under UTF8', SQL_TYPE_TEXT, NONE, 11, 4, 44),
            ('NONE varchar under UNICODE_FSS', SQL_TYPE_VARYING, NONE, 40, 3, 120),
            ('NONE varchar under SJIS_0208', SQL_TYPE_VARYING, NONE, 40, 2, 80),
            ('NONE varchar under WIN1252', SQL_TYPE_VARYING, NONE, 40, 1, 40),
            ('ASCII is untransliterated too', SQL_TYPE_VARYING, ASCII, 10, 4, 40),
            ('collation in the high byte is ignored', SQL_TYPE_VARYING, NONE | 3 << 8, 10, 4, 40),
            ('transliterated column: described as UTF8', SQL_TYPE_VARYING, UTF8, 160, 4, 160),
            ('UTF8 column under SJIS: already wider', SQL_TYPE_VARYING, UTF8, 160, 2, 160),
            ('UNICODE_FSS column under UTF8', SQL_TYPE_VARYING, FSS, 30, 4, 40),
            ('SJIS column under UTF8', SQL_TYPE_TEXT, SJIS, 20, 4, 40),
            ('single-byte column under UTF8', SQL_TYPE_VARYING, WIN1252, 10, 4, 40),
            ('OCTETS is binary: never widened', SQL_TYPE_VARYING, OCTETS, 16, 4, 16),
            ('OCTETS char', SQL_TYPE_TEXT, OCTETS, 16, 4, 16),
            ('empty column', SQL_TYPE_VARYING, NONE, 0, 4, 0),
            ('capped to the 16-bit BLR length', SQL_TYPE_VARYING, NONE, 20000, 4, 65532),
            ('cap is a multiple of the char width', SQL_TYPE_VARYING, NONE, 30000, 3, 65535),
            ('cap never shrinks a column', SQL_TYPE_VARYING, NONE, 32765, 4, 65532),
        ]
        for desc, sqltype, subtype, sqllen, width, want in cases:
            with self.subTest(desc):
                x = _var(sqltype, subtype, sqllen)
                widen_untransliterated_text([x], width)
                self.assertEqual(want, x.message_length())
                self.assertEqual(sqllen, x.sqllen, 'the described length is kept')
                if sqltype == SQL_TYPE_TEXT:
                    self.assertEqual(sqllen, x.display_length(), 'metadata keeps the described length')

    def test_other_types_untouched(self):
        x = _var(SQL_TYPE_LONG, 0, 4)
        widen_untransliterated_text([x], 4)
        self.assertIsNone(x.msglen)

    def test_blr_and_wire_length(self):
        xs = [
            _var(SQL_TYPE_VARYING, 0, 40),  # NONE
            _var(SQL_TYPE_TEXT, 0, 11),     # NONE
            _var(SQL_TYPE_VARYING, 4, 80),  # UTF8
        ]
        widen_untransliterated_text(xs, 4)
        self.assertEqual(bytes([
            5, 2, 4, 0, 6, 0,   # blr_version5, blr_begin, blr_message 0, 6 items
            37, 160, 0, 7, 0,   # blr_varying(160), blr_short 0 (null indicator)
            14, 44, 0, 7, 0,    # blr_text(44), blr_short 0
            37, 80, 0, 7, 0,    # blr_varying(80), blr_short 0
            255, 76,            # blr_end, blr_eoc
        ]), calc_blr(xs))
        self.assertEqual(44, xs[1].wire_length(), 'CHAR is read with its message length')
        self.assertEqual(11, xs[1].io_length(), 'cursor.description keeps the described length')
        self.assertEqual(-1, xs[0].wire_length(), 'VARCHAR is read with its length prefix')

    def test_charset_widths(self):
        # RDB$CHARACTER_SETS rows with RDB$BYTES_PER_CHARACTER > 1 (Firebird 2.5-6.0)
        wide = {3: 3, 4: 4, 5: 2, 6: 2, 44: 2, 56: 2, 57: 2, 67: 2, 68: 2, 69: 4}
        for charset_id in range(256):
            self.assertEqual(wide.get(charset_id, 1), charset_bytes_per_char(charset_id), charset_id)
        for name, want in {
            'UTF8': 4, 'utf8': 4, 'GB18030': 4, 'UNICODE_FSS': 3, 'SJIS_0208': 2, 'EUCJ_0208': 2,
            'KSC_5601': 2, 'BIG_5': 2, 'GB_2312': 2, 'GBK': 2, 'CP943C': 2,
            'WIN1252': 1, 'WIN1251': 1, 'ISO8859_1': 1, 'KOI8R': 1, None: 1,
        }.items():
            self.assertEqual(want, connection_bytes_per_char(name), name)


class TestCharsetNoneColumns(TestBase):
    """Live tests against the server."""

    def _connect(self, charset):
        return firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name, wire_crypt=self.wire_crypt,
            host=self.host, port=self.port, database=self.database,
            user=self.user, password=self.password, charset=charset)

    def setUp(self):
        super().setUp()
        cur = self.connection.cursor()
        cur.execute('''
            CREATE TABLE cs_none (
                id  INTEGER NOT NULL PRIMARY KEY,
                v40 VARCHAR(40) CHARACTER SET NONE,
                c11 CHAR(11) CHARACTER SET NONE,
                u20 VARCHAR(20) CHARACTER SET UTF8,
                n   INTEGER)''')
        self.connection.commit()
        self.rows = [
            (1, 'science-fiction', 'PRIMARY KEY'),  # 15 > 40/4 and 11 > 11/4
            (2, 'x' * 40, 'UNIQUE'),                # full length; CHAR padding
            (3, '', ''),                            # empty
            (4, 'abc', 'x'),                        # short
        ]
        for i, v40, c11 in self.rows:
            cur.execute(
                "INSERT INTO cs_none (id, v40, c11, u20, n) VALUES (%d, '%s', '%s', 'tail', %d)"
                % (i, v40, c11, i * 10))
        self.connection.commit()

    def test_fetch_over_connection_charsets(self):
        for charset in ('UTF8', 'UNICODE_FSS', 'SJIS_0208', 'WIN1252'):
            with self.subTest(charset=charset):
                conn = self._connect(charset)
                try:
                    cur = conn.cursor()
                    cur.execute('SELECT id, v40, c11, u20, n FROM cs_none ORDER BY id')
                    got = cur.fetchall()
                    self.assertEqual(
                        [(i, v40, c11, 'tail', i * 10) for i, v40, c11 in self.rows],
                        [tuple(r) for r in got])
                    # a NONE value as a parameter (input message) keeps working
                    cur.execute('SELECT id FROM cs_none WHERE v40 = ?', ('science-fiction',))
                    self.assertEqual(1, cur.fetchone()[0])
                finally:
                    conn.close()

    def test_description_keeps_described_length(self):
        cur = self.connection.cursor()
        cur.execute('SELECT v40, c11 FROM cs_none')
        desc = cur.description
        self.assertEqual(11, desc[1][3], 'CHAR internal_size is the described length')
        self.assertEqual(11, desc[1][2])
        cur.fetchall()

    def test_system_tables(self):
        # RDB$CONSTRAINT_TYPE is CHAR(11) CHARACTER SET NONE: on Firebird 6 over
        # UTF8 it failed with "expected length 2, actual 11".
        cur = self.connection.cursor()
        cur.execute('''CREATE TABLE cs_none_child (id INTEGER NOT NULL PRIMARY KEY,
                         parent_id INTEGER REFERENCES cs_none (id))''')
        self.connection.commit()
        cur.execute('''SELECT TRIM(RDB$RELATION_NAME), RDB$CONSTRAINT_TYPE
                       FROM RDB$RELATION_CONSTRAINTS
                       WHERE RDB$RELATION_NAME IN ('CS_NONE', 'CS_NONE_CHILD')
                         AND RDB$CONSTRAINT_TYPE <> 'NOT NULL'
                       ORDER BY 1, 2''')
        self.assertEqual([
            ('CS_NONE', 'PRIMARY KEY'),
            ('CS_NONE_CHILD', 'FOREIGN KEY'),
            ('CS_NONE_CHILD', 'PRIMARY KEY'),
        ], [tuple(r) for r in cur.fetchall()])

    def test_long_column(self):
        # VARCHAR(20000) NONE x 4 exceeds the 16-bit BLR length: the buffer is
        # capped, and a value that fits the cap is returned intact.
        cur = self.connection.cursor()
        cur.execute('CREATE TABLE cs_none_long (v VARCHAR(20000) CHARACTER SET NONE)')
        self.connection.commit()
        value = '0123456789' * 1500
        cur.execute('INSERT INTO cs_none_long (v) VALUES (?)', (value,))
        self.connection.commit()
        cur.execute('SELECT v FROM cs_none_long')
        self.assertEqual(value, cur.fetchone()[0])

    def test_wide_row(self):
        # six widened VARCHAR(8000) NONE columns make a 192 KB message, which
        # Firebird 3+ accepts (widening is not applied to Firebird 2.5)
        cur = self.connection.cursor()
        cur.execute('CREATE TABLE cs_none_wide (%s)' % ', '.join(
            'v%d VARCHAR(8000) CHARACTER SET NONE' % i for i in range(6)))
        self.connection.commit()
        value = 'a' * 8000
        cur.execute('INSERT INTO cs_none_wide VALUES (?, ?, ?, ?, ?, ?)', (value,) * 6)
        self.connection.commit()
        cur.execute('SELECT * FROM cs_none_wide')
        self.assertEqual((value,) * 6, tuple(cur.fetchone()))

    def test_aio_fetch(self):
        # the asyncio API describes and fetches through its own code path
        import asyncio
        import firebirdsql.aio

        async def _fetch():
            conn = await firebirdsql.aio.connect(
                auth_plugin_name=self.auth_plugin_name, wire_crypt=self.wire_crypt,
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password, charset='UTF8')
            try:
                cur = conn.cursor()
                await cur.execute('SELECT id, v40, c11, u20, n FROM cs_none ORDER BY id')
                return [tuple(r) for r in await cur.fetchall()]
            finally:
                await conn.close()

        self.assertEqual(
            [(i, v40, c11, 'tail', i * 10) for i, v40, c11 in self.rows],
            asyncio.run(_fetch()))
