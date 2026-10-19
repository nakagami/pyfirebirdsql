##############################################################################
# Copyright (c) 2026, Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#  this list of conditions and the following disclaimer in the documentation
#  and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Python DB-API 2.0 module for Firebird.
##############################################################################
import datetime
import decimal

from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql.wireprotocol import INFO_SQL_SELECT_DESCRIBE_VARS, INFO_SQL_BIND_DESCRIBE_VARS
from firebirdsql.tz_utils import get_tzinfo_by_id
from firebirdsql import decfloat


class XSQLVAR:
    type_length = {
        SQL_TYPE_VARYING: -1,
        SQL_TYPE_SHORT: 4,
        SQL_TYPE_LONG: 4,
        SQL_TYPE_FLOAT: 4,
        SQL_TYPE_TIME: 4,
        SQL_TYPE_DATE: 4,
        SQL_TYPE_DOUBLE: 8,
        SQL_TYPE_TIMESTAMP: 8,
        SQL_TYPE_BLOB: 8,
        SQL_TYPE_ARRAY: 8,
        SQL_TYPE_QUAD: 8,
        SQL_TYPE_INT64: 8,
        SQL_TYPE_INT128: 16,
        SQL_TYPE_TIMESTAMP_TZ: 12,
        SQL_TYPE_TIME_TZ: 8,
        SQL_TYPE_DEC64: 8,
        SQL_TYPE_DEC128: 16,
        SQL_TYPE_DEC_FIXED: 16,
        SQL_TYPE_BOOLEAN: 1,
        }

    type_display_length = {
        SQL_TYPE_VARYING: -1,
        SQL_TYPE_SHORT: 6,
        SQL_TYPE_LONG: 11,
        SQL_TYPE_FLOAT: 17,
        SQL_TYPE_TIME: 11,
        SQL_TYPE_DATE: 10,
        SQL_TYPE_DOUBLE: 17,
        SQL_TYPE_TIMESTAMP: 22,
        SQL_TYPE_BLOB: 0,
        SQL_TYPE_ARRAY: -1,
        SQL_TYPE_QUAD: 20,
        SQL_TYPE_INT64: 20,
        SQL_TYPE_INT128: 20,
        SQL_TYPE_TIMESTAMP_TZ: 28,
        SQL_TYPE_TIME_TZ: 17,
        SQL_TYPE_DEC64: 16,
        SQL_TYPE_DEC128: 34,
        SQL_TYPE_DEC_FIXED: 34,
        SQL_TYPE_BOOLEAN: 5,
        }

    def __init__(self, bytes_to_str):
        self.bytes_to_str = bytes_to_str
        self.sqltype = None
        self.sqlscale = None
        self.sqlsubtype = None
        self.sqllen = None
        # Byte length declared for a text column in the output message BLR
        # when it differs from sqllen (see widen_untransliterated_text).
        # None means sqllen. Metadata (io_length, display_length and so
        # cursor.description) keeps using sqllen.
        self.msglen = None
        self.null_ok = None
        self.fieldname = ''
        self.relname = ''
        self.ownname = ''
        self.aliasname = ''
        self.array_meta = None

    def io_length(self):
        sqltype = self.sqltype
        if sqltype == SQL_TYPE_TEXT:
            return self.sqllen
        else:
            return self.type_length[sqltype]

    def message_length(self):
        "Byte length of a text column in the message buffer."
        return self.msglen if self.msglen else self.sqllen

    def wire_length(self):
        "Bytes of a fetched value on the wire; -1 for a length-prefixed VARCHAR."
        if self.sqltype == SQL_TYPE_TEXT:
            return self.message_length()
        return self.type_length[self.sqltype]

    def display_length(self):
        sqltype = self.sqltype
        if sqltype == SQL_TYPE_TEXT:
            return self.sqllen
        else:
            return self.type_display_length[sqltype]

    def precision(self):
        return self.display_length()

    def __str__(self):
        s = ','.join([
            str(self.sqltype), str(self.sqlscale), str(self.sqlsubtype),
            str(self.sqllen), str(self.null_ok), self.fieldname,
            self.relname, self.ownname, self.aliasname,
        ])
        return '[' + s + ']'

    def _parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_bint(raw_value) + 678882
        century = (4 * nday - 1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day - 3) // 153
        day = 5 * day - 3 - 153 * month
        day = (day + 5) // 5
        year = 100 * century + nday
        if month < 10:
            month += 3
        else:
            month -= 9
            year += 1
        return year, month, day

    def _parse_time(self, raw_value):
        "Convert raw data to datetime.time"
        n = bytes_to_bint(raw_value)
        s = n // 10000
        m = s // 60
        h = m // 60
        m = m % 60
        s = s % 60
        return (h, m, s, (n % 10000) * 100)

    def _parse_time_zone(self, raw_value):
        return get_tzinfo_by_id(bytes_to_bint(raw_value, u=True))

    def value(self, raw_value):
        if self.sqltype == SQL_TYPE_TEXT:
            return self.bytes_to_str(raw_value).rstrip()
        elif self.sqltype == SQL_TYPE_VARYING:
            return self.bytes_to_str(raw_value)
        elif self.sqltype in (SQL_TYPE_SHORT, SQL_TYPE_LONG, SQL_TYPE_INT64, SQL_TYPE_INT128):
            n = bytes_to_bint(raw_value)
            if self.sqlscale:
                return decimal.Decimal(str(n) + 'e' + str(self.sqlscale))
            else:
                return n
        elif self.sqltype == SQL_TYPE_DATE:
            yyyy, mm, dd = self._parse_date(raw_value)
            return datetime.date(yyyy, mm, dd)
        elif self.sqltype == SQL_TYPE_TIME:
            h, m, s, ms = self._parse_time(raw_value)
            return datetime.time(h, m, s, ms)
        elif self.sqltype == SQL_TYPE_TIMESTAMP:
            yyyy, mm, dd = self._parse_date(raw_value[:4])
            h, m, s, ms = self._parse_time(raw_value[4:])
            return datetime.datetime(yyyy, mm, dd, h, m, s, ms)
        elif self.sqltype == SQL_TYPE_FLOAT:
            return struct.unpack('!f', raw_value)[0]
        elif self.sqltype == SQL_TYPE_DOUBLE:
            return struct.unpack('!d', raw_value)[0]
        elif self.sqltype == SQL_TYPE_BOOLEAN:
            return True if raw_value[0] != 0 else False
        elif self.sqltype == SQL_TYPE_TIMESTAMP_TZ:
            yyyy, mm, dd = self._parse_date(raw_value[:4])
            h, m, s, ms = self._parse_time(raw_value[4:8])
            if raw_value[8:10] == b'\x00\x00':
                return datetime.datetime(yyyy, mm, dd, h, m, s, ms, tzinfo=datetime.timezone.utc)
            tz = self._parse_time_zone(raw_value[8:10])
            offset = self._parse_time_zone(raw_value[10:12])
            dt = datetime.datetime(yyyy, mm, dd, h, m, s, ms, tzinfo=tz)
            return dt.astimezone(offset)
        elif self.sqltype == SQL_TYPE_TIME_TZ:
            h, m, s, ms = self._parse_time(raw_value[:4])
            if raw_value[4:6] == b'\x00\x00':
                return datetime.time(h, m, s, ms, tzinfo=datetime.timezone.utc)
            tz = self._parse_time_zone(raw_value[4:6])
            offset = self._parse_time_zone(raw_value[6:8])
            t = datetime.time(h, m, s, ms, tzinfo=tz)
            dt = datetime.datetime.combine(datetime.date.today(), t).astimezone(offset)
            t = datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=offset)
            return t
        elif self.sqltype == SQL_TYPE_DEC_FIXED:
            return decfloat.decimal_fixed_to_decimal(raw_value, self.sqlscale)
        elif self.sqltype == SQL_TYPE_DEC64:
            return decfloat.decimal64_to_decimal(raw_value)
        elif self.sqltype == SQL_TYPE_DEC128:
            return decfloat.decimal128_to_decimal(raw_value)
        else:
            return raw_value


sqltype2blr = {
    SQL_TYPE_DOUBLE: [27],
    SQL_TYPE_FLOAT: [10],
    SQL_TYPE_D_FLOAT: [11],
    SQL_TYPE_DATE: [12],
    SQL_TYPE_TIME: [13],
    SQL_TYPE_TIMESTAMP: [35],
    SQL_TYPE_BLOB: [9, 0],
    SQL_TYPE_ARRAY: [9, 0],
    SQL_TYPE_BOOLEAN: [23],
    SQL_TYPE_DEC64: [24],
    SQL_TYPE_DEC128: [25],
    SQL_TYPE_TIME_TZ: [28],
    SQL_TYPE_TIMESTAMP_TZ: [29],
    }


# RDB$CHARACTER_SETS.RDB$BYTES_PER_CHARACTER of the multi-byte charsets, by id
# and by name; every other charset is single-byte. The same in Firebird 2.5-6.0.
CHARSET_ID_OCTETS = 1
_BYTES_PER_CHAR_BY_ID = {3: 3, 4: 4, 5: 2, 6: 2, 44: 2, 56: 2, 57: 2, 67: 2, 68: 2, 69: 4}
_BYTES_PER_CHAR_BY_NAME = {
    'UNICODE_FSS': 3, 'UTF8': 4, 'GB18030': 4,
    'SJIS_0208': 2, 'EUCJ_0208': 2, 'KSC_5601': 2, 'BIG_5': 2, 'GB_2312': 2, 'GBK': 2, 'CP943C': 2,
}


def charset_bytes_per_char(charset_id):
    "Maximum bytes per character of a charset id."
    return _BYTES_PER_CHAR_BY_ID.get(charset_id, 1)


def connection_bytes_per_char(charset):
    "Maximum bytes per character of a connection charset name."
    return _BYTES_PER_CHAR_BY_NAME.get((charset or '').upper(), 1)


def widen_untransliterated_text(xsqlda, conn_bytes_per_char):
    """
    Size the output message buffer of text columns that the server returns in
    their own charset instead of the connection charset.

    Under a multi-byte connection charset (UTF8, UNICODE_FSS, SJIS_0208, ...)
    the server transliterates text columns and describes them in the
    connection charset: a WIN1252 VARCHAR(40) is described as sqllen 160 under
    UTF8. Columns in CHARACTER SET NONE (and OCTETS) are not transliterated and
    are described with their byte length: a NONE VARCHAR(40) is sqllen 40.

    The output message declares text items as blr_text/blr_varying, which
    carry no charset. Firebird 2.5 to 5.0 treat such an item's length as
    bytes. Firebird 6 treats it as characters of the connection charset, so a
    40-byte item holds only 40 / 4 = 10 UTF8 characters and fetching a longer
    NONE value fails with "string right truncation, expected length 10,
    actual 15". System columns such as RDB$CONSTRAINT_TYPE (CHAR(11)
    CHARACTER SET NONE) fail the same way: "expected length 2, actual 11".

    Declaring char length * connection bytes per character satisfies
    Firebird 6 and is harmless on Firebird 3 to 5: VARCHAR values carry their
    own length, and the blank padding of a wider CHAR buffer is stripped like
    any CHAR padding. The widened length is capped to fit the 16-bit BLR
    length. Callers apply it on protocol 13+ only (Firebird 3+): Firebird 2.5
    is not affected and rejects messages over 64 KB with "SQLDA error", which
    widening can produce. node-firebird applies the same widening (its issue
    #422).

    OCTETS columns are binary and never widened.
    """
    if conn_bytes_per_char <= 1:
        return
    max_len = 0xFFFF // conn_bytes_per_char * conn_bytes_per_char
    for x in xsqlda:
        if x is None or x.sqltype not in (SQL_TYPE_TEXT, SQL_TYPE_VARYING) or not x.sqllen:
            continue
        charset_id = (x.sqlsubtype or 0) & 0xFF
        if charset_id == CHARSET_ID_OCTETS:
            continue
        col_bytes_per_char = charset_bytes_per_char(charset_id)
        if col_bytes_per_char >= conn_bytes_per_char:
            continue    # transliterated, or already as wide as the connection charset
        widened = min(x.sqllen // col_bytes_per_char * conn_bytes_per_char, max_len)
        if widened > x.sqllen:
            x.msglen = widened


def widen_output_messages(xsqlda, connection):
    "Apply widen_untransliterated_text for a statement's output columns."
    if connection.accept_version >= PROTOCOL_VERSION13:
        widen_untransliterated_text(xsqlda, connection_bytes_per_char(connection.charset))


def calc_blr(xsqlda):
    "Calculate  BLR from XSQLVAR array."
    ln = len(xsqlda) * 2
    blr = [5, 2, 4, 0, ln & 255, ln >> 8]
    for x in xsqlda:
        sqltype = x.sqltype
        if sqltype == SQL_TYPE_VARYING:
            ln = x.message_length()
            blr += [37, ln & 255, ln >> 8]
        elif sqltype == SQL_TYPE_TEXT:
            ln = x.message_length()
            blr += [14, ln & 255, ln >> 8]
        elif sqltype == SQL_TYPE_LONG:
            blr += [8, x.sqlscale]
        elif sqltype == SQL_TYPE_SHORT:
            blr += [7, x.sqlscale]
        elif sqltype == SQL_TYPE_INT64:
            blr += [16, x.sqlscale]
        elif sqltype == SQL_TYPE_INT128:
            blr += [26, x.sqlscale]
        elif sqltype == SQL_TYPE_QUAD:
            blr += [9, x.sqlscale]
        elif sqltype == SQL_TYPE_DEC_FIXED:
            blr += [26, x.sqlscale]
        else:
            blr += sqltype2blr[sqltype]
        blr += [7, 0]   # [blr_short, 0]
    blr += [255, 76]    # [blr_end, blr_eoc]

    # x.sqlscale value should be negative, so b convert to range(0, 256)
    return bytes(256 + b if b < 0 else b for b in blr)


def parse_select_items_with_offset(buf, offset, xsqlda, connection):
    index = 0
    i = offset
    while i < len(buf):
        item = buf[i]
        if item == isc_info_end:
            return -1, i
        elif item in (isc_info_sql_select, isc_info_sql_bind) and i + 1 < len(buf) and buf[i+1] == isc_info_sql_describe_vars:
            return -1, i
        elif item == isc_info_truncated:
            return index, i + 1
        elif item == isc_info_sql_describe_end:
            i += 1
        elif item == isc_info_sql_sqlda_seq:
            ln = bytes_to_int(buf[i+1:i+3])
            index = bytes_to_int(buf[i+3:i+3+ln])
            if index - 1 < len(xsqlda):
                xsqlda[index-1] = XSQLVAR(connection.bytes_to_str)
            i = i + 3 + ln
        elif item == isc_info_sql_type:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].sqltype = bytes_to_int(buf[i+3:i+3+ln]) & ~ 1
            i = i + 3 + ln
        elif item == isc_info_sql_sub_type:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].sqlsubtype = bytes_to_int(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_scale:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].sqlscale = bytes_to_int(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_length:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].sqllen = bytes_to_int(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_null_ind:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].null_ok = bytes_to_int(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_field:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].fieldname = connection.bytes_to_str(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_relation:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].relname = connection.bytes_to_str(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_owner:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].ownname = connection.bytes_to_str(buf[i+3:i+3+ln])
            i = i + 3 + ln
        elif item == isc_info_sql_alias:
            ln = bytes_to_int(buf[i+1:i+3])
            if index - 1 < len(xsqlda) and xsqlda[index-1] is not None:
                xsqlda[index-1].aliasname = connection.bytes_to_str(buf[i+3:i+3+ln])
            i = i + 3 + ln
        else:
            i = i + 1
    return -1, i


def parse_select_items(buf, xsqlda, connection):
    index, _ = parse_select_items_with_offset(buf, 0, xsqlda, connection)
    return index


def parse_xsqlda(buf, connection, stmt_handle):
    from firebirdsql.array import resolve_array_meta
    xsqlda = []
    bind_xsqlda = []
    stmt_type = None
    i = 0
    while i < len(buf):
        if buf[i:i+3] == bytes([isc_info_sql_stmt_type, 0x04, 0x00]):
            stmt_type = bytes_to_int(buf[i+3:i+7])
            i += 7
        elif buf[i:i+2] == bytes([isc_info_sql_select, isc_info_sql_describe_vars]):
            i += 2
            ln = bytes_to_int(buf[i:i+2])
            i += 2
            col_len = bytes_to_int(buf[i:i+ln])
            i += ln
            xsqlda = [None] * col_len
            next_index, i = parse_select_items_with_offset(buf, i, xsqlda, connection)
            while next_index > 0:   # more describe vars
                connection._op_info_sql(
                    stmt_handle,
                    bytes([isc_info_sql_sqlda_start, 2]) + int_to_bytes(next_index, 2) + INFO_SQL_SELECT_DESCRIBE_VARS
                )
                (h, oid, buf2) = connection._op_response()
                assert buf2[:2] == bytes([0x04, 0x07])
                ln2 = bytes_to_int(buf2[2:4])
                assert bytes_to_int(buf2[4:4+ln2]) == col_len
                next_index, _ = parse_select_items_with_offset(buf2, 4+ln2, xsqlda, connection)
        elif buf[i:i+2] == bytes([isc_info_sql_bind, isc_info_sql_describe_vars]):
            i += 2
            ln = bytes_to_int(buf[i:i+2])
            i += 2
            col_len = bytes_to_int(buf[i:i+ln])
            i += ln
            bind_xsqlda = [None] * col_len
            next_index, i = parse_select_items_with_offset(buf, i, bind_xsqlda, connection)
            while next_index > 0:   # more describe vars
                connection._op_info_sql(
                    stmt_handle,
                    bytes([isc_info_sql_sqlda_start, 2]) + int_to_bytes(next_index, 2) + INFO_SQL_BIND_DESCRIBE_VARS
                )
                (h, oid, buf2) = connection._op_response()
                assert buf2[:2] == bytes([0x08, 0x07])
                ln2 = bytes_to_int(buf2[2:4])
                assert bytes_to_int(buf2[4:4+ln2]) == col_len
                next_index, _ = parse_select_items_with_offset(buf2, 4+ln2, bind_xsqlda, connection)
        elif buf[i] == isc_info_end:
            break
        else:
            i += 1

    resolve_array_meta(connection, xsqlda)
    resolve_array_meta(connection, bind_xsqlda)
    widen_output_messages(xsqlda, connection)
    return stmt_type, xsqlda, bind_xsqlda
