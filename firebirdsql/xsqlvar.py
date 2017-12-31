##############################################################################
# Copyright (c) 2009-2018, Hajime Nakagami<nakagami@gmail.com>
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

from firebirdsql.consts import *
from firebirdsql.utils import *
from firebirdsql.wireprotocol import INFO_SQL_SELECT_DESCRIBE_VARS
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
        SQL_TYPE_DEC64 : 8,
        SQL_TYPE_DEC128 : 16,
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
        self.null_ok = None
        self.fieldname = ''
        self.relname = ''
        self.ownname = ''
        self.aliasname = ''

    def io_length(self):
        sqltype = self.sqltype
        if sqltype == SQL_TYPE_TEXT:
            return self.sqllen
        else:
            return self.type_length[sqltype]

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

    def value(self, raw_value):
        if self.sqltype == SQL_TYPE_TEXT:
            return self.bytes_to_str(raw_value).rstrip()
        elif self.sqltype == SQL_TYPE_VARYING:
            return self.bytes_to_str(raw_value)
        elif self.sqltype in (SQL_TYPE_SHORT, SQL_TYPE_LONG, SQL_TYPE_INT64):
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
            return True if byte_to_int(raw_value[0]) != 0 else False
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
    }


def calc_blr(xsqlda):
    "Calculate  BLR from XSQLVAR array."
    ln = len(xsqlda) * 2
    blr = [5, 2, 4, 0, ln & 255, ln >> 8]
    for x in xsqlda:
        sqltype = x.sqltype
        if sqltype == SQL_TYPE_VARYING:
            blr += [37, x.sqllen & 255, x.sqllen >> 8]
        elif sqltype == SQL_TYPE_TEXT:
            blr += [14, x.sqllen & 255, x.sqllen >> 8]
        elif sqltype == SQL_TYPE_LONG:
            blr += [8, x.sqlscale]
        elif sqltype == SQL_TYPE_SHORT:
            blr += [7, x.sqlscale]
        elif sqltype == SQL_TYPE_INT64:
            blr += [16, x.sqlscale]
        elif sqltype == SQL_TYPE_QUAD:
            blr += [9, x.sqlscale]
        elif sqltype == SQL_TYPE_DEC_FIXED:
            blr += [26, x.sqlscale]
        else:
            blr += sqltype2blr[sqltype]
        blr += [7, 0]   # [blr_short, 0]
    blr += [255, 76]    # [blr_end, blr_eoc]

    # x.sqlscale value shoud be negative, so b convert to range(0, 256)
    return bs(256 + b if b < 0 else b for b in blr)


def parse_select_items(buf, xsqlda, connection):
    index = 0
    i = 0
    item = byte_to_int(buf[i])
    while item != isc_info_end:
        if item == isc_info_sql_sqlda_seq:
            l = bytes_to_int(buf[i+1:i+3])
            index = bytes_to_int(buf[i+3:i+3+l])
            xsqlda[index-1] = XSQLVAR(connection.bytes_to_ustr if connection.use_unicode else connection.bytes_to_str)
            i = i + 3 + l
        elif item == isc_info_sql_type:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].sqltype = bytes_to_int(buf[i+3:i+3+l]) & ~ 1
            i = i + 3 + l
        elif item == isc_info_sql_sub_type:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].sqlsubtype = bytes_to_int(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_scale:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].sqlscale = bytes_to_int(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_length:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].sqllen = bytes_to_int(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_null_ind:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].null_ok = bytes_to_int(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_field:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].fieldname = connection.bytes_to_str(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_relation:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].relname = connection.bytes_to_str(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_owner:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].ownname = connection.bytes_to_str(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_sql_alias:
            l = bytes_to_int(buf[i+1:i+3])
            xsqlda[index-1].aliasname = connection.bytes_to_str(buf[i+3:i+3+l])
            i = i + 3 + l
        elif item == isc_info_truncated:
            return index    # return next index
        elif item == isc_info_sql_describe_end:
            i = i + 1
        else:
            print('\t', item, 'Invalid item [%02x] ! i=%d' % (buf[i], i))
            i = i + 1
        item = byte_to_int(buf[i])
    return -1   # no more info


def parse_xsqlda(buf, connection, stmt_handle):
    xsqlda = []
    stmt_type = None
    i = 0
    while i < len(buf):
        if buf[i:i+3] == bs([isc_info_sql_stmt_type, 0x04, 0x00]):
            stmt_type = bytes_to_int(buf[i+3:i+7])
            i += 7
        elif buf[i:i+2] == bs([isc_info_sql_select, isc_info_sql_describe_vars]):
            i += 2
            l = bytes_to_int(buf[i:i+2])
            i += 2
            col_len = bytes_to_int(buf[i:i+l])
            xsqlda = [None] * col_len
            next_index = parse_select_items(buf[i+l:], xsqlda, connection)
            while next_index > 0:   # more describe vars
                connection._op_info_sql(
                    stmt_handle,
                    bs([isc_info_sql_sqlda_start, 2]) + int_to_bytes(next_index, 2) + INFO_SQL_SELECT_DESCRIBE_VARS
                )
                (h, oid, buf) = connection._op_response()
                assert buf[:2] == bs([0x04, 0x07])
                l = bytes_to_int(buf[2:4])
                assert bytes_to_int(buf[4:4+l]) == col_len
                next_index = parse_select_items(buf[4+l:], xsqlda, connection)
        else:
            break
    return stmt_type, xsqlda
