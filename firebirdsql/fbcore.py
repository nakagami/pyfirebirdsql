##############################################################################
# Copyright (c) 2009-2011 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import sys, os, socket
import xdrlib, time, datetime, decimal, struct
from firebirdsql.fberrmsgs import messages
from firebirdsql import (DatabaseError, InternalError, OperationalError, 
    ProgrammingError, IntegrityError, DataError, NotSupportedError,
)
from firebirdsql.consts import *

PYTHON_MAJOR_VER = sys.version_info[0]

def bi(b):
    "byte to integer"
    if PYTHON_MAJOR_VER==3:
        return b
    return ord(b)

def bs(byte_array):
    "bytes to string"
    if PYTHON_MAJOR_VER==3:
        return bytes(byte_array)
    return ''.join([chr(c) for c in byte_array])

DEBUG = False
__version__ = '0.6.0'
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'


transaction_parameter_block = [
    # ISOLATION_LEVEL_READ_UNCOMMITTED
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
    # ISOLATION_LEVEL_READ_COMMITED
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
    # ISOLATION_LEVEL_REPEATABLE_READ
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_concurrency]),
    # ISOLATION_LEVEL_SERIALIZABLE
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_consistency]),
    # ISOLATION_LEVEL_READ_COMMITED_READ_ONLY
    bs([isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
]

INFO_SQL_SELECT_DESCRIBE_VARS = bs([
    isc_info_sql_select,
    isc_info_sql_describe_vars,
    isc_info_sql_sqlda_seq,
    isc_info_sql_type,
    isc_info_sql_sub_type,
    isc_info_sql_scale,
    isc_info_sql_length,
    isc_info_sql_null_ind,
    isc_info_sql_field,
    isc_info_sql_relation,
    isc_info_sql_owner,
    isc_info_sql_alias,
    isc_info_sql_describe_end])

def Date(year, month, day):
    return datetime.date(year, month, day)
def Time(hour, minite, second):
    return datetime.time(hour, minite, second)
def DateFromTicks(ticks):
    return apply(Date,time.localtime(ticks)[:3])
def TimeFromTicks(ticks):
    return apply(Time,time.localtime(ticks)[3:6])
def TimestampFromTicks(ticks):
    return apply(Timestamp,time.localtime(ticks)[:6])
def Binary(b):
    return b

class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1
STRING = DBAPITypeObject(str)
if PYTHON_MAJOR_VER==3:
    BINARY = DBAPITypeObject(bytes)
else:
    BINARY = DBAPITypeObject(str)
NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID = DBAPITypeObject()

def recv_channel(sock, nbytes, word_alignment=False):
    n = nbytes
    if word_alignment and (n % 4):
        n += 4 - nbytes % 4  # 4 bytes word alignment
    r = bs([])
    while n:
        b = sock.recv(n)
        r += b
        n -= len(b)
    return r[:nbytes]

def send_channel(sock, b):
    sock.send(b)

def bytes_to_bint(b):           # Read as big endian
    len_b = len(b)
    if len_b == 1:
        fmt = 'b'
    elif len_b ==2:
        fmt = '>h'
    elif len_b ==4:
        fmt = '>l'
    elif len_b ==8:
        fmt = '>q'
    else:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bytes_to_int(b):            # Read as little endian.
    len_b = len(b)
    if len_b == 1:
        fmt = 'b'
    elif len_b ==2:
        fmt = '<h'
    elif len_b ==4:
        fmt = '<l'
    elif len_b ==8:
        fmt = '<q'
    else:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bint_to_bytes(val, nbytes): # Convert int value to big endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8*(nbytes - n - 1)) & 0xff))
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[-1] += 1
        for i in range(nbytes):
            if b[nbytes -i -1] == 256:
                b[nbytes -i -1] = 0
                b[nbytes -i -2] += 1
    return bs(b)

def int_to_bytes(val, nbytes):  # Convert int value to little endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8 * n)) & 0xff)
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[0] += 1
        for i in range(nbytes):
            if b[i] == 256:
                b[i] = 0
                b[i+1] += 1
    return bs(b)

def convert_date(v):  # Convert datetime.date to BLR format data
    i = v.month + 9
    jy = v.year + (i // 12) -1
    jm = i % 12
    c = jy // 100
    jy -= 100 * c
    j = (146097*c) // 4 + (1461*jy) // 4 + (153*jm+2) // 5 + v.day - 678882
    return bint_to_bytes(j, 4)

def convert_time(v):  # Convert datetime.time to BLR format time
    t = (v.hour*3600 + v.minute*60 + v.second) *10000 + v.microsecond // 100
    return bint_to_bytes(t, 4)

def convert_timestamp(v):   # Convert datetime.datetime to BLR format timestamp
    return convert_date(v.date()) + convert_time(v.time())


def wire_operation(fn):
    if not DEBUG:
        return fn
    def f(*args):
        print(fn, args)
        return fn(*args)
    return f
#------------------------------------------------------------------------------
class XSQLVAR:
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
        if self.sqltype == SQL_TYPE_TEXT:
            return self.sqllen
        elif self.sqltype == SQL_TYPE_VARYING:
            return -1   # First 4 bytes 
        elif self.sqltype in (SQL_TYPE_SHORT, SQL_TYPE_LONG, SQL_TYPE_FLOAT,
                SQL_TYPE_TIME, SQL_TYPE_DATE):
            return 4
        elif self.sqltype in (SQL_TYPE_DOUBLE, SQL_TYPE_TIMESTAMP,
                SQL_TYPE_BLOB, SQL_TYPE_ARRAY, SQL_TYPE_QUAD, SQL_TYPE_INT64):
            return 8

    def __str__(self):
        s  = '[' + str(self.sqltype) + ',' + str(self.sqlscale) + ',' \
                + str(self.sqlsubtype) + ',' + str(self.sqllen)  + ',' \
                + str(self.null_ok) + ',' + self.fieldname + ',' \
                + self.relname + ',' + self.ownname + ',' \
                + self.aliasname + ']'
        return s

    def _parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_bint(raw_value) + 678882
        century = (4 * nday -1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day -3) // 153
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
            if self.sqlsubtype in (4, 69):  # UTF8 and GB18030 
                reallength = self.sqllen // 4 
                return self.bytes_to_str(raw_value)[:reallength]
            elif self.sqlsubtype == 3:      # UNICODE_FSS 
                reallength = self.sqllen // 3
                return self.bytes_to_str(raw_value)[:reallength]
            else:
                return self.bytes_to_str(raw_value) 
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
        else:
            return raw_value

def calc_blr(xsqlda):
    "Calculate  BLR from XSQLVAR array."
    ln = len(xsqlda) *2
    blr = [5, 2, 4, 0, ln & 255, ln >> 8]
    for x in xsqlda:
        if x.sqltype == SQL_TYPE_VARYING:
            blr += [37, x.sqllen & 255, x.sqllen >> 8]
        elif x.sqltype == SQL_TYPE_TEXT:
            blr += [14, x.sqllen & 255, x.sqllen >> 8]
        elif x.sqltype == SQL_TYPE_DOUBLE:
            blr += [27]
        elif x.sqltype == SQL_TYPE_FLOAT:
            blr += [10]
        elif x.sqltype == SQL_TYPE_D_FLOAT:
            blr += [11]
        elif x.sqltype == SQL_TYPE_DATE:
            blr += [12]
        elif x.sqltype == SQL_TYPE_TIME:
            blr += [13]
        elif x.sqltype == SQL_TYPE_TIMESTAMP:
            blr += [35]
        elif x.sqltype == SQL_TYPE_BLOB:
            blr += [9, 0]
        elif x.sqltype == SQL_TYPE_ARRAY:
            blr += [9, 0]
        elif x.sqltype == SQL_TYPE_LONG:
            blr += [8, x.sqlscale]
        elif x.sqltype == SQL_TYPE_SHORT:
            blr += [7, x.sqlscale]
        elif x.sqltype == SQL_TYPE_INT64:
            blr += [16, x.sqlscale]
        elif x.sqltype == SQL_TYPE_QUAD:
            blr += [9, x.sqlscale]
        blr += [7, 0]   # [blr_short, 0]
    blr += [255, 76]    # [blr_end, blr_eoc]

    # x.sqlscale value shoud be negative, so b convert to range(0, 256)
    return bs([256 + b if b < 0 else b for b in blr])

def parse_select_items(buf, xsqlda, connection):
    index = 0
    i = 0
    item = bi(buf[i])
    while item != isc_info_end:
        if item == isc_info_sql_sqlda_seq:
            l = bytes_to_int(buf[i+1:i+3])
            index = bytes_to_int(buf[i+3:i+3+l])
            xsqlda[index-1] = XSQLVAR(connection.bytes_to_str)
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
        item = bi(buf[i])
    return -1   # no more info

def parse_xsqlda(buf, connection, stmt_handle):
    assert buf[:3] == bs([0x15,0x04,0x00]) # isc_info_sql_stmt_type
    stmt_type = bytes_to_int(buf[3:7])
    if stmt_type != isc_info_sql_stmt_select:
        return []

    assert buf[7:9] == bs([0x04,0x07])
    l = bytes_to_int(buf[9:11])
    col_len = bytes_to_int(buf[11:11+l])
    xsqlda = [None] * col_len
    next_index = parse_select_items(buf[11+l:], xsqlda, connection)
    while next_index > 0:   # more describe vars
        self.connection._op_info_sql(stmt_handle,
                    bs([isc_info_sql_sqlda_start, 2])
                        + int_to_bytes(next_index, 2)
                        + INFO_SQL_SELECT_DESCRIBE_VARS)
        (h, oid, buf) = connection._op_response()
        assert buf[:2] == bs([0x04,0x07])
        l = bytes_to_int(buf[2:4])
        assert bytes_to_int(buf[4:4+l]) == col_len
        next_index = parse_select_items(buf[4+l:], xsqlda, connection)
    return xsqlda

class PreparedStatement:
    def __init__(self, cur, sql):
        self.cur = cur
        self.sql = sql
        transaction = self.cur.transaction
        connection = transaction.connection

        connection._op_allocate_statement(transaction)
        (h, oid, buf) = connection._op_response()
        self.stmt_handle = h

        connection._op_prepare_statement(
                self.stmt_handle, transaction.trans_handle, sql, 
                option_items=bs([isc_info_sql_get_plan]))
        (h, oid, buf) = connection._op_response()

        i = 0
        if bi(buf[i]) == isc_info_sql_get_plan:
            l = bytes_to_int(buf[i+1:i+3])
            self.plan = connection.bytes_to_str(buf[i+3:i+3+l])
            i += 3 + l

        assert buf[i:i+3] == bs([0x15,0x04,0x00]) # isc_info_sql_stmt_type (4 bytes)
        self.statement_type = bytes_to_int(buf[i+3:i+7])
        self._xsqlda = parse_xsqlda(buf[i:], connection, self.stmt_handle)

        # TODO: implement later
        self.n_input_params = None

    def __getattr__(self, attrname):
        if attrname == 'description':
            if len(self._xsqlda) == 0:
                return None
            r = []
            for x in self._xsqlda:
                r.append((x.aliasname, x.sqltype, None, x.io_length(), None, 
                        x.sqlscale, True if x.null_ok else False))
            return r
        elif attrname == 'n_output_params':
            return len(self._xsqlda)
        raise AttributeError

class Cursor:
    def __init__(self, trans):
        self._transaction = trans
        self.transaction.connection._op_allocate_statement(self.transaction)
        (h, oid, buf) = self.transaction.connection._op_response()
        self.stmt_handle = h
        self.arraysize = 1

    @property
    def transaction(self):
        return self._transaction

    def callproc(self, procname, *params):
        raise NotSupportedError()

    def _convert_params(self, params):
        cooked_params = []
        for param in params:        # Convert str/bytes parameter to blob id
            if type(param) == str:
                param = self.transaction.connection.str_to_bytes(param)
            cooked_params.append(param)
            continue
            self.transaction.connection._op_create_blob2()
            (blob_handle, blob_id, buf2) = self.transaction.connection._op_response()
            seg_size = self.transaction.connection.buffer_length
            (seg, remains) = param[:seg_size], param[seg_size:]
            while seg:
                self.transaction.connection._op_batch_segments(blob_handle, seg)
                (h3, oid3, buf3) = self.transaction.connection._op_response()
                (seg, remains) = remains[:seg_size], remains[seg_size:]
            self.transaction.connection._op_close_blob(blob_handle)
            (h4, oid4, buf4) = self.transaction.connection._op_response()
            assert blob_id == oid4
            cooked_params.append(blob_id)
        return cooked_params

    def _execute(self, query, params):
        cooked_params = self._convert_params(params)

        if isinstance(query, PreparedStatement):
            stmt_handle = query.stmt_handle
            stmt_type = query.statement_type
            self._xsqlda = query._xsqlda
        else:
            stmt_handle = self.stmt_handle
            self.transaction.connection._op_prepare_statement(stmt_handle, 
                                        self.transaction.trans_handle, query)
            (h, oid, buf) = self.transaction.connection._op_response()
            assert buf[:3] == bs([0x15,0x04,0x00]) # isc_info_sql_stmt_type
            stmt_type = bytes_to_int(buf[3:7])
            self._xsqlda = parse_xsqlda(buf, self.transaction.connection, 
                                                                stmt_handle)

        self.transaction.connection._op_execute(stmt_handle, 
                                self.transaction.trans_handle, cooked_params)
        try:
            (h, oid, buf) = self.transaction.connection._op_response()
        except OperationalError:
            e = sys.exc_info()[1]
            if 335544665 in e.gds_codes:
                raise IntegrityError(e.message, e.gds_codes, e.sql_code)
        return stmt_type, stmt_handle

    def prep(self, query):
        prepared_statement = PreparedStatement(self, query)
        return prepared_statement

    def execute(self, query, params = []):
        stmt_type, stmt_handle = self._execute(query, params)
        if stmt_type == isc_info_sql_stmt_select:
            self._fetch_records = self._fetch_generator(stmt_handle)

    def executemany(self, query, seq_of_params):
        for params in seq_of_params:
            self.execute(query, params)

    def _fetch_generator(self, stmt_handle):
        connection = self.transaction.connection
        more_data = True
        while more_data:
            connection._op_fetch(stmt_handle, calc_blr(self._xsqlda))
            (rows, more_data) = connection._op_fetch_response(
                                            stmt_handle, self._xsqlda)
            for r in rows:
                # Convert BLOB handle to data    
                for i in range(len(self._xsqlda)):    
                    x = self._xsqlda[i]    
                    if x.sqltype == SQL_TYPE_BLOB:    
                        if not r[i]:
                            continue
                        connection._op_open_blob(r[i])
                        (h, oid, buf) = connection._op_response()
                        v = bs([])
                        n = 1   # 1:mora data 2:no more data
                        while n == 1:
                            connection._op_get_segment(h)
                            (n, oid, buf) = connection._op_response()
                            while buf:
                                ln = bytes_to_int(buf[:2])
                                v += buf[2:ln+2]
                                buf = buf[ln+2:]
                        connection._op_close_blob(h)
                        (h, oid, buf) = connection._op_response()
                        r[i] = v
                yield r

        # recreate stmt_handle
        connection._op_free_statement(stmt_handle, 2) # DSQL_drop
        (h, oid, buf) = connection._op_response()
        connection._op_allocate_statement(self.transaction)
        (h, oid, buf) = connection._op_response()
        self.stmt_handle = h

        raise StopIteration()
            
    def fetchone(self):
        try:
            if PYTHON_MAJOR_VER==3:
                return next(self._fetch_records)
            else:
                return self._fetch_records.next()
        except StopIteration:
            return None

    def fetchall(self):
        return list(self._fetch_records)

    def fetchmany(self, size=None):
        rows = []
        if not size:
            size = self.arraysize
        r = self.fetchone()
        while r:
            rows.append(r)
            size -= 1
            if size == 0:
                break
            r = self.fetchone()
        return rows

    def close(self):
        if not hasattr(self, "stmt_handle"):
            return
        self.transaction.connection._op_free_statement(self.stmt_handle, 2)   # DSQL_drop
        (h, oid, buf) = self.transaction.connection._op_response()
        delattr(self, "stmt_handle")

    def nextset():
        raise NotSupportedError()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column):
        pass

    def __getattr__(self, attrname):
        if attrname == 'description':
            r = []
            for x in self._xsqlda:
                r.append((x.aliasname, x.sqltype, None, x.io_length(), None, 
                        x.sqlscale, True if x.null_ok else False))
            return r
        elif attrname == 'rowcount':
            return -1
        raise AttributeError

class Connection:
    buffer_length = 1024
    op_connect = 1
    op_accept = 3
    op_response = 9
    op_dummy = 17
    op_attach = 19
    op_create = 20
    op_detach = 21
    op_transaction = 29
    op_commit = 30
    op_commit_retaining = 50
    op_rollback_retaining = 86
    op_rollback = 31
    op_open_blob = 35
    op_get_segment = 36
    op_close_blob = 39
    op_info_database = 40
    op_batch_segments = 44
    op_allocate_statement = 62
    op_create_blob2 = 57
    op_execute = 63
    op_fetch = 65
    op_fetch_response = 66
    op_free_statement = 67
    op_prepare_statement = 68
    op_info_sql = 70
    op_service_attach = 82
    op_service_detach = 83
    op_service_info = 84
    op_service_start = 85

    charset_map = {
    # DB CHAR SET NAME    :   PYTHON CODEC NAME (CANONICAL)
    # ---------------------------------------------------------------------------
    'OCTETS'              :   None, # Allow to pass through unchanged.
    'UNICODE_FSS'         :   'utf_8',
    'UTF8'                :   'utf_8', # (Firebird 2.0+)
    'SJIS_0208'           :   'shift_jis',
    'EUCJ_0208'           :   'euc_jp',
    'DOS737'              :   'cp737',
    'DOS437'              :   'cp437',
    'DOS850'              :   'cp850',
    'DOS865'              :   'cp865',
    'DOS860'              :   'cp860',
    'DOS863'              :   'cp863',
    'DOS775'              :   'cp775',
    'DOS862'              :   'cp862',
    'DOS864'              :   'cp864',
    'ISO8859_1'           :   'iso8859_1',
    'ISO8859_2'           :   'iso8859_2',
    'ISO8859_3'           :   'iso8859_3',
    'ISO8859_4'           :   'iso8859_4',
    'ISO8859_5'           :   'iso8859_5',
    'ISO8859_6'           :   'iso8859_6',
    'ISO8859_7'           :   'iso8859_7',
    'ISO8859_8'           :   'iso8859_8',
    'ISO8859_9'           :   'iso8859_9',
    'ISO8859_13'          :   'iso8859_13',
    'KSC_5601'            :   'euc_kr',
    'DOS852'              :   'cp852',
    'DOS857'              :   'cp857',
    'DOS861'              :   'cp861',
    'DOS866'              :   'cp866',
    'DOS869'              :   'cp869',
    'WIN1250'             :   'cp1250',
    'WIN1251'             :   'cp1251',
    'WIN1252'             :   'cp1252',
    'WIN1253'             :   'cp1253',
    'WIN1254'             :   'cp1254',
    'BIG_5'               :   'big5',
    'GB_2312'             :   'gb2312',
    'WIN1255'             :   'cp1255',
    'WIN1256'             :   'cp1256',
    'WIN1257'             :   'cp1257',
    'KOI8-R'              :   'koi8_r', # (Firebird 2.0+)
    'KOI8-U'              :   'koi8_u', # (Firebird 2.0+)
    'WIN1258'             :   'cp1258', # (Firebird 2.0+)
    }

    def str_to_bytes(self, s):
        return s.encode(self.charset_map.get(self.charset, self.charset))

    def bytes_to_str(self, b):
        "convert bytes array to raw string"
        if PYTHON_MAJOR_VER == 3:
            return b.decode(self.charset_map.get(self.charset, self.charset))
        return b

    def bytes_to_ustr(self, b):
        "convert bytes array to unicode string"
        return b.decode(self.charset_map.get(self.charset, self.charset))

    def uid(self):
        if sys.platform == 'win32':
            user = os.environ['USERNAME']
            hostname = os.environ['COMPUTERNAME']
        else:
            user = os.environ['USER']
            hostname = os.environ.get('HOSTNAME', '')
        return bs([1] + [len(user)] + [ord(c) for c in user] 
                + [4] + [len(hostname)] + [ord(c) for c in hostname] + [6, 0])

    def params_to_blr(self, params):
        "Convert parameter array to BLR and values format."
        ln = len(params) * 2
        blr = bs([5, 2, 4, 0, ln & 255, ln >> 8])
        values = bs([])
        for p in params:
            t = type(p)
            if ((PYTHON_MAJOR_VER == 2 and t == str) or
                (PYTHON_MAJOR_VER == 3 and t == bytes)):
                v = p
                nbytes = len(v)
                pad_length = ((4-nbytes) & 3)
                v += bs([0]) * pad_length
                blr += bs([14, nbytes & 255, nbytes >> 8])
            elif t == int:
                v = bint_to_bytes(p, 4)
                blr += bytes([7, 0])
            elif t == decimal.Decimal or t == float:
                if t == float:
                    p = decimal.Decimal(str(p))
                (sign, digits, exponent) = p.as_tuple()
                v = 0
                ln = len(digits)
                for i in range(ln):
                    v += digits[i] * (10 ** (ln -i-1))
                if sign:
                    v *= -1
                v = bint_to_bytes(v, 8)
                if exponent < 0:
                    exponent += 256
                blr += bytes([16, exponent])
            elif t == datetime.date:
                v = convert_date(p)
                blr += bytes([12])
            elif t == datetime.time:
                v = convert_time(p)
                blr += bytes([13])
            elif t == datetime.datetime:
                v = convert_timestamp(p)
                blr += bytes([35])
            elif p == None:
                v = bs([0]) * 8
                blr += bytes([9, 0])
            values += v
            blr += bs([7, 0])
            values += bs([0]) * 4 if p != None else bs([0xff,0xff,0x34,0x8c])
        blr += bs([255, 76])    # [blr_end, blr_eoc]
        return blr, values


    @wire_operation
    def _op_connect(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_connect)
        p.pack_int(self.op_attach)
        p.pack_int(2)   # CONNECT_VERSION2
        p.pack_int(1)   # Arch type (Generic = 1)
        p.pack_string(self.str_to_bytes(self.filename if self.filename else ''))
        p.pack_int(1)   # Protocol version understood count.
        p.pack_bytes(self.uid())
        p.pack_int(10)  # PROTOCOL_VERSION10
        p.pack_int(1)   # Arch type (Generic = 1)
        p.pack_int(2)   # Min type
        p.pack_int(3)   # Max type
        p.pack_int(2)   # Preference weight
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_create(self, page_size=4096):
        dpb = bs([1])
        s = self.str_to_bytes(self.charset)
        dpb += bs([68, len(s)]) + s
        dpb += bs([48, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bs([28, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bs([29, len(s)]) + s
        dpb += bs([63, 4]) + int_to_bytes(3, 4) # isc_dpb_sql_dialect = 3
        dpb += bs([24, 4]) + bint_to_bytes(1, 4) # isc_dpb_force_write = 1
        dpb += bs([54, 4]) + bint_to_bytes(1, 4) # isc_dpb_overwirte = 1
        dpb += bs([4, 4]) + bint_to_bytes(page_size, 4)
        p = xdrlib.Packer()
        p.pack_int(self.op_create)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_accept(self):
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        assert bytes_to_bint(b) == self.op_accept
        b = recv_channel(self.sock, 12)
        up = xdrlib.Unpacker(b)
        assert up.unpack_int() == 10
        assert  up.unpack_int() == 1
        assert up.unpack_int() == 3
        up.done()

    @wire_operation
    def _op_attach(self):
        dpb = bs([1])
        s = self.str_to_bytes(self.charset)
        dpb += bs([48, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bs([28, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bs([29, len(s)]) + s
        p = xdrlib.Packer()
        p.pack_int(self.op_attach)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_attach(self):
        dpb = bs([2,2])
        s = self.str_to_bytes(self.user)
        dpb += bs([isc_spb_user_name, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bs([isc_spb_password, len(s)]) + s
        dpb += bs([isc_spb_dummy_packet_interval,0x04,0x78,0x0a,0x00,0x00])
        p = xdrlib.Packer()
        p.pack_int(self.op_service_attach)
        p.pack_int(0)
        p.pack_string(self.str_to_bytes('service_mgr'))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_info(self, param, item, buffer_length=512):
        p = xdrlib.Packer()
        p.pack_int(self.op_service_info)
#        p.pack_int(self.svc_handle)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        p.pack_bytes(item)
        p.pack_int(buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_start(self, param):
        p = xdrlib.Packer()
        p.pack_int(self.op_service_start)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_detach(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_service_detach)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_info_database(self, b):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_database)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(b)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_transaction(self, tpb):
        p = xdrlib.Packer()
        p.pack_int(self.op_transaction)
        p.pack_int(self.db_handle)
        p.pack_bytes(tpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_commit(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_commit_retaining(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit_retaining)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_rollback(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_rollback_retaining(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback_retaining)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_allocate_statement(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_allocate_statement)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_free_statement(self, stmt_handle, mode):
        p = xdrlib.Packer()
        p.pack_int(self.op_free_statement)
        p.pack_int(stmt_handle)
        p.pack_int(mode)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_prepare_statement(self, stmt_handle, trans_handle, query, option_items=bs([])):
        desc_items = option_items + bs([isc_info_sql_stmt_type])+INFO_SQL_SELECT_DESCRIBE_VARS
        p = xdrlib.Packer()
        p.pack_int(self.op_prepare_statement)
        p.pack_int(trans_handle)
        p.pack_int(stmt_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_string(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_info_sql(self, stmt_handle, vars):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_sql)
        p.pack_int(stmt_handle)
        p.pack_int(0)
        p.pack_bytes(vars)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_execute(self, stmt_handle, trans_handle, params):
        p = xdrlib.Packer()
        p.pack_int(self.op_execute)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            p.pack_bytes(bs([]))
            p.pack_int(0)
            p.pack_int(0)
            send_channel(self.sock, p.get_buffer())
        else:
            (blr, values) = self.params_to_blr(params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)
            send_channel(self.sock, p.get_buffer() + values)

    @wire_operation
    def _op_fetch(self, stmt_handle, blr):
        p = xdrlib.Packer()
        p.pack_int(self.op_fetch)
        p.pack_int(stmt_handle)
        p.pack_bytes(blr)
        p.pack_int(0)
        p.pack_int(400)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_fetch_response(self, stmt_handle, xsqlda):
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        if bytes_to_bint(b) != self.op_fetch_response:
            raise InternalError
        b = recv_channel(self.sock, 8)
        status = bytes_to_bint(b[:4])
        count = bytes_to_bint(b[4:8])
        rows = []
        while count:
            r = [None] * len(xsqlda)
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if x.io_length() < 0:
                    b = recv_channel(self.sock, 4)
                    ln = bytes_to_bint(b)
                else:
                    ln = x.io_length()
                raw_value = recv_channel(self.sock, ln, True)
                if recv_channel(self.sock, 4) == bs([0]) * 4: # Not NULL
                    r[i] = x.value(raw_value)
            rows.append(r)
            b = recv_channel(self.sock, 12)
            op = bytes_to_bint(b[:4])
            status = bytes_to_bint(b[4:8])
            count = bytes_to_bint(b[8:])
        return rows, status != 100

    @wire_operation
    def _op_detach(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_detach)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_open_blob(self, blob_id):
        p = xdrlib.Packer()
        p.pack_int(self.op_open_blob)
        p.pack_int(self.trans_handle)
        send_channel(self.sock, p.get_buffer() + blob_id)

    @wire_operation
    def _op_create_blob2(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_create_blob2)
        p.pack_int(0)
        p.pack_int(self.trans_handle)
        p.pack_int(0)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_get_segment(self, blob_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_get_segment)
        p.pack_int(blob_handle)
        p.pack_int(self.buffer_length)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_batch_segments(self, blob_handle, seg_data):
        ln = len(seg_data)
        p = xdrlib.Packer()
        p.pack_int(self.op_batch_segments)
        p.pack_int(blob_handle)
        p.pack_int(ln + 2)
        p.pack_int(ln + 2)
        pad_length = ((4-(ln+2)) & 3)
        send_channel(self.sock, p.get_buffer() 
                + int_to_bytes(ln, 2) + seg_data + bs([0])*pad_length)

    @wire_operation
    def _op_close_blob(self, blob_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_close_blob)
        p.pack_int(blob_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_response(self):
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        if bytes_to_bint(b) != self.op_response:
            raise InternalError
        b = recv_channel(self.sock, 16)
        h = bytes_to_bint(b[0:4])         # Object handle
        oid = b[4:12]                       # Object ID
        buf_len = bytes_to_bint(b[12:])   # buffer length
        buf = recv_channel(self.sock, buf_len, True)

        # Parse status vector
        sql_code = 0
        gds_codes = set()
        message = ''
        n = bytes_to_bint(recv_channel(self.sock, 4))
        while n != isc_arg_end:
            if n == isc_arg_gds:
                gds_code = bytes_to_bint(recv_channel(self.sock, 4))
                if gds_code:
                    gds_codes.add(gds_code)
                    message += messages.get(gds_code, '@1')
                    num_arg = 0
            elif n == isc_arg_number:
                num = bytes_to_bint(recv_channel(self.sock, 4))
                if gds_code == 335544436:
                    sql_code = num
                num_arg += 1
                message = message.replace('@' + str(num_arg), str(num))
            elif n == isc_arg_string or n == isc_arg_interpreted:
                nbytes = bytes_to_bint(recv_channel(self.sock, 4))
                n = str(recv_channel(self.sock, nbytes, True))
                num_arg += 1
                message = message.replace('@' + str(num_arg), n)
            n = bytes_to_bint(recv_channel(self.sock, 4))

        if sql_code or message:
            raise OperationalError(message, gds_codes, sql_code)

        return (h, oid, buf)

    def cursor(self):
        if self.main_transaction is None:
            self.begin()
        c = Cursor(self.main_transaction)
        return c

    def begin(self):
        if not hasattr(self, "db_handle"):
            raise InternalError
        trans = Transaction(self)
        trans.begin()

    @property
    def transactions(self):
        return self._transactions

    @property
    def main_transaction(self):
        if len(self._transactions):
            return self._transactions[0]
        return None
    
    def commit(self, retaining=False):
        self.main_transaction.commit(retaining=retaining)

    def rollback(self, retaining=False):
        self.main_transaction.rollback(retaining=retaining)

    def __init__(self, dsn=None, user=None, password=None, host=None,
                    database=None, charset=DEFAULT_CHARSET, port=3050, 
                    page_size=None,
                    is_services = False):
        if dsn:
            i = dsn.find(':')
            if i < 0:
                self.hostname = host
                self.filename = dsn
            else:
                hostport = dsn[:i]
                self.filename = dsn[i+1:]
                i = hostport.find('/')
                if i < 0:
                    self.hostname = hostport
                    self.port = port
                else:
                    self.hostname = hostport[:i]
                    self.port = int(hostport[i+1:])
        else:
            self.hostname = host
            self.filename = database
            self.port = port
        self.user = user
        self.password = password
        self.charset = charset
        self._transactions = []
        self.isolation_level = ISOLATION_LEVEL_READ_COMMITED

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.hostname, self.port))

        self.page_size = page_size
        self.is_services = is_services
        self._op_connect()
        self._op_accept()
        if self.page_size:                      # create database
            self._op_create(self.page_size)
        elif self.is_services:                  # service api
            self._op_service_attach()
        else:                                   # connect
            self._op_attach()
        (h, oid, buf) = self._op_response()
        self.db_handle = h

    def set_isolation_level(self, isolation_level):
        self.isolation_level = isolation_level

    def _db_info(self, info_requests):
        if info_requests[-1] == isc_info_end:
            self._op_info_database(bs(info_requests))
        else:
            self._op_info_database(
                        bs(info_requests+type(info_requests)([isc_info_end])))
        (h, oid, buf) = self._op_response()
        i = 0
        i_request = 0
        r = []
        while i < len(buf):
            req = bi(buf[i])
            if req == isc_info_end:
                break
            assert req == info_requests[i_request]
            if req == isc_info_user_names:
                user_names = []
                while req == isc_info_user_names:
                    l = bytes_to_int(buf[i+1:i+3])
                    user_names.append(buf[i+3:i+3+l])
                    i = i + 3 + l
                    req = bi(buf[i])
                r.append(user_names)
            else:
                l = bytes_to_int(buf[i+1:i+3])
                r.append(buf[i+3:i+3+l])
                i = i + 3 + l
            i_request += 1
        return r

    def _db_info_convert_type(self, info_request, v):
        REQ_INT = [
            isc_info_allocation, isc_info_no_reserve, isc_info_db_sql_dialect,
            isc_info_ods_minor_version, isc_info_ods_version, 
            isc_info_page_size, isc_info_current_memory, isc_info_forced_writes,
            isc_info_max_memory, isc_info_num_buffers, isc_info_sweep_interval,
            isc_info_limbo, isc_info_attachment_id, isc_info_fetches, 
            isc_info_marks, isc_info_reads, isc_info_writes, 
            isc_info_set_page_buffers, isc_info_db_read_only,
            isc_info_db_size_in_pages, isc_info_page_errors, 
            isc_info_record_errors, isc_info_bpage_errors, 
            isc_info_dpage_errors, isc_info_ipage_errors,
            isc_info_ppage_errors, isc_info_tpage_errors,
        ]
        REQ_COUNT = [
            isc_info_backout_count, isc_info_delete_count, 
            isc_info_expunge_count, isc_info_insert_count, isc_info_purge_count,
            isc_info_read_idx_count, isc_info_read_seq_count, 
            isc_info_update_count
        ]

        if info_request in (isc_info_base_level, ):
            # IB6 API guide p52
            return bi(v[1])
        elif info_request in (isc_info_db_id, ):
            # IB6 API guide p52
            conn_code = bi(v[0])
            len1 = bi(v[1])
            filename = self.bytes_to_str(v[2:2+len1])
            len2 = bi(v[2+len1])
            sitename = self.bytes_to_str(v[3+len1:3+len1+len2])
            return (conn_code, filename, sitename)
        elif info_request in (isc_info_implementation, ):
            return (bi(v[1]), bi(v[2]))
        elif info_request in (isc_info_version, isc_info_firebird_version):
            # IB6 API guide p53
            return self.bytes_to_str(v[2:2+bi(v[1])])
        elif info_request in (isc_info_user_names, ):
            # IB6 API guide p54
            user_names = []
            for u in v:
                user_names.append(self.bytes_to_str(u[1:]))
            return user_names
        elif info_request in REQ_INT:
            return bytes_to_int(v)
        elif info_request in REQ_COUNT:
            counts = {}
            i = 0
            while i < len(v):
                counts[bytes_to_int(v[i:i+2])] = bytes_to_int(v[i+2:i+6])
                i += 6
            return counts
        else:
            return v

    def db_info(self, info_requests):
        if type(info_requests) == int:  # singleton
            r = self._db_info([info_requests])
            return self._db_info_convert_type(info_requests, r[0])
        else:
            results = {}
            rs = self._db_info(info_requests)
            for i in range(len(info_requests)):
                results[info_requests[i]] =  self._db_info_convert_type(
                                                    info_requests[i], rs[i])
            return results

    def close(self):
        if not hasattr(self, "db_handle"):
            return
        for trans in self._transactions:
            trans.close()
        if self.is_services:
            self._op_service_detach()
        else:
            self._op_detach()
        (h, oid, buf) = self._op_response()
        delattr(self, "db_handle")


    def __del__(self):
        if hasattr(self, "db_handle"):
            self.close()

class Transaction:
    def __init__(self, connection, tpb=None):
        self._connection = connection

    def begin(self):
        self.close()
        self.connection._op_transaction(
                transaction_parameter_block[self.connection.isolation_level])
        (h, oid, buf) = self.connection._op_response()
        self.trans_handle = h
        self.connection._transactions.append(self)

    def commit(self, retaining=False):
        if retaining:
            self.connection._op_commit_retaining(self.trans_handle)
            (h, oid, buf) = self.connection._op_response()
        else:
            self.connection._op_commit(self.trans_handle)
            (h, oid, buf) = self.connection._op_response()
            delattr(self, "trans_handle")
            self.connection._transactions.remove(self)

    def rollback(self, retaining=False):
        if retaining:
            self.connection._op_rollback_retaining(self.trans_handle)
            (h, oid, buf) = self.connection._op_response()
        else:
            self.connection._op_rollback()
            (h, oid, buf) = self.connection._op_response(self.trans_handle)
            delattr(self, "trans_handle")
            self.connection._transactions.remove(self)

    def close(self):
        if self.closed:
            return
        self.connection._op_rollback(self.trans_handle)
        (h, oid, buf) = self.connection._op_response()
        delattr(self, "trans_handle")
        self.connection._transactions.remove(self)

    @property
    def connection(self):
        return self._connection

    @property
    def closed(self):
        return not hasattr(self, "trans_handle")

