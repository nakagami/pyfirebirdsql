##############################################################################
# Copyright (c) 2009,2010 Hajime Nakagami<nakagami@gmail.com>
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

DEFAULT_CHARSET='UTF8'
PYTHON_MAJOR_VER = sys.version_info[0]

def bs(byte_array):
    if PYTHON_MAJOR_VER==3:
        return bytes(byte_array)
    return ''.join([chr(c) for c in byte_array])

DEBUG = False
__version__ = '0.3.3'
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

ISOLATION_LEVEL_READ_UNCOMMITTED = 0
ISOLATION_LEVEL_READ_COMMITED = 1
ISOLATION_LEVEL_REPEATABLE_READ = 2
ISOLATION_LEVEL_SERIALIZABLE = 3

isc_tpb_version1 = 1
isc_tpb_version3 = 3
isc_tpb_consistency = 1
isc_tpb_concurrency = 2
isc_tpb_shared = 3
isc_tpb_protected = 4
isc_tpb_exclusive = 5
isc_tpb_wait = 6
isc_tpb_nowait = 7
isc_tpb_read = 8
isc_tpb_write = 9
isc_tpb_lock_read = 10
isc_tpb_lock_write = 11
isc_tpb_verb_time = 12
isc_tpb_commit_time = 13
isc_tpb_ignore_limbo = 14
isc_tpb_read_committed = 15
isc_tpb_autocommit = 16
isc_tpb_rec_version = 17
isc_tpb_no_rec_version = 18
isc_tpb_restart_requests = 19
isc_tpb_no_auto_undo = 20
isc_tpb_lock_timeout = 21

transaction_parameter_block = [
    # ISOLATION_LEVEL_READ_UNCOMMITTED
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
    # ISOLATION_LEVEL_READ_COMMITED
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
    # ISOLATION_LEVEL_REPEATABLE_READ
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_concurrency]),
    # ISOLATION_LEVEL_SERIALIZABLE
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_consistency]),
]

isc_info_sql_names = [
  None, 'isc_info_end', 'isc_info_truncated', 'isc_info_error', 
  'isc_info_sql_select', 'isc_info_sql_bind',
  'isc_info_sql_num_variables', 'isc_info_sql_describe_vars',
  'isc_info_sql_describe_end', 'isc_info_sql_sqlda_seq',
  'isc_info_sql_message_seq', 'isc_info_sql_type', 'isc_info_sql_sub_type',
  'isc_info_sql_scale', 'isc_info_sql_length', 'isc_info_sql_null_ind',
  'isc_info_sql_field', 'isc_info_sql_relation', 'isc_info_sql_owner',
  'isc_info_sql_alias', 'isc_info_sql_sqlda_start', 'isc_info_sql_stmt_type',
  'isc_info_sql_get_plan', 'isc_info_sql_records', 'isc_info_sql_batch_fetch',
]

isc_status_names = [
  'isc_arg_end', 'isc_arg_gds', 'isc_arg_string', 'isc_arg_cstring',
  'isc_arg_number', 'isc_arg_interpreted', 'isc_arg_vms', 'isc_arg_unix',
  'isc_arg_domain', 'isc_arg_dos', 'isc_arg_mpexl', 'isc_arg_mpexl_ipc',
  None, None, None, 
  'isc_arg_next_mach', 'isc_arg_netware', 'isc_arg_win32', 'isc_arg_warning',
]

isc_info_names = [
  None, 'isc_info_end', 'isc_info_truncated', 'isc_info_error', 
  'isc_info_db_id', 'isc_info_reads',
  'isc_info_writes', 'isc_info_fetches', 'isc_info_marks', None, None,
  'isc_info_implementation', 'isc_info_isc_version', 'isc_info_base_level',
  'isc_info_page_size', 'isc_info_num_buffers', 'isc_info_limbo',
  'isc_info_current_memory', 'isc_info_max_memory', 'isc_info_window_turns',
  'isc_info_license', 'isc_info_allocation', 'isc_info_attachment_id',
  'isc_info_read_seq_count', 'isc_info_read_idx_count',
  'isc_info_insert_count', 'isc_info_update_count', 'isc_info_delete_count',
  'isc_info_backout_count', 'isc_info_purge_count', 'isc_info_expunge_count',
  'isc_info_sweep_interval', 'isc_info_ods_version',
  'isc_info_ods_minor_version', 'isc_info_no_reserve', 'isc_info_logfile',
  'isc_info_cur_logfile_name', 'isc_info_cur_log_part_offset',
  'isc_info_num_wal_buffers', 'isc_info_wal_buffer_size',
  'isc_info_wal_ckpt_length', 'isc_info_wal_cur_ckpt_interval',
  'isc_info_wal_prv_ckpt_fname', 'isc_info_wal_prv_ckpt_poffset',
  'isc_info_wal_recv_ckpt_fname', 'isc_info_wal_recv_ckpt_poffset', None,
  'isc_info_wal_grpc_wait_usecs', 'isc_info_wal_num_io',
  'isc_info_wal_avg_io_size', 'isc_info_wal_num_commits',
  'isc_info_wal_avg_grpc_size', 'isc_info_forced_writes',
  'isc_info_user_names', 'isc_info_page_errors',
  'isc_info_record_errors', 'isc_info_bpage_errors',
  'isc_info_dpage_errors', 'isc_info_ipage_errors',
  'isc_info_ppage_errors', 'isc_info_tpage_errors',
  'isc_info_set_page_buffers', 'isc_info_db_sql_dialect',
  'isc_info_db_read_only', 'isc_info_db_size_in_pages',
]
for i in range(len(isc_info_names), 101): # 65-100 no use
    isc_info_names.append(None)
isc_info_names += [
  'frb_info_att_charset', 'isc_info_db_class', 'isc_info_firebird_version',
  'isc_info_oldest_transaction', 'isc_info_oldest_active',
  'isc_info_oldest_snapshot', 'isc_info_next_transaction',
  'isc_info_db_provider', 'isc_info_active_transactions',
]

SQL_TYPE_TEXT = 452
SQL_TYPE_VARYING = 448
SQL_TYPE_SHORT = 500
SQL_TYPE_LONG = 496
SQL_TYPE_FLOAT = 482
SQL_TYPE_DOUBLE = 480
SQL_TYPE_D_FLOAT = 530
SQL_TYPE_TIMESTAMP = 510
SQL_TYPE_BLOB = 520
SQL_TYPE_ARRAY = 540
SQL_TYPE_QUAD = 550
SQL_TYPE_TIME = 560
SQL_TYPE_DATE = 570
SQL_TYPE_INT64 = 580

INFO_SQL_STMT_TYPE = bs([0x15])
INFO_SQL_SQLDA_START = bs([0x14,0x02])
INFO_SQL_SELECT_DESCRIBE_VARS = bs([0x04,0x07,0x09,0x0b,0x0c,0x0d,0x0e,0x0f,0x10,0x11,0x12,0x13,0x08])

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
        self.sqlname = ''
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
                + str(self.null_ok) + ',' \
                + self.sqlname + ',' + self.relname + ',' + self.ownname + ',' \
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
        if self.sqltype in (SQL_TYPE_TEXT, SQL_TYPE_VARYING):
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



class cursor:
    def _parse_select_items(self, buf):
        index = 0
        i = 0
        if PYTHON_MAJOR_VER==3:
            item = isc_info_sql_names[buf[i]]
        else:
            item = isc_info_sql_names[ord(buf[i])]
        while item != 'isc_info_end':
            if item == 'isc_info_sql_sqlda_seq':
                l = bytes_to_int(buf[i+1:i+3])
                index = bytes_to_int(buf[i+3:i+3+l])
                self._xsqlda[index-1] = XSQLVAR(self.connection.bytes_to_str)
                i = i + 3 + l
            elif item == 'isc_info_sql_type':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].sqltype = \
                                        bytes_to_int(buf[i+3:i+3+l]) & ~ 1
                i = i + 3 + l
            elif item == 'isc_info_sql_sub_type':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].sqlsubtype = bytes_to_int(buf[i+3:i+3+l])
                i = i + 3 + l
            elif item == 'isc_info_sql_scale':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].sqlscale = bytes_to_int(buf[i+3:i+3+l])
                i = i + 3 + l
            elif item == 'isc_info_sql_length':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].sqllen = bytes_to_int(buf[i+3:i+3+l])
                i = i + 3 + l
            elif item == 'isc_info_sql_null_ind':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].null_ok = bytes_to_int(buf[i+3:i+3+l])
                i = i + 3 + l
            elif item == 'isc_info_sql_field':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].sqlname = \
                        self.connection.bytes_to_str(buf[i + 3: i + 3 + l])
                i = i + 3 + l
            elif item == 'isc_info_sql_relation':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].relname = \
                        self.connection.bytes_to_str(buf[i + 3: i + 3 + l])
                i = i + 3 + l
            elif item == 'isc_info_sql_owner':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].ownname = \
                        self.connection.bytes_to_str(buf[i + 3: i + 3 + l])
                i = i + 3 + l
            elif item == 'isc_info_sql_alias':
                l = bytes_to_int(buf[i+1:i+3])
                self._xsqlda[index-1].aliasname = \
                        self.connection.bytes_to_str(buf[i + 3: i + 3 + l])
                i = i + 3 + l
            elif item == 'isc_info_truncated':
                return index    # return next index
            elif item == 'isc_info_sql_describe_end':
                i = i + 1
            else:
                print('\t', item, 'Invalid item [%02x] ! i=%d' % (buf[i], i))
                i = i + 1
            if PYTHON_MAJOR_VER==3:
                item = isc_info_sql_names[buf[i]]
            else:
                item = isc_info_sql_names[ord(buf[i])]
        return -1   # no more info

    def __init__(self, conn):
        if not hasattr(conn, "db_handle"):
            raise InternalError()
        self.connection = conn
        if not hasattr(self.connection, "trans_handle"):
            self.connection.begin()
        self.connection._op_allocate_statement()
        (h, oid, buf) = self.connection._op_response()
        self.stmt_handle = h
        self.arraysize = 1

    def callproc(self, procname, *params):
        raise NotSupportedError()

    def execute(self, query, params = []):
        self.connection._op_prepare_statement(self.stmt_handle, query)
        (h, oid, buf) = self.connection._op_response()

        for i in range(len(params)):    # Convert bytes parameter to blob id
            if PYTHON_MAJOR_VER==3 and type(params[i]) != bytes:
                continue
            elif PYTHON_MAJOR_VER==2 and type(params[i]) != str:
                continue
            self.connection._op_create_blob2()
            (blob_handle, blob_id, buf2) = self.connection._op_response()
            seg_size = self.connection.buffer_length
            (seg, remains) = params[i][:seg_size], params[i][seg_size:]
            while seg:
                self.connection._op_batch_segments(blob_handle, seg)
                (h3, oid3, buf3) = self.connection._op_response()
                (seg, remains) = remains[:seg_size], remains[seg_size:]
            self.connection._op_close_blob(blob_handle)
            (h4, oid4, buf4) = self.connection._op_response()
            assert blob_id == oid4
            params[i] = blob_id

        assert buf[:3] == bs([0x15,0x04,0x00]) # isc_info_sql_stmt_type (4 bytes)
        stmt_type = bytes_to_int(buf[3:7])
        if stmt_type == 1:  # isc_info_sql_stmt_select
            assert buf[7:9] == bs([0x04,0x07])
            l = bytes_to_int(buf[9:11])
            col_len = bytes_to_int(buf[11:11+l])
            self._xsqlda = [None] * col_len
            next_index = self._parse_select_items(buf[11+l:])
            while next_index > 0:   # more describe vars
                self.connection._op_info_sql(self.stmt_handle,
                            INFO_SQL_SQLDA_START + int_to_bytes(next_index, 2) 
                            + INFO_SQL_SELECT_DESCRIBE_VARS)
                (h, oid, buf) = self.connection._op_response()
                assert buf[:2] == bs([0x04,0x07])
                l = bytes_to_int(buf[2:4])
                assert bytes_to_int(buf[4:4+l]) == col_len
                next_index = self._parse_select_items(buf[4+l:])

            self.connection._op_execute(self.stmt_handle, params)
            (h, oid, buf) = self.connection._op_response()

            # Fetch
            self.rows = []
            more_data = True
            while more_data:
                self.connection._op_fetch(
                                    self.stmt_handle, calc_blr(self._xsqlda))
                (rows, more_data) = self.connection._op_fetch_response(
                                                self.stmt_handle, self._xsqlda)
                self.rows += rows

            # Convert BLOB handle to data
            for i in range(len(self._xsqlda)):
                x = self._xsqlda[i]
                if x.sqltype == SQL_TYPE_BLOB:
                    for r in self.rows:
                        if not r[i]:
                            continue
                        self.connection._op_open_blob(r[i])
                        (h, oid, buf) = self.connection._op_response()
                        v = bs([])
                        n = 1   # 1:mora data 2:no more data
                        while n == 1:
                            self.connection._op_get_segment(h)
                            (n, oid, buf) = self.connection._op_response()
                            while buf:
                                ln = bytes_to_int(buf[:2])
                                v += buf[2:ln+2]
                                buf = buf[ln+2:]
                        self.connection._op_close_blob(h)
                        (h, oid, buf) = self.connection._op_response()
                        r[i] = v
            self.cur_row = 0
        else:
            self.connection._op_execute(self.stmt_handle, params)
            try:
                (h, oid, buf) = self.connection._op_response()
            except OperationalError as o:
                if 335544665 in o.gds_codes:
                    raise IntegrityError(o.message, o.gds_codes, o.sql_code)
            self.rows = None

    def executemany(self, query, seq_of_params):
        for params in seq_of_params:
            self.execute(query, params)
            
    def fetchone(self):
        return self.rows[self.cur_row]
        self.cur_row += 1

    def fetchall(self):
        return self.rows

    def fetchmany(self, size=None):
        if not size:
            size = self.arraysize
        rows = self.rows[self.cur_row:self.cur_row+size]
        self.cur_row += size
        if self.cur_row > len(self.rows):
            self.cur_row = len(self.rows)

    def close(self):
        if not hasattr(self, "stmt_handle"):
            return
        self.connection._op_free_statement(self.stmt_handle, 2)   # DSQL_drop
        (h, oid, buf) = self.connection._op_response()
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
                r.append((x.sqlname, x.sqltype, None, x.io_length(), None, 
                        x.sqlscale, True if x.null_ok else False))
            return r
        elif attrname == 'rowcount':
            if self.rows:
                return len(self.rows)
            else:
                return -1
        raise AttributeError

    def __del__(self):
        if hasattr(self, "stmt_handle"):
            self.close()

class BaseConnect:
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
        'UNICODE_FSS' : 'utf-8',
    }

    def str_to_bytes(self, s):
        return s.encode(self.charset_map.get(self.charset, self.charset))

    def bytes_to_str(self, b):
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
            if t == str:
                v = self.str_to_bytes(p)
                nbytes = len(v)
                pad_length = ((4-nbytes) & 3)
                v += bs([0]) * pad_length
                blr += bs([14, nbytes & 255, nbytes >> 8])
            elif t == bytes:
                v = p
                blr += bytes([9, 0])
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
        p.pack_string(self.str_to_bytes(self.filename))
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
        dpb += bs([28, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bs([29, len(s)]) + s
        dpb += bs([0x3a,0x04,0x78,0x0a,0x00,0x00])  # isc_dpb_dummy_packet_interval
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
        p.pack_int(self.svc_handle)
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
    def _op_commit(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit)
        p.pack_int(self.trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_rollback(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback)
        p.pack_int(self.trans_handle)
        send_channel(self.sock, p.get_buffer())


    @wire_operation
    def _op_allocate_statement(self):
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
    def _op_prepare_statement(self, stmt_handle, query):
        p = xdrlib.Packer()
        p.pack_int(self.op_prepare_statement)
        p.pack_int(self.trans_handle)
        p.pack_int(stmt_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_string(self.str_to_bytes(query))
        p.pack_bytes(INFO_SQL_STMT_TYPE + INFO_SQL_SELECT_DESCRIBE_VARS)
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
    def _op_execute(self, stmt_handle, params):
        p = xdrlib.Packer()
        p.pack_int(self.op_execute)
        p.pack_int(stmt_handle)
        p.pack_int(self.trans_handle)

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
        s = isc_status_names[bytes_to_bint(recv_channel(self.sock, 4))] 
        while s != 'isc_arg_end':
            if s == 'isc_arg_gds':
                gds_code = bytes_to_bint(recv_channel(self.sock, 4))
                if gds_code:
                    gds_codes.add(gds_code)
                    message += messages.get(gds_code, '@1')
                    num_arg = 0
            elif s == 'isc_arg_number':
                num = bytes_to_bint(recv_channel(self.sock, 4))
                if gds_code == 335544436:
                    sql_code = num
                num_arg += 1
                message = message.replace('@' + str(num_arg), str(num))
            elif s == 'isc_arg_string' or s == 'isc_arg_interpreted':
                nbytes = bytes_to_bint(recv_channel(self.sock, 4))
                s = str(recv_channel(self.sock, nbytes, True))
                num_arg += 1
                message = message.replace('@' + str(num_arg), s)
            s = isc_status_names[bytes_to_bint(recv_channel(self.sock, 4))] 

        if sql_code or message:
            raise OperationalError(message, gds_codes, sql_code)

        return (h, oid, buf)

    def cursor(self):
        c = cursor(self)
        self.cursor_set.add(c)
        return c

    def begin(self):
        if not hasattr(self, "db_handle"):
            raise InternalError
        self._op_transaction(transaction_parameter_block[self.isolation_level])
        (h, oid, buf) = self._op_response()
        self.trans_handle = h
    
    def commit(self):
        self._op_commit()
        (h, oid, buf) = self._op_response()
        self.begin()

    def rollback(self):
        self._op_rollback()
        (h, oid, buf) = self._op_response()
        self.begin()

    def close(self):
        if not hasattr(self, "db_handle"):
            return
        for c in self.cursor_set:
            c.close()
        self._op_rollback()
        (h, oid, buf) = self._op_response()
        self._op_detach()
        (h, oid, buf) = self._op_response()
        delattr(self, "db_handle")

    def __init__(self, dsn, user, password, charset=DEFAULT_CHARSET, port=3050):
        i = dsn.find(':')
        if i < 0:
            self.hostname = ''
            self.filename = dsn
        else:
            self.hostname = dsn[:i]
            self.filename = dsn[i+1:]
        self.user = user
        self.password = password
        self.charset = charset
        self.port = port
        self.cursor_set = set()
        self.isolation_level = ISOLATION_LEVEL_READ_COMMITED

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.hostname, self.port))

    def set_isolation_level(self, isolation_level):
        self.isolation_level = isolation_level

    def info_database(self, info_names):
        if info_names[-1] != 'isc_info_end':
            info_names.append('isc_info_end')
        b = bs([isc_info_names.index(s)  for s in info_names])
        self._op_info_database(b)
        (h, oid, buf) = self._op_response()
        i = 0
        r = []
        while i < len(buf):
            if PYTHON_MAJOR_VER==3:
                s = isc_info_names[buf[i]]
            else:
                s = isc_info_names[ord(buf[i])]
            if s == 'isc_info_end':
                break
            l = bytes_to_int(buf[i+1:i+3])
            r.append(buf[i+3:i+3+l])
            i = i + 3 + l
        return r

    def __del__(self):
        if hasattr(self, "db_handle"):
            self.close()

class connect(BaseConnect):
    def __init__(self, dsn, user, password, charset=DEFAULT_CHARSET , port=3050):
        BaseConnect.__init__(self, dsn, user, password, charset, port)
        self._op_connect()
        self._op_accept()
        self._op_attach()
        (h, oid, buf) = self._op_response()
        self.db_handle = h


class create_database(BaseConnect):
    def __init__(self, dsn, user, password, charset=DEFAULT_CHARSET , port=3050,
                                                            page_size = 4096):
        self.page_size = page_size
        BaseConnect.__init__(self, dsn, user, password, charset, port)
        self._op_connect()
        self._op_accept()
        self._op_create(self.page_size)
        (h, oid, buf) = self._op_response()
        self.db_handle = h

class service_mgr(BaseConnect):
    def __init__(self, dsn, user, password, charset=DEFAULT_CHARSET , port=3050):
        BaseConnect.__init__(self, dsn, user, password, charset, port)
        self._op_connect()
        self._op_accept()
        self._op_service_attach()
        (h, oid, buf) = self._op_response()
        self.db_handle = h

    def backup_database(self, backup_filename, f=None):
        spb = bs([1])
        s = self.str_to_bytes(self.filename)
        spb += bs([0x6a]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(backup_filename)
        spb += bs([0x05]) + int_to_bytes(len(s), 2) + s
        if f:
            spb += bs([0x6b])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            (f if f else sys.stdout).write(self.bytes_to_str(buf[3:3+ln]))

    def restore_database(self, restore_filename, f=None):
        spb = bs([2])
        s = self.str_to_bytes(restore_filename)
        spb += bs([0x05]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(self.filename)
        spb += bs([0x6a]) + int_to_bytes(len(s), 2) + s
        if f:
            spb += bs([0x6b])
        spb += bs([0x09,0x00,0x08,0x00,0x00,0x0a,0x00,0x10,0x00,0x00,0x6c,0x00,0x30,0x00,0x00])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            (f if f else sys.stdout).write(self.bytes_to_str(buf[3:3+ln]))

    def close(self):
        if not hasattr(self, "db_handle"):
            return
        self._op_service_detach()
        (h, oid, buf) = self._op_response()
        delattr(self, "db_handle")

    def __del__(self):
        if hasattr(self, "db_handle"):
            self.close()
