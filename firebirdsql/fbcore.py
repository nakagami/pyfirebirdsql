##############################################################################
# Copyright (c) 2009-2013 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird.
##############################################################################
import sys, os, socket
import xdrlib, time, datetime, decimal, struct
import itertools
from firebirdsql.fberrmsgs import messages
from firebirdsql import (DatabaseError, InternalError, OperationalError,
    ProgrammingError, IntegrityError, DataError, NotSupportedError,)
from firebirdsql.consts import *
from firebirdsql.wireprotocol import (WireProtocol,
    bytes_to_bint, bytes_to_int, bint_to_bytes, int_to_bytes, byte_to_int,
    INFO_SQL_SELECT_DESCRIBE_VARS,)

try:
    from collections import Mapping
    HAS_MAPPING = True
except ImportError:
    # Python 2.5
    from UserDict import DictMixin as Mapping
    HAS_MAPPING = False

try:
    import fcntl
except ImportError:
    def setcloexec(sock):
        pass
else:
    def setcloexec(sock):
        """Set FD_CLOEXEC property on a file descriptors
        """
        fd = sock.fileno()
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


__version__ = '0.8.0'
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

transaction_parameter_block = (
    # ISOLATION_LEVEL_READ_UNCOMMITTED
    bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
    # ISOLATION_LEVEL_READ_COMMITED
    bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
    # ISOLATION_LEVEL_REPEATABLE_READ
    bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_concurrency]),
    # ISOLATION_LEVEL_SERIALIZABLE
    bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_consistency]),
    # ISOLATION_LEVEL_READ_COMMITED_READ_ONLY
    bytes([isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
)

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

#------------------------------------------------------------------------------
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
        return None

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
            if self.sqlsubtype == 1:    # TEXT
                return self.bytes_to_str(raw_value)
            else:                       # BINARY
                return raw_value
        elif self.sqltype == SQL_TYPE_VARYING:
            if self.sqlsubtype == 1:    # TEXT
                return self.bytes_to_str(raw_value)
            else:                       # BINARY
                return raw_value
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
    }

def calc_blr(xsqlda):
    "Calculate  BLR from XSQLVAR array."
    ln = len(xsqlda) *2
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
        else:
            blr += sqltype2blr[sqltype]
        blr += [7, 0]   # [blr_short, 0]
    blr += [255, 76]    # [blr_end, blr_eoc]

    # x.sqlscale value shoud be negative, so b convert to range(0, 256)
    return bytes(256 + b if b < 0 else b for b in blr)

def parse_select_items(buf, xsqlda, connection):
    index = 0
    i = 0
    item = byte_to_int(buf[i])
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
        item = byte_to_int(buf[i])
    return -1   # no more info

def parse_xsqlda(buf, connection, stmt_handle):
    xsqlda = []
    stmt_type = None
    i = 0
    while i < len(buf):
        if buf[i:i+3] == bytes([isc_info_sql_stmt_type,0x04,0x00]):
            stmt_type = bytes_to_int(buf[i+3:i+7])
            i += 7
        elif buf[i:i+2] == bytes([isc_info_sql_select, isc_info_sql_describe_vars]):
            i += 2
            l = bytes_to_int(buf[i:i+2])
            i += 2
            col_len = bytes_to_int(buf[i:i+l])
            xsqlda = [None] * col_len
            next_index = parse_select_items(buf[i+l:], xsqlda, connection)
            while next_index > 0:   # more describe vars
                connection._op_info_sql(stmt_handle,
                            bytes([isc_info_sql_sqlda_start, 2])
                                + int_to_bytes(next_index, 2)
                                + INFO_SQL_SELECT_DESCRIBE_VARS)
                (h, oid, buf) = connection._op_response()
                assert buf[:2] == bytes([0x04,0x07])
                l = bytes_to_int(buf[2:4])
                assert bytes_to_int(buf[4:4+l]) == col_len
                next_index = parse_select_items(buf[4+l:], xsqlda, connection)
        else:
            break
    return stmt_type, xsqlda

class PreparedStatement:
    def __init__(self, cur, sql, explain_plan=False):
        self.cur = cur
        self.sql = sql
        transaction = self.cur.transaction
        connection = transaction.connection

        connection._op_allocate_statement()
        (h, oid, buf) = connection._op_response()
        self.stmt_handle = h
        self.is_opened = False

        if explain_plan:
            connection._op_prepare_statement(
                self.stmt_handle, transaction.trans_handle, sql,
                option_items=bytes([isc_info_sql_get_plan]))
        else:
            connection._op_prepare_statement(
                self.stmt_handle, transaction.trans_handle, sql)
            self.plan = None

        (h, oid, buf) = connection._op_response()

        i = 0
        if byte_to_int(buf[i]) == isc_info_sql_get_plan:
            l = bytes_to_int(buf[i+1:i+3])
            self.plan = connection.bytes_to_str(buf[i+3:i+3+l])
            i += 3 + l

        self.statement_type, self._xsqlda = parse_xsqlda(
                                        buf[i:], connection, self.stmt_handle)

    def __getattr__(self, attrname):
        if attrname == 'description':
            if len(self._xsqlda) == 0:
                return None
            r = []
            for x in self._xsqlda:
                r.append((x.aliasname, x.sqltype, x.display_length(),
                        x.io_length(), x.precision(),
                        x.sqlscale, True if x.null_ok else False))
            return r
        elif attrname == 'n_output_params':
            return len(self._xsqlda)
        raise AttributeError

class Cursor:
    def __init__(self, trans):
        self._transaction = trans
        self.stmt_handle = None
        self.arraysize = 1

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    @property
    def transaction(self):
        return self._transaction

    def _convert_params(self, params):
        cooked_params = []
        for param in params:
            if (type(param)==str or
                (PYTHON_MAJOR_VER == 2 and type(param)==unicode)):
                param = self.transaction.connection.str_to_bytes(param)
            cooked_params.append(param)
        return cooked_params

    def _get_stmt_handle(self, query):
        if isinstance(query, PreparedStatement):
            if query.is_opened:
                self.transaction.connection._op_free_statement(
                                            query.stmt_handle, 1) # DSQL_close
                (h, oid, buf) = self.transaction.connection._op_response()
            stmt_handle = query.stmt_handle
            stmt_type = query.statement_type
            self._xsqlda = query._xsqlda
            query.is_opened = True
        else:
            if self.stmt_handle:
                self.transaction.connection._op_free_statement(
                                            self.stmt_handle, 2) # DSQL_drop
                (h, oid, buf) = self.transaction.connection._op_response()
            self.transaction.connection._op_allocate_statement()
            (stmt_handle, oid, buf) = self.transaction.connection._op_response()
            self.stmt_handle = stmt_handle
            self.transaction.connection._op_prepare_statement(stmt_handle,
                                        self.transaction.trans_handle, query)
            (h, oid, buf) = self.transaction.connection._op_response()
            stmt_type, self._xsqlda = parse_xsqlda(
                                buf, self.transaction.connection, stmt_handle)
        return stmt_type, stmt_handle

    def _execute(self, stmt_handle, params):
        cooked_params = self._convert_params(params)
        self.transaction.connection._op_execute(stmt_handle,
                                self.transaction.trans_handle, cooked_params)
        try:
            (h, oid, buf) = self.transaction.connection._op_response()
        except OperationalError:
            e = sys.exc_info()[1]
            if 335544665 in e.gds_codes:
                raise IntegrityError(e._message, e.gds_codes, e.sql_code)
            raise OperationalError(e._message, e.gds_codes, e.sql_code)

    def _callproc(self, stmt_handle, query, params):
        cooked_params = self._convert_params(params)
        self.transaction.connection._op_prepare_statement(stmt_handle,
                                        self.transaction.trans_handle, query)
        (h, oid, buf) = self.transaction.connection._op_response()
        stmt_type, self._xsqlda = parse_xsqlda(
                            buf, self.transaction.connection, stmt_handle)

        self.transaction.connection._op_execute2(stmt_handle,
            self.transaction.trans_handle, cooked_params,
            calc_blr(self._xsqlda))
        return self.transaction.connection._op_sql_response(self._xsqlda)

    def prep(self, query, explain_plan=False):
        prepared_statement = PreparedStatement(self, query,
                                                explain_plan=explain_plan)
        return prepared_statement

    def execute(self, query, params=[]):
        stmt_type, stmt_handle = self._get_stmt_handle(query)
        if stmt_type == stmt_type == isc_info_sql_stmt_exec_procedure:
            cooked_params = self._convert_params(params)
            self.transaction.connection._op_execute2(stmt_handle,
                self.transaction.trans_handle, cooked_params,
                calc_blr(self._xsqlda))
            self._callproc_result = \
                self.transaction.connection._op_sql_response(self._xsqlda)
            self._fetch_records = None
        else:
            self._execute(stmt_handle, params)
            if stmt_type == isc_info_sql_stmt_select:
                self._fetch_records = self._fetch_generator(stmt_handle)
            else:
                self._fetch_records = None

    def callproc(self, procname, params=[]):
        self.transaction.connection._op_allocate_statement()
        (stmt_handle, oid, buf) = self.transaction.connection._op_response()
        query = 'EXECUTE PROCEDURE ' + procname + ' ' + ','.join('?'*len(params))
        self._callproc_result = self._callproc(stmt_handle, query, params)
        self._fetch_records = None
        self.transaction.connection._op_free_statement(stmt_handle, 2) # DSQL_drop
        (h, oid, buf) = self.transaction.connection._op_response()

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
                        connection._op_open_blob(r[i], self.transaction.trans_handle)
                        (h, oid, buf) = connection._op_response()
                        v = bytes([])
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

        raise StopIteration()

    def fetchone(self):
        # callproc or not select statement
        if not self._fetch_records:
            if self._callproc_result:
                r = self._callproc_result
                self._callproc_result = None
                return r
            return None
        # select statement
        try:
            if PYTHON_MAJOR_VER == 3:
                return next(self._fetch_records)
            else:
                return self._fetch_records.next()
        except StopIteration:
            return None

    def __iter__(self):
        return self

    def __next__(self):
        r = self.fetchone()
        if not r:
            raise StopIteration()
        return r

    def next(self):
        return self.__next__()

    def fetchall(self):
        # callproc or not select statement
        if not self._fetch_records:
            if self._callproc_result:
                r = [self._callproc_result]
                self._callproc_result = None
                return r
            return None
        # select statement
        return list(self._fetch_records)

    def fetchmany(self, size=None):
        if not size:
            size = self.arraysize
        # callproc or not select statement
        if not self._fetch_records:
            if self._callproc_result:
                r = [self._callproc_result]
                self._callproc_result = None
                return r
            return None
        # select statement
        return list(itertools.islice(self._fetch_records, size))

    # kinterbasdb extended API
    def fetchonemap(self):
        r = self.fetchone()
        if r is None:
            return {}
        return RowMapping(r, self.description)

    def fetchallmap(self):
        desc = self.description
        return [RowMapping(row, desc) for row in self.fetchall()]

    def fetchmanymap(self, size=None):
        desc = self.description
        return [RowMapping(row, desc) for row in self.fetchmany(size)]

    def itermap(self):
        r = self.fetchonemap()
        while r:
            yield r
            r = self.fetchonemap()

    def close(self):
        if not self.stmt_handle:
            return
        self.transaction.connection._op_free_statement(self.stmt_handle, 2)   # DSQL_drop
        (h, oid, buf) = self.transaction.connection._op_response()
        self.stmt_handle = None

    def nextset(self):
        raise NotSupportedError()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column):
        pass

    @property
    def description(self):
        if not hasattr(self, "_xsqlda"):
            return None
        return [(x.aliasname, x.sqltype, x.display_length(), x.io_length(),
                    x.precision(), x.sqlscale, True if x.null_ok else False)
                for x in self._xsqlda]

    @property
    def rowcount(self):
        self.transaction.connection._op_info_sql(self.stmt_handle,
                                     bytes([isc_info_sql_stmt_type]))
        (h, oid, buf) = self.transaction.connection._op_response()
        assert buf[:3] == bytes([0x15,0x04,0x00]) # isc_info_sql_stmt_type
        stmt_type = bytes_to_int(buf[3:7])

        self.transaction.connection._op_info_sql(self.stmt_handle,
                                     bytes([isc_info_sql_records]))
        (h, oid, buf) = self.transaction.connection._op_response()
        assert buf[:3] == bytes([0x17,0x1d,0x00]) # isc_info_sql_records
        if stmt_type == isc_info_sql_stmt_select:
            # rowcount for select stmt changes based on num rows fetched
            count = -1
        elif stmt_type == isc_info_sql_stmt_insert:
            assert buf[24:27] == bytes([0x0e,0x04,0x00])
                                              # isc_info_req_insert_count
            count = bytes_to_int(buf[27:31])
        elif stmt_type == isc_info_sql_stmt_update:
            assert buf[3:6] == bytes([0x0f,0x04,0x00])
                                              # isc_info_req_update_count
            count = bytes_to_int(buf[6:10])
        elif stmt_type == isc_info_sql_stmt_delete:
            assert buf[10:13] == bytes([0x10,0x04,0x00])
                                              # isc_info_req_delete_count
            count = bytes_to_int(buf[13:17])
        return count

class EventConduit(WireProtocol):
    def __init__(self, conn, names, timeout):
        self.connection = conn
        self.event_names = {}
        for name in names:
            self.event_names[name] = 0
        self.timeout = timeout
        (h, port, family, ip_address) = self.connection._op_connect_request()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((ip_address, port))
        self.connection.last_event_id += 1
        self.event_id = self.connection.last_event_id

        self.connection._op_que_events(self.event_names, 0, 0, self.event_id)
        (h, oid, buf) = self.connection._op_response()

        (event_id, event_names) = self._wait_for_event(timeout=timeout)
        assert event_id == self.event_id   # treat only one event_id
        self.event_names.update(event_names)

    def wait(self, timeout=None):
        self.connection._op_que_events(self.event_names, 0, 0, self.event_id)
        (h, oid, buf) = self.connection._op_response()

        r = self._wait_for_event(timeout=timeout)
        if r:
            (event_id, event_names) = r
            assert event_id == self.event_id   # treat only one event_id
            r = {}
            for k in event_names:
                r[k] = event_names[k]-self.event_names[k]
                self.event_names[k] = event_names[k]
        else:
            r = {}
            for k in self.event_names:
                r[k] = 0
        return r

    def close(self):
        self.connection._op_cancel_events(self.event_id)
        (h, oid, buf) = self.connection._op_response()
        self.sock.close()

class Connection(WireProtocol):
    def uid(self):
        if sys.platform == 'win32':
            user = os.environ['USERNAME']
            hostname = os.environ['COMPUTERNAME']
        else:
            user = os.environ.get('USER', '')
            hostname = socket.gethostname()
        return bytes([1] + [len(user)] + [ord(c) for c in user]
                + [4] + [len(hostname)] + [ord(c) for c in hostname] + [6, 0])

    def cursor(self):
        if self._transaction is None:
            self.begin()
        return Cursor(self._transaction)

    def begin(self):
        if not self.sock:
            raise InternalError
        if self._transaction is None:
            self._transaction = Transaction(self)
        self._transaction.begin()

    def commit(self, retaining=False):
        if self._transaction:
            self._transaction.commit(retaining=retaining)

    def savepoint(self, name):
        return self._transaction.savepoint(name)

    def rollback(self, retaining=False, savepoint=None):
        if self._transaction:
            self._transaction.rollback(retaining=retaining, savepoint=savepoint)

    def execute_immediate(self, query):
        self._op_exec_immediate(
            self._transaction.trans_handle, query=query)
        (h, oid, buf) = self._op_response()

    def __init__(self, dsn=None, user=None, password=None, role=None, host=None,
                    database=None, charset=DEFAULT_CHARSET, port=3050,
                    page_size=None, is_services=False, cloexec=False,
                    timeout=None, isolation_level=None):
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
                else:
                    self.hostname = hostport[:i]
                    port = int(hostport[i+1:])
        else:
            self.hostname = host
            self.filename = database
        if self.hostname is None:
            self.hostname = 'localhost'
        self.port = port
        self.user = user
        self.password = password
        self.role = role
        self.charset = charset
        self.timeout = float(timeout) if timeout is not None else None
        self.page_size = page_size
        self.is_services = is_services
        if isolation_level is None:
            self.isolation_level = ISOLATION_LEVEL_READ_COMMITED
        else:
            self.isolation_level = int(isolation_level)
        self.last_event_id = 0

        self._transaction = None

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if cloexec:
            setcloexec(self.sock)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.hostname, self.port))

        self._op_connect()
        try:
            self._op_accept()
        except OperationalError:
            self.sock.close()
            self.sock = None
            e = sys.exc_info()[1]
            raise e
        if self.page_size:                      # create database
            self._op_create(self.page_size)
        elif self.is_services:                  # service api
            self._op_service_attach()
        else:                                   # connect
            self._op_attach()
        (h, oid, buf) = self._op_response()
        self.db_handle = h

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def set_isolation_level(self, isolation_level):
        self.isolation_level = int(isolation_level)

    def _db_info(self, info_requests):
        if info_requests[-1] == isc_info_end:
            self._op_info_database(bytes(info_requests))
        else:
            self._op_info_database(
                        bytes(info_requests+type(info_requests)([isc_info_end])))
        (h, oid, buf) = self._op_response()
        i = 0
        i_request = 0
        r = []
        while i < len(buf):
            req = byte_to_int(buf[i])
            if req == isc_info_end:
                break
            assert req == info_requests[i_request] or req == isc_info_error
            if req == isc_info_user_names:
                user_names = []
                while req == isc_info_user_names:
                    l = bytes_to_int(buf[i+1:i+3])
                    user_names.append(buf[i+3:i+3+l])
                    i = i + 3 + l
                    req = byte_to_int(buf[i])
                r.append((req, user_names))
            else:
                l = bytes_to_int(buf[i+1:i+3])
                r.append((req, buf[i+3:i+3+l]))
                i = i + 3 + l
            i_request += 1
        return r

    def _db_info_convert_type(self, info_request, v):
        REQ_INT = set([
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
            # may not be available in some versions of Firebird
            isc_info_oldest_transaction, isc_info_oldest_active,
            isc_info_oldest_snapshot, isc_info_next_transaction,
            isc_info_active_tran_count
        ])
        REQ_COUNT = set([
            isc_info_backout_count, isc_info_delete_count,
            isc_info_expunge_count, isc_info_insert_count, isc_info_purge_count,
            isc_info_read_idx_count, isc_info_read_seq_count,
            isc_info_update_count
        ])

        if info_request in (isc_info_base_level, ):
            # IB6 API guide p52
            return byte_to_int(v[1])
        elif info_request in (isc_info_db_id, ):
            # IB6 API guide p52
            conn_code = byte_to_int(v[0])
            len1 = byte_to_int(v[1])
            filename = self.bytes_to_str(v[2:2+len1])
            len2 = byte_to_int(v[2+len1])
            sitename = self.bytes_to_str(v[3+len1:3+len1+len2])
            return (conn_code, filename, sitename)
        elif info_request in (isc_info_implementation, ):
            return (byte_to_int(v[1]), byte_to_int(v[2]))
        elif info_request in (isc_info_version, isc_info_firebird_version):
            # IB6 API guide p53
            return self.bytes_to_str(v[2:2+byte_to_int(v[1])])
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
        elif info_request in (isc_info_creation_date,):
            nday = bytes_to_int(v[:4]) + 2400001 - 1721119
            century = (4 * nday - 1) // 146097
            nday = 4 * nday - 1 - 146097 * century
            dd = nday // 4
            nday = (4 * dd + 3) // 1461
            dd = 4 * dd + 3 - 1461 * nday
            dd = (dd + 4) // 4
            mm = (5 * dd - 3) // 153
            dd = 5 * dd - 3 - 153 * mm
            dd = (dd + 5) // 5
            yyyy = 100 * century + nday;
            if mm < 10:
                mm += 3
            else:
                mm -= 9
                yyyy += 1

            ntime = bytes_to_int(v[4:])
            h = ntime // (3600 * ISC_TIME_SECONDS_PRECISION)
            ntime %= 3600 * ISC_TIME_SECONDS_PRECISION
            m = ntime // (60 * ISC_TIME_SECONDS_PRECISION)
            ntime %= 60 * ISC_TIME_SECONDS_PRECISION
            s = ntime // ISC_TIME_SECONDS_PRECISION
            ms = ntime % ISC_TIME_SECONDS_PRECISION * 100

            return datetime.datetime(yyyy, mm, dd, h, m, s, ms)
        else:
            return v

    def db_info(self, info_requests):
        if type(info_requests) == int:  # singleton
            r = self._db_info([info_requests])
            return self._db_info_convert_type(info_requests, r[0][1])
        else:
            results = {}
            rs = self._db_info(info_requests)
            for i in range(len(info_requests)):
                if rs[i][0] == isc_info_error:
                    results[info_requests[i]] = None
                else:
                    results[info_requests[i]] = self._db_info_convert_type(
                                                    info_requests[i], rs[i][1])
            return results

    def trans_info(self, info_requests):
        if self._transaction:
            return self._transaction.trans_info(info_requests)
        return {}

    def close(self):
        if self.sock is None:
            return
        if self._transaction:
            self._transaction.close()
        if self.is_services:
            self._op_service_detach()
        else:
            self._op_detach()
        (h, oid, buf) = self._op_response()
        self.sock.close()
        self.sock = None

    def drop_database(self):
        self._op_drop_database()
        (h, oid, buf) = self._op_response()
        self.sock.close()
        self.sock = None
        delattr(self, "db_handle")

    def event_conduit(self, event_names, timeout=None):
        return EventConduit(self, event_names, timeout)

    def __del__(self):
        if self.sock:
            self.close()

class Transaction:
    def __init__(self, connection, tpb=None):
        self._connection = connection
        self._trans_handle = None

    def _begin(self):
        self._close()
        self.connection._op_transaction(
                transaction_parameter_block[self.connection.isolation_level])
        (h, oid, buf) = self.connection._op_response()
        self._trans_handle = h

    def _close(self):
        if self._trans_handle:
            self.connection._op_rollback(self.trans_handle)
            (h, oid, buf) = self.connection._op_response()
        self._trans_handle = None

    def begin(self):
        self._begin()

    def commit(self, retaining=False):
        if self._trans_handle is None:
            return
        if retaining:
            self.connection._op_commit_retaining(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
        else:
            self.connection._op_commit(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
            self._trans_handle = None

    def savepoint(self, name):
        if self._trans_handle is None:
            return
        self.connection._op_exec_immediate(self._trans_handle,
                        query='SAVEPOINT '+name)
        (h, oid, buf) = self.connection._op_response()

    def rollback(self, retaining=False, savepoint=None):
        if self._trans_handle is None:
            return
        if savepoint:
            self.connection._op_exec_immediate(self._trans_handle,
                        query='ROLLBACK TO '+savepoint)
            (h, oid, buf) = self.connection._op_response()
            return

        if retaining:
            self.connection._op_rollback_retaining(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
        else:
            self.connection._op_rollback(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
            self._trans_handle = None

    def _trans_info(self, info_requests):
        if info_requests[-1] == isc_info_end:
            self.connection._op_info_transaction(self.trans_handle,
                    bytes(info_requests))
        else:
            self.connection._op_info_transaction(self.trans_handle,
                    bytes(info_requests+type(info_requests)([isc_info_end])))
        (h, oid, buf) = self.connection._op_response()
        i = 0
        i_request = 0
        r = []
        while i < len(buf):
            req = byte_to_int(buf[i])
            if req == isc_info_end:
                break
            assert req == info_requests[i_request] or req == isc_info_error
            l = bytes_to_int(buf[i+1:i+3])
            r.append((req, buf[i+3:i+3+l]))
            i = i + 3 + l

            i_request += 1
        return r

    def trans_info(self, info_requests):
        if type(info_requests) == int:  # singleton
            r = self._trans_info([info_requests])
            return {info_requests: r[1][0]}
        else:
            results = {}
            rs = self._trans_info(info_requests)
            for i in range(len(info_requests)):
                if rs[i][0] == isc_info_tra_isolation:
                    v = (byte_to_int(rs[i][1][0]), byte_to_int(rs[i][1][1]))
                elif rs[i][0] == isc_info_error:
                    v = None
                else:
                    v = bytes_to_int(rs[i][1])
                results[info_requests[i]] = v
            return results

    def close(self):
        self._close()

    @property
    def connection(self):
        return self._connection

    @property
    def trans_handle(self):
        if self._trans_handle is None:
            self._begin()
        return self._trans_handle


class RowMapping(Mapping):
    """dict like interface to result rows
    """
    __slots__ = ("_description", "_fields")

    def __init__(self, row, description):
        self._fields = fields = {}
        # result may contain multiple fields with the same name. The
        # RowMapping API ignores these additional fields.
        for i, descr in enumerate(description):
            fields.setdefault(descr[0], row[i])
        self._description = description

    def __getitem__(self, key):
        fields = self._fields
        # try unnormalized key first
        try:
            return fields[key]
        except KeyError:
            pass

        # normalize field name
        if key[0] == '"' and key[-1] == '"':
            # field names in quotes are case sensitive
            normkey = key[1:-1]
        else:
            # default is all upper case fields
            normkey = key.upper()

        try:
            return fields[normkey]
        except KeyError:
            raise KeyError("RowMapping has no field names '%s'. Available "
                           "field names are: %s" %
                           (key, ", ".join(self.keys())))

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __repr__(self):
        fields = self._fields
        values = ["%s=%r" % (desc[0], fields[desc[0]])
                  for desc in self._description]
        return ("<RowMapping at 0x%08x with fields: %s>" %
                (id(self), ", ".join(values)))

    if not HAS_MAPPING:
        def keys(self):
            return list(self)

        def __setitem__(self, key, value):
            raise NotImplementedError

        def __delitem__(self, key):
            raise NotImplementedError
