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
from __future__ import print_function
import sys
import time
import datetime
import decimal
import itertools
from collections import Mapping
from firebirdsql import InternalError, OperationalError, IntegrityError, NotSupportedError
from firebirdsql.consts import *
from firebirdsql.utils import *
from firebirdsql.wireprotocol import WireProtocol
from firebirdsql.socketstream import SocketStream
from firebirdsql.xsqlvar import calc_blr, parse_xsqlda
__version__ = '1.1.1'
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

DEBUG = False


def DEBUG_OUTPUT(*argv):
    if not DEBUG:
        return
    for s in argv:
        print(s, end=' ', file=sys.stderr)
    print(file=sys.stderr)


transaction_parameter_block = (
    # ISOLATION_LEVEL_READ_COMMITED_LEGACY
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
    # ISOLATION_LEVEL_READ_COMMITED
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
    # ISOLATION_LEVEL_REPEATABLE_READ
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_concurrency]),
    # ISOLATION_LEVEL_SERIALIZABLE
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_consistency]),
    # ISOLATION_LEVEL_READ_COMMITED_RO
    bs([isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
)

Date = datetime.date
Time = datetime.time
TimeDelta = datetime.timedelta
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    return apply(Date, time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return apply(Time, time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return apply(Timestamp, time.localtime(ticks)[:6])


def Binary(b):
    return bytes(b)


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


STRING = DBAPITypeObject(str)
if PYTHON_MAJOR_VER == 3:
    BINARY = DBAPITypeObject(bytes)
else:
    BINARY = DBAPITypeObject(str)
NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
DATE = DBAPITypeObject(datetime.date)
TIME = DBAPITypeObject(datetime.time)
ROWID = DBAPITypeObject()


class Statement(object):
    """
    statement handle and status (open/close)
    """
    def __init__(self, trans):
        DEBUG_OUTPUT("Statement::__init__()")
        self.trans = trans
        self._allocate_stmt()
        self._is_open = False
        self.stmt_type = None

    def _allocate_stmt(self):
        self.trans.connection._op_allocate_statement()
        if self.trans.connection.accept_type == ptype_lazy_send:
            self.trans.connection.lazy_response_count += 1
            self.handle = -1
        else:
            (h, oid, buf) = self.trans.connection._op_response()
            self.handle = h

    def prepare(self, sql, explain_plan=False):
        DEBUG_OUTPUT("Statement::prepare()", self.handle)
        if explain_plan:
            self.trans.connection._op_prepare_statement(
                self.handle, self.trans.trans_handle, sql,
                option_items=bs([isc_info_sql_get_plan]))
        else:
            self.trans.connection._op_prepare_statement(
                self.handle, self.trans.trans_handle, sql)
            self.plan = None

        if self.trans.connection.lazy_response_count:
            self.trans.connection.lazy_response_count -= 1
            (h, oid, buf) = self.trans.connection._op_response()
            self.handle = h

        (h, oid, buf) = self.trans.connection._op_response()

        i = 0
        if byte_to_int(buf[i]) == isc_info_sql_get_plan:
            l = bytes_to_int(buf[i+1:i+3])
            self.plan = self.trans.connection.bytes_to_str(buf[i+3:i+3+l])
            i += 3 + l
        self.stmt_type, self.xsqlda = parse_xsqlda(buf[i:], self.trans.connection, self.handle)
        if self.stmt_type == isc_info_sql_stmt_select:
            self._is_open = True

    def close(self):
        DEBUG_OUTPUT("Statement::close()", self.handle)
        if (self.stmt_type == isc_info_sql_stmt_select and self._is_open):
            self.trans.connection._op_free_statement(self.handle, DSQL_close)
            if self.trans.connection.accept_type == ptype_lazy_send:
                self.trans.connection.lazy_response_count += 1
            else:
                (h, oid, buf) = self.trans.connection._op_response()
        self._is_open = False

    def drop(self):
        DEBUG_OUTPUT("Statement::drop()", self.handle)
        if self.handle != -1:
            self.trans.connection._op_free_statement(self.handle, DSQL_drop)
            if self.trans.connection.accept_type == ptype_lazy_send:
                self.trans.connection.lazy_response_count += 1
            else:
                (h, oid, buf) = self.trans.connection._op_response()
        self._is_open = False
        self.handle = -1

    @property
    def is_opened(self):
        return self._is_open and self.handle != -1


class PreparedStatement(object):
    def __init__(self, cur, sql, explain_plan=False):
        DEBUG_OUTPUT("PreparedStatement::__init__()")
        cur.transaction.check_trans_handle()
        self.stmt = Statement(cur.transaction)
        self.stmt.prepare(sql, explain_plan)
        self.sql = sql

    def __getattr__(self, attrname):
        if attrname == 'description':
            if len(self.stmt.xsqlda) == 0:
                return None
            r = []
            for x in self.stmt.xsqlda:
                r.append((
                    x.aliasname, x.sqltype, x.display_length(),
                    x.io_length(), x.precision(),
                    x.sqlscale, True if x.null_ok else False))
            return r
        elif attrname == 'n_output_params':
            return len(self.stmt.xsqlda)
        raise AttributeError


def _fetch_generator(stmt):
    DEBUG_OUTPUT("_fetch_generator()", stmt.handle, stmt.trans._trans_handle)
    connection = stmt.trans.connection
    more_data = True
    while more_data:
        if not stmt.is_opened:
            return
        connection._op_fetch(stmt.handle, calc_blr(stmt.xsqlda))
        (rows, more_data) = connection._op_fetch_response(stmt.handle, stmt.xsqlda)
        for r in rows:
            # Convert BLOB handle to data
            for i in range(len(stmt.xsqlda)):
                x = stmt.xsqlda[i]
                if x.sqltype == SQL_TYPE_BLOB:
                    if not r[i]:
                        continue
                    connection._op_open_blob(r[i], stmt.trans.trans_handle)
                    (h, oid, buf) = connection._op_response()
                    v = bs([])
                    n = 1   # 0,1:mora data 2:no more data
                    while n != 2:
                        connection._op_get_segment(h)
                        (n, oid, buf) = connection._op_response()
                        while buf:
                            ln = bytes_to_int(buf[:2])
                            v += buf[2:ln+2]
                            buf = buf[ln+2:]
                    connection._op_close_blob(h)
                    if connection.accept_type == ptype_lazy_send:
                        connection.lazy_response_count += 1
                    else:
                        (h, oid, buf) = connection._op_response()
                    r[i] = v
                    if x.sqlsubtype == 1:    # TEXT
                        if connection.use_unicode:
                            r[i] = connection.bytes_to_ustr(r[i])
                        else:
                            r[i] = connection.bytes_to_str(r[i])
            yield tuple(r)
    return


class Cursor(object):
    def __init__(self, trans):
        DEBUG_OUTPUT("Cursor::__init__()")
        self._transaction = trans
        self.stmt = None
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
            if type(param) == str or (PYTHON_MAJOR_VER == 2 and type(param) == unicode):
                param = self.transaction.connection.str_to_bytes(param)
            cooked_params.append(param)
        return cooked_params

    def _get_stmt(self, query):
        self.query = query
        if isinstance(query, PreparedStatement):
            stmt = query.stmt
        else:
            if self.stmt:
                self.stmt.drop()
                self.stmt = None
            if self.stmt is None:
                self.stmt = Statement(self.transaction)
            stmt = self.stmt
            stmt.prepare(query)
        return stmt

    def prep(self, query, explain_plan=False):
        DEBUG_OUTPUT("Cursor::prep()")
        prepared_statement = PreparedStatement(self, query, explain_plan=explain_plan)
        return prepared_statement

    def execute(self, query, params=None):
        if params is None:
            params = []
        DEBUG_OUTPUT("Cursor::execute()", query, params)
        self.transaction.check_trans_handle()
        stmt = self._get_stmt(query)
        cooked_params = self._convert_params(params)
        if stmt.stmt_type == isc_info_sql_stmt_exec_procedure:
            self.transaction.connection._op_execute2(
                stmt.handle,
                self.transaction.trans_handle, cooked_params,
                calc_blr(stmt.xsqlda))
            self._callproc_result = self.transaction.connection._op_sql_response(stmt.xsqlda)
            self.transaction.connection._op_response()
            self._fetch_records = None
        else:
            DEBUG_OUTPUT(
                "Cursor::execute() _op_execute()",
                stmt.handle, self.transaction.trans_handle)
            self.transaction.connection._op_execute(
                stmt.handle, self.transaction.trans_handle, cooked_params)
            (h, oid, buf) = self.transaction.connection._op_response()

            if stmt.stmt_type == isc_info_sql_stmt_select:
                self._fetch_records = _fetch_generator(stmt)
            else:
                self._fetch_records = None
            self._callproc_result = None
        self.transaction.is_dirty = True

        return self

    def callproc(self, procname, params=None):
        if params is None:
            params = []
        DEBUG_OUTPUT("Cursor::callproc()")
        query = 'EXECUTE PROCEDURE ' + procname + ' ' + ','.join('?'*len(params))
        self.execute(query, params)
        return self._callproc_result

    def executemany(self, query, seq_of_params):
        for params in seq_of_params:
            self.execute(query, params)

    def fetchone(self):
        if not self.transaction.is_dirty:
            return None
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
                return tuple(next(self._fetch_records))
            else:
                return tuple(self._fetch_records.next())
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
        if not self.transaction.is_dirty:
            return None
        if not self._fetch_records:
            if self._callproc_result:
                proc_r = [tuple(self._callproc_result)]
                self._callproc_result = None
                return proc_r
            return []
        # select statement
        return [tuple(r) for r in self._fetch_records]

    def fetchmany(self, size=None):
        if not size:
            size = self.arraysize
        # callproc or not select statement
        if not self._fetch_records:
            if self._callproc_result:
                r = [self._callproc_result]
                self._callproc_result = None
                return r
            return []
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
        DEBUG_OUTPUT("Cursor::close()")
        if not self.stmt:
            return
        self.stmt.drop()
        self.stmt = None

    def nextset(self):
        raise NotSupportedError()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column):
        pass

    @property
    def description(self):
        if not self.stmt:
            return None
        return [(
            x.aliasname, x.sqltype, x.display_length(), x.io_length(),
            x.precision(), x.sqlscale, True if x.null_ok else False
        ) for x in self.stmt.xsqlda]

    @property
    def rowcount(self):
        DEBUG_OUTPUT("Cursor::rowcount()")
        if self.stmt.handle == -1:
            return -1

        self.transaction.connection._op_info_sql(self.stmt.handle, bs([isc_info_sql_records]))
        (h, oid, buf) = self.transaction.connection._op_response()
        assert buf[:3] == bs([0x17, 0x1d, 0x00])    # isc_info_sql_records
        if self.stmt.stmt_type == isc_info_sql_stmt_select:
            assert buf[17:20] == bs([0x0d, 0x04, 0x00])     # isc_info_req_select_count
            count = bytes_to_int(buf[20:24])
        else:
            count = bytes_to_int(buf[27:31]) + bytes_to_int(buf[6:10]) + bytes_to_int(buf[13:17])
        DEBUG_OUTPUT("Cursor::rowcount()", self.stmt.stmt_type, count)
        return count


class EventConduit(WireProtocol):
    def __init__(self, conn, names, timeout):
        self.sock = None
        self.connection = conn
        self.event_names = {}
        for name in names:
            self.event_names[name] = 0
        self.timeout = timeout
        self.connection._op_connect_request()
        (h, oid, buf) = self.connection._op_response()
        family = buf[:2]
        port = bytes_to_bint(buf[2:4], u=True)
        if family == b'\x02\x00':     # IPv4
            ip_address = '.'.join([str(byte_to_int(c)) for c in buf[4:8]])
        elif family == b'\x0a\x00':  # IPv6
            address = bytes_to_hex(buf[8:24])
            if not isinstance(address, str):    # Py3
                address = address.decode('ascii')
            ip_address = ':'.join(
                [address[i: i+4] for i in range(0, len(address), 4)]
            )
        self.sock = SocketStream(ip_address, port, timeout)
        self.connection.last_event_id += 1
        self.event_id = self.connection.last_event_id

        self.connection._op_que_events(self.event_names, self.event_id)
        (h, oid, buf) = self.connection._op_response()

        (event_id, event_names) = self._wait_for_event(timeout=timeout)
        assert event_id == self.event_id   # treat only one event_id
        self.event_names.update(event_names)

    def wait(self, timeout=None):
        self.connection._op_que_events(self.event_names, self.event_id)
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
        self.sock = None


class Connection(WireProtocol):
    def cursor(self, factory=Cursor):
        DEBUG_OUTPUT("Connection::cursor()")
        if self._transaction is None:
            self.begin()
        return factory(self._transaction)

    def begin(self):
        DEBUG_OUTPUT("Connection::begin()")
        if not self.sock:
            raise InternalError
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
        self._transaction.begin()

    def commit(self, retaining=False):
        DEBUG_OUTPUT("Connection::commit()")
        if self._transaction:
            self._transaction.commit(retaining=retaining)

    def savepoint(self, name):
        return self._transaction.savepoint(name)

    def rollback(self, retaining=False, savepoint=None):
        DEBUG_OUTPUT("Connection::rollback()")
        if self._transaction:
            self._transaction.rollback(retaining=retaining, savepoint=savepoint)

    def execute_immediate(self, query):
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
            self._transaction.begin()
        self._transaction.check_trans_handle()
        self._op_exec_immediate(
            self._transaction.trans_handle, query=query)
        (h, oid, buf) = self._op_response()
        self._transaction.is_dirty = True

    def __init__(
        self, dsn=None, user=None, password=None, role=None, host=None,
        database=None, charset=DEFAULT_CHARSET, port=3050,
        page_size=4096, is_services=False, cloexec=False,
        timeout=None, isolation_level=None, use_unicode=None,
        auth_plugin_name=None, wire_crypt=True, create_new=False
    ):
        DEBUG_OUTPUT("Connection::__init__()")
        if auth_plugin_name is None:
            auth_plugin_name = 'Srp'
        WireProtocol.__init__(self)
        self.sock = None
        self.db_handle = None
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
        self.auth_plugin_name = auth_plugin_name
        self.wire_crypt = wire_crypt
        self.page_size = page_size
        self.is_services = is_services
        if isolation_level is None:
            self.isolation_level = ISOLATION_LEVEL_READ_COMMITED
        else:
            self.isolation_level = int(isolation_level)
        self.use_unicode = use_unicode
        self.last_event_id = 0

        self._autocommit = False
        self._transaction = None
        self.sock = SocketStream(self.hostname, self.port, self.timeout, cloexec)

        self._op_connect(auth_plugin_name, wire_crypt)
        try:
            self._op_accept()
        except OperationalError as e:
            self.sock.close()
            self.sock = None
            raise e
        if create_new:                      # create database
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

    def set_autocommit(self, is_autocommit):
        if self._autocommit != is_autocommit and self._transaction is not None:
            self.rollback()
            self._transaction = None
        self._autocommit = is_autocommit

    def _db_info(self, info_requests):
        if info_requests[-1] == isc_info_end:
            self._op_info_database(bs(info_requests))
        else:
            self._op_info_database(bs(info_requests+type(info_requests)([isc_info_end])))
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
            yyyy = 100 * century + nday
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
        DEBUG_OUTPUT("Connection::db_info()")
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
                    results[info_requests[i]] = self._db_info_convert_type(info_requests[i], rs[i][1])
            return results

    def trans_info(self, info_requests):
        if self._transaction:
            return self._transaction.trans_info(info_requests)
        return {}

    def close(self):
        DEBUG_OUTPUT("Connection::close()")
        if self.sock is None:
            return
        if self.db_handle:
            if self.is_services:
                self._op_service_detach()
            else:
                self._op_detach()
            (h, oid, buf) = self._op_response()
        self.sock.close()
        self.sock = None
        self.db_handle = None

    def drop_database(self):
        DEBUG_OUTPUT("Connection::drop_database()")
        self._op_drop_database()
        (h, oid, buf) = self._op_response()
        self.sock.close()
        self.sock = None
        self.db_handle = None

    def event_conduit(self, event_names, timeout=None):
        return EventConduit(self, event_names, timeout)

    def __del__(self):
        if self.sock:
            self.close()

    def is_disconnect(self):
        return self.sock is None


class Transaction(object):
    def __init__(self, connection, is_autocommit=False):
        DEBUG_OUTPUT("Transaction::__init__()")
        self._connection = connection
        self._trans_handle = None
        self._autocommit = is_autocommit

    def _begin(self):
        tpb = transaction_parameter_block[self.connection.isolation_level]
        if self._autocommit:
            tpb += bs([isc_tpb_autocommit])
        self.connection._op_transaction(tpb)
        (h, oid, buf) = self.connection._op_response()
        self._trans_handle = None if h < 0 else h
        DEBUG_OUTPUT(
            "Transaction::_begin()", self._trans_handle, self.connection)
        self.is_dirty = False

    def begin(self):
        DEBUG_OUTPUT("Transaction::begin()")
        self._begin()

    def savepoint(self, name):
        if self._trans_handle is None:
            return
        self.connection._op_exec_immediate(self._trans_handle, query='SAVEPOINT '+name)
        (h, oid, buf) = self.connection._op_response()

    def commit(self, retaining=False):
        DEBUG_OUTPUT(
            "Transaction::commit()", self._trans_handle, self, self.connection, retaining)
        if self._trans_handle is None:
            return
        if not self.is_dirty:
            return
        if retaining:
            self.connection._op_commit_retaining(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
        else:
            self.connection._op_commit(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
            self._trans_handle = None
        self.is_dirty = False

    def rollback(self, retaining=False, savepoint=None):
        DEBUG_OUTPUT(
            "Transaction::rollback()", self._trans_handle, self,
            self.connection, retaining, savepoint)
        if self._trans_handle is None:
            return
        if savepoint:
            self.connection._op_exec_immediate(
                self._trans_handle, query='ROLLBACK TO '+savepoint)
            (h, oid, buf) = self.connection._op_response()
            return
        if not self.is_dirty:
            return
        if retaining:
            self.connection._op_rollback_retaining(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
        else:
            self.connection._op_rollback(self._trans_handle)
            (h, oid, buf) = self.connection._op_response()
            self._trans_handle = None
        self.is_dirty = False

    def _trans_info(self, info_requests):
        if info_requests[-1] == isc_info_end:
            self.connection._op_info_transaction(self.trans_handle, bs(info_requests))
        else:
            self.connection._op_info_transaction(
                self.trans_handle, bs(info_requests+type(info_requests)([isc_info_end])))
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

    def check_trans_handle(self):
        if self._trans_handle is None:
            self._begin()

    @property
    def connection(self):
        return self._connection

    @property
    def trans_handle(self):
        assert(self._trans_handle is not None)
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
