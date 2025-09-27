##############################################################################
# Copyright (c) 2009-2025, Hajime Nakagami<nakagami@gmail.com>
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
import datetime
import itertools
import hashlib
import select
from firebirdsql.fberrmsgs import messages
from firebirdsql.err import InternalError, OperationalError, NotSupportedError, IntegrityError, DataError
from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql.wireprotocol import WireProtocol, get_crypt
from firebirdsql.stream import SocketStream
from firebirdsql.xsqlvar import calc_blr, parse_xsqlda
from firebirdsql.event_conduit import EventConduit
from firebirdsql import srp
from firebirdsql.arc4 import ARC4
from firebirdsql.chacha import ChaCha20

def DEBUG_OUTPUT(*argv):
    if debug_level() == 0:
        return
    for s in argv:
        print(s, end=' ', file=sys.stderr)
    print(file=sys.stderr)


class Statement(object):
    """
    statement handle and status (open/close)
    """
    def __init__(self, trans):
        DEBUG_OUTPUT("Statement::__init__()")
        self.trans = trans

        self.trans.connection._op_allocate_statement()
        if (self.trans.connection.accept_type & ptype_MASK) == ptype_lazy_send:
            self.trans.connection.lazy_response_count += 1
            self.handle = -1
        else:
            (h, oid, buf) = self.trans.connection._op_response()
            self.handle = h

        self._is_open = False
        self.stmt_type = None

    def fetch_generator(self, rows, more_data):
        DEBUG_OUTPUT("Statement::_fetch_generator()", self.handle, self.trans._trans_handle, self.trans.connection.db_handle)
        connection = self.trans.connection
        while rows:
            for r in rows:
                # Convert BLOB handle to data
                for i in range(len(self.xsqlda)):
                    x = self.xsqlda[i]
                    if x.sqltype == SQL_TYPE_BLOB:
                        if not r[i]:
                            continue
                        connection._op_open_blob2(r[i], self.trans.trans_handle)
                        if (connection.accept_type & ptype_MASK)== ptype_lazy_send:
                            connection.lazy_response_count += 1
                            h = -1
                        else:
                            (h, oid, buf) = connection._op_response()
                        v = bytes([])
                        n = 1   # 0,1:mora data 2:no more data
                        while n != 2:
                            connection._op_get_segment(h)
                            (n, oid, buf) = connection._op_response()
                            while buf:
                                ln = bytes_to_int(buf[:2])
                                v += buf[2:ln+2]
                                buf = buf[ln+2:]
                        connection._op_close_blob(h)
                        if (connection.accept_type & ptype_MASK)== ptype_lazy_send:
                            connection.lazy_response_count += 1
                        else:
                            (h, oid, buf) = connection._op_response()
                        r[i] = v
                        if x.sqlsubtype == 1:    # TEXT
                            r[i] = connection.bytes_to_str(r[i])
                yield tuple(r)
            if more_data:
                connection._op_fetch(self.handle, calc_blr(self.xsqlda))
                (rows, more_data) = connection._op_fetch_response(self.handle, self.xsqlda)
            else:
                break

        return

    def prepare(self, sql, explain_plan=False):
        DEBUG_OUTPUT("Statement::prepare()", self.handle)
        if explain_plan:
            self.trans.connection._op_prepare_statement(
                self.handle, self.trans.trans_handle, sql,
                option_items=bytes([isc_info_sql_get_plan]))
        else:
            self.trans.connection._op_prepare_statement(
                self.handle, self.trans.trans_handle, sql)
            self.plan = None

        while self.trans.connection.lazy_response_count:
            self.trans.connection.lazy_response_count -= 1
            (h, oid, buf) = self.trans.connection._op_response()
            self.handle = h

        (h, oid, buf) = self.trans.connection._op_response()

        i = 0
        if buf[i] == isc_info_sql_get_plan:
            ln = bytes_to_int(buf[i+1:i+3])
            self.plan = self.trans.connection.bytes_to_str(buf[i+3:i+3+ln])
            i += 3 + ln
        self.stmt_type, self.xsqlda = parse_xsqlda(buf[i:], self.trans.connection, self.handle)
        if self.stmt_type == isc_info_sql_stmt_select:
            self._is_open = True

    def close(self):
        DEBUG_OUTPUT("Statement::close()", self.handle)
        if self.stmt_type == isc_info_sql_stmt_select and self._is_open:
            self.trans.connection._op_free_statement(self.handle, DSQL_close)
            if (self.trans.connection.accept_type & ptype_MASK)== ptype_lazy_send:
                self.trans.connection.lazy_response_count += 1
            else:
                (h, oid, buf) = self.trans.connection._op_response()
        self._is_open = False

    def drop(self):
        DEBUG_OUTPUT("Statement::drop()", self.handle)
        if self.handle != -1:
            self.trans.connection._op_free_statement(self.handle, DSQL_drop)
            if (self.trans.connection.accept_type & ptype_MASK) == ptype_lazy_send:
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

    def close(self):
        DEBUG_OUTPUT("PreparedStatement::close()")
        self.stmt.close()


class Cursor(object):
    def __init__(self, obj):
        DEBUG_OUTPUT("Cursor::__init__()")
        if isinstance(obj, Connection):
            self._transaction = obj._transaction
            conn = obj
        elif isinstance(obj, Transaction):
            self._transaction = obj
            conn = obj.connection
        else:
            raise NotSupportedError()
        if self._transaction not in conn._cursors:
            conn._cursors[self._transaction] = []
        conn._cursors[self._transaction].append(self)
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
            if isinstance(param, str):
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

    def _execute(self, query, params):
        if params is None:
            params = []
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
            self.transaction.connection._op_execute(
                stmt.handle, self.transaction.trans_handle, cooked_params)
            (h, oid, buf) = self.transaction.connection._op_response()

            if stmt.stmt_type == isc_info_sql_stmt_select:
                self.transaction.connection._op_fetch(stmt.handle, calc_blr(stmt.xsqlda))
                (rows, more_data) = self.transaction.connection._op_fetch_response(stmt.handle, stmt.xsqlda)
                self._fetch_records = stmt.fetch_generator(rows, more_data)
            else:
                self._fetch_records = None
            self._callproc_result = None

        return self

    def execute(self, query, params=None):
        DEBUG_OUTPUT("Cursor::execute()", query, params)
        try:
            return self._execute(query, params)
        finally:
            self.transaction.is_dirty = True

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
            DEBUG_OUTPUT("Cursor::fetchone() not dirty")
            return None
        # callproc or not select statement
        if not self._fetch_records:
            if self._callproc_result:
                r = self._callproc_result
                self._callproc_result = None
                DEBUG_OUTPUT("Cursor::fetchone()", r)
                return r
            return None
        # select statement
        try:
            result = tuple(next(self._fetch_records))
        except StopIteration:
            result = None
        DEBUG_OUTPUT("Cursor::fetchone()", result)
        return result

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
        results = [tuple(r) for r in self._fetch_records]
        DEBUG_OUTPUT("Cursor::fetchall()", results)
        return results

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
        results_list = list(itertools.islice(self._fetch_records, size))
        DEBUG_OUTPUT("Cursor::fetchmany()", results_list)
        return results_list

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

        self.transaction.connection._op_info_sql(self.stmt.handle, bytes([isc_info_sql_records]))
        (h, oid, buf) = self.transaction.connection._op_response()
        assert buf[:3] == bytes([0x17, 0x1d, 0x00])    # isc_info_sql_records
        if self.stmt.stmt_type == isc_info_sql_stmt_select:
            assert buf[17:20] == bytes([0x0d, 0x04, 0x00])     # isc_info_req_select_count
            # select count
            count = bytes_to_int(buf[20:24])
        else:
            # insert count + update count + delete count
            count = bytes_to_int(buf[27:31]) + bytes_to_int(buf[6:10]) + bytes_to_int(buf[13:17])
        return count


class Transaction(object):
    transaction_parameter_block = (
        # ISOLATION_LEVEL_READ_COMMITED_LEGACY
        bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_no_rec_version]),
        # ISOLATION_LEVEL_READ_COMMITED
        bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
        # ISOLATION_LEVEL_REPEATABLE_READ
        bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_concurrency]),
        # ISOLATION_LEVEL_SERIALIZABLE
        bytes([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_consistency]),
        # ISOLATION_LEVEL_READ_COMMITED_RO
        bytes([isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version]),
    )

    def __init__(self, connection, is_autocommit=False, isolation_level=None):
        DEBUG_OUTPUT("Transaction::__init__()")
        self._connection = connection
        self._trans_handle = None
        self._autocommit = is_autocommit
        self._isolation_level = isolation_level

    def _begin(self):
        isolation_level = self._isolation_level if self._isolation_level is not None else self.connection.isolation_level
        tpb = self.transaction_parameter_block[isolation_level]
        if self._autocommit:
            tpb += bytes([isc_tpb_autocommit])
        self.connection._op_transaction(tpb)
        (h, oid, buf) = self.connection._op_response()
        self._trans_handle = None if h < 0 else h
        DEBUG_OUTPUT(
            "Transaction::_begin()", self.connection.db_handle, isolation_level, self._autocommit, self._trans_handle)
        self.is_dirty = False

    def close(self):
        if self._trans_handle is None:
            return
        if not self.is_dirty:
            return
        DEBUG_OUTPUT("Transaction::close()", self._trans_handle, self.connection.db_handle)
        self.connection._op_rollback(self._trans_handle)
        (h, oid, buf) = self.connection._op_response()
        self._trans_handle = None
        self.is_dirty = False

    def begin(self):
        DEBUG_OUTPUT("Transaction::begin()")
        self._begin()

    def savepoint(self, name):
        DEBUG_OUTPUT("Transaction::savepoint()", name)
        if self._trans_handle is None:
            return
        self.connection._op_exec_immediate(self._trans_handle, query='SAVEPOINT '+name)
        (h, oid, buf) = self.connection._op_response()

    def commit(self, retaining=False):
        DEBUG_OUTPUT(
            "Transaction::commit()", self._trans_handle, self.connection.db_handle, retaining)
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
            "Transaction::rollback()", self._trans_handle,
            self.connection.db_handle, retaining, savepoint)
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
            self.connection._op_info_transaction(self.trans_handle, bytes(info_requests))
        else:
            self.connection._op_info_transaction(
                self.trans_handle, bytes(info_requests+type(info_requests)([isc_info_end])))
        (h, oid, buf) = self.connection._op_response()
        i = 0
        i_request = 0
        r = []
        while i < len(buf):
            req = buf[i]
            if req == isc_info_end:
                break
            assert req == info_requests[i_request] or req == isc_info_error
            ln = bytes_to_int(buf[i+1:i+3])
            r.append((req, buf[i+3:i+3+ln]))
            i = i + 3 + ln

            i_request += 1
        return r

    def trans_info(self, info_requests):
        if isinstance(info_requests, int):  # singleton
            r = self._trans_info([info_requests])
            return {info_requests: r[1][0]}
        else:
            results = {}
            rs = self._trans_info(info_requests)
            for i in range(len(info_requests)):
                if rs[i][0] == isc_info_tra_isolation:
                    v = (rs[i][1][0], rs[i][1][1])
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


class ConnectionResponseMixin:
    def _recv_channel(self, nbytes, word_alignment=False):
        n = nbytes
        if word_alignment and (n % 4):
            n += 4 - nbytes % 4  # 4 bytes word alignment
        r = bytes([])
        while n:
            if (self.timeout is not None and select.select([self.sock._sock], [], [], self.timeout)[0] == []):
                break
            b = self.sock.recv(n)
            if not b:
                break
            r += b
            n -= len(b)
        if len(r) < nbytes:
            raise OperationalError('Can not recv() packets')
        return r[:nbytes]

    def _parse_status_vector(self):
        sql_code = 0
        gds_codes = set()
        num_arg = 0
        message = ''
        n = bytes_to_bint(self._recv_channel(4))
        while n != isc_arg_end:
            if n == isc_arg_gds:
                gds_code = bytes_to_bint(self._recv_channel(4))
                if gds_code:
                    gds_codes.add(gds_code)
                    message += messages.get(gds_code, '@1')
                    num_arg = 0
            elif n == isc_arg_number:
                num = bytes_to_bint(self._recv_channel(4))
                if gds_code == 335544436:
                    sql_code = num
                num_arg += 1
                message = message.replace('@' + str(num_arg), str(num))
            elif n == isc_arg_string:
                nbytes = bytes_to_bint(self._recv_channel(4))
                s = self.bytes_to_str(self._recv_channel(nbytes, word_alignment=True))
                num_arg += 1
                message = message.replace('@' + str(num_arg), s)
            elif n == isc_arg_interpreted:
                nbytes = bytes_to_bint(self._recv_channel(4))
                s = str(self._recv_channel(nbytes, word_alignment=True))
                message += s
            elif n == isc_arg_sql_state:
                nbytes = bytes_to_bint(self._recv_channel(4))
                s = str(self._recv_channel(nbytes, word_alignment=True))
            n = bytes_to_bint(self._recv_channel(4))

        return (gds_codes, sql_code, message)

    def _parse_op_response(self):
        b = self._recv_channel(16)
        h = bytes_to_bint(b[0:4])         # Object handle
        oid = b[4:12]                       # Object ID
        buf_len = bytes_to_bint(b[12:])   # buffer length
        buf = self._recv_channel(buf_len, word_alignment=True)

        (gds_codes, sql_code, message) = self._parse_status_vector()
        if gds_codes.intersection([
            335544838, 335544879, 335544880, 335544466, 335544665, 335544347, 335544558
        ]):
            raise IntegrityError(message, gds_codes, sql_code)
        elif gds_codes.intersection([335544321]):
            raise DataError(message, gds_codes, sql_code)
        elif (sql_code or message) and not gds_codes.intersection([335544434]):
            raise OperationalError(message, gds_codes, sql_code)
        return (h, oid, buf)

    def _op_response(self):
        b = self._recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self._recv_channel(4)
        op_code = bytes_to_bint(b)
        while op_code == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            h, oid, buf = self._parse_op_response()
            b = self._recv_channel(4)
        if op_code == self.op_cont_auth:
            raise OperationalError('Unauthorized')
        elif op_code != self.op_response:
            raise InternalError("_op_response:op_code = %d" % (op_code,))
        return self._parse_op_response()

    def _parse_connect_response(self):
        # want and treat op_accept or op_cond_accept or op_accept_data
        b = self._recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self._recv_channel(4)
        if bytes_to_bint(b) == self.op_reject:
            raise OperationalError('Connection is rejected')

        op_code = bytes_to_bint(b)
        if op_code == self.op_response:
            return self._parse_op_response()    # error occurred

        b = self._recv_channel(12)
        self.accept_version = b[3]
        self.accept_architecture = bytes_to_bint(b[4:8])
        self.accept_type = bytes_to_bint(b[8:])
        self.lazy_response_count = 0

        if op_code == self.op_cond_accept or op_code == self.op_accept_data:
            ln = bytes_to_bint(self._recv_channel(4))
            data = self._recv_channel(ln, word_alignment=True)

            ln = bytes_to_bint(self._recv_channel(4))
            self.accept_plugin_name = self._recv_channel(ln, word_alignment=True)

            is_authenticated = bytes_to_bint(self._recv_channel(4))
            ln = bytes_to_bint(self._recv_channel(4))
            self._recv_channel(ln, word_alignment=True)   # keys

            if is_authenticated == 0:
                if self.accept_plugin_name in (b'Srp256',  b'Srp'):
                    hash_algo = {
                        b'Srp256': hashlib.sha256,
                        b'Srp': hashlib.sha1,
                    }[self.accept_plugin_name]

                    user = self.user
                    if len(user) > 2 and user[0] == user[-1] == '"':
                        user = user[1:-1]
                        user = user.replace('""', '"')
                    else:
                        user = user.upper()

                    if len(data) == 0:
                        # send op_cont_auth
                        self._op_cont_auth(
                            srp.long2bytes(self.client_public_key),
                            self.accept_plugin_name,
                            self.plugin_list,
                            b''
                        )
                        b = self._recv_channel(4)
                        if bytes_to_bint(b) == self.op_response:
                            self._parse_op_response()   # error occurred
                        # parse op_cont_auth
                        assert bytes_to_bint(b) == self.op_cont_auth
                        ln = bytes_to_bint(self._recv_channel(4))
                        data = self._recv_channel(ln, word_alignment=True)
                        ln = bytes_to_bint(self._recv_channel(4))
                        self._recv_channel(ln, word_alignment=True)  # plugin_name
                        ln = bytes_to_bint(self._recv_channel(4))
                        self._recv_channel(ln, word_alignment=True)  # plugin_list
                        ln = bytes_to_bint(self._recv_channel(4))
                        self._recv_channel(ln, word_alignment=True)  # keys

                    if len(data) == 0:
                        raise OperationalError('Unauthorized')

                    ln = bytes_to_int(data[:2])
                    server_salt = data[2:ln+2]
                    server_public_key = srp.bytes2long(
                        hex_to_bytes(data[4+ln:]))

                    auth_data, session_key = srp.client_proof(
                        self.str_to_bytes(user),
                        self.str_to_bytes(self.password),
                        server_salt,
                        self.client_public_key,
                        server_public_key,
                        self.client_private_key,
                        hash_algo)
                elif self.accept_plugin_name == b'Legacy_Auth':
                    auth_data = self.str_to_bytes(get_crypt(self.password))
                    session_key = b''
                else:
                    raise OperationalError(
                        'Unknown auth plugin %s' % (self.accept_plugin_name)
                    )
            else:
                auth_data = b''
                session_key = b''

            if op_code == self.op_cond_accept:
                self._op_cont_auth(
                    auth_data,
                    self.accept_plugin_name,
                    self.plugin_list,
                    b''
                )
                (h, oid, buf) = self._op_response()
                enc_plugin, nonce = guess_wire_crypt(buf)
            else:
                enc_plugin = nonce = None

            if enc_plugin and self.wire_crypt and session_key:
                self._op_crypt(enc_plugin)
                if enc_plugin in (b'ChaCha64', b'ChaCha'):
                    k = hashlib.sha256(session_key).digest()
                    self.sock.set_translator(
                        ChaCha20.new(k, nonce),
                        ChaCha20.new(k, nonce),
                    )
                elif enc_plugin == b'Arc4':
                    self.sock.set_translator(
                        ARC4.new(session_key), ARC4.new(session_key))
                else:
                    raise OperationalError(
                        'Unknown wirecrypt plugin %s' % (enc_plugin)
                    )
                (h, oid, buf) = self._op_response()
            else:
                # no matched wire encription plugin
                # self.auth_data use _op_attach() and _op_create()
                self.auth_data = auth_data
        else:
            assert op_code == self.op_accept

    def _op_sql_response(self, xsqlda):
        b = self._recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self._recv_channel(4)
        op_code = bytes_to_bint(b)
        if op_code != self.op_sql_response:
            if op_code == self.op_response:
                self._parse_op_response()
            raise InternalError("_op_sql_response:op_code = %d" % (op_code,))

        b = self._recv_channel(4)
        count = bytes_to_bint(b[:4])
        r = []
        if count == 0:
            return []
        if self.accept_version < PROTOCOL_VERSION13:
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if x.io_length() < 0:
                    b = self._recv_channel(4)
                    ln = bytes_to_bint(b)
                else:
                    ln = x.io_length()
                raw_value = self._recv_channel(ln, word_alignment=True)
                if self._recv_channel(4) == bytes([0]) * 4:     # Not NULL
                    r.append(x.value(raw_value))
                else:
                    r.append(None)
        else:
            n = len(xsqlda) // 8
            if len(xsqlda) % 8 != 0:
                n += 1
            null_indicator = 0
            for c in reversed(self._recv_channel(n, word_alignment=True)):
                null_indicator <<= 8
                null_indicator += c
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if null_indicator & (1 << i):
                    r.append(None)
                else:
                    if x.io_length() < 0:
                        b = self._recv_channel(4)
                        ln = bytes_to_bint(b)
                    else:
                        ln = x.io_length()
                    raw_value = self._recv_channel(ln, word_alignment=True)
                    r.append(x.value(raw_value))
        return r

    def _op_fetch_response(self, stmt_handle, xsqlda):
        op_code = bytes_to_bint(self._recv_channel(4))
        while op_code == self.op_dummy:
            op_code = bytes_to_bint(self._recv_channel(4))

        while op_code == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            h, oid, buf = self._parse_op_response()
            op_code = bytes_to_bint(self._recv_channel(4))

        if op_code != self.op_fetch_response:
            if op_code == self.op_response:
                self._parse_op_response()
            raise InternalError("op_fetch_response:op_code = %d" % (op_code,))
        b = self._recv_channel(8)
        status = bytes_to_bint(b[:4])
        count = bytes_to_bint(b[4:8])
        rows = []
        while count:
            r = [None] * len(xsqlda)
            if self.accept_version < PROTOCOL_VERSION13:
                for i in range(len(xsqlda)):
                    x = xsqlda[i]
                    if x.io_length() < 0:
                        b = self._recv_channel(4)
                        ln = bytes_to_bint(b)
                    else:
                        ln = x.io_length()
                    raw_value = self._recv_channel(ln, word_alignment=True)
                    if self._recv_channel(4) == bytes([0]) * 4:     # Not NULL
                        r[i] = x.value(raw_value)
            else:   # PROTOCOL_VERSION13
                n = len(xsqlda) // 8
                if len(xsqlda) % 8 != 0:
                    n += 1
                null_indicator = 0
                for c in reversed(self._recv_channel(n, word_alignment=True)):
                    null_indicator <<= 8
                    null_indicator += c
                for i in range(len(xsqlda)):
                    x = xsqlda[i]
                    if null_indicator & (1 << i):
                        continue
                    if x.io_length() < 0:
                        b = self._recv_channel(4)
                        ln = bytes_to_bint(b)
                    else:
                        ln = x.io_length()
                    raw_value = self._recv_channel(ln, word_alignment=True)
                    r[i] = x.value(raw_value)
            rows.append(r)
            b = self._recv_channel(12)
            op_code = bytes_to_bint(b[:4])
            status = bytes_to_bint(b[4:8])
            count = bytes_to_bint(b[8:])
        return rows, status != 100


class ConnectionBase(WireProtocol):
    def cursor(self, factory=Cursor):
        DEBUG_OUTPUT("Connection::cursor()")
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
        self._cursors[self._transaction] = []
        return factory(self)

    def begin(self):
        DEBUG_OUTPUT("Connection::begin()")
        if not self.sock:
            raise InternalError("Missing socket")
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
        self._cursors[self._transaction] = []
        self._transaction.begin()

    def commit(self, retaining=False):
        DEBUG_OUTPUT("Connection::commit()")
        if self._transaction:
            self._transaction.commit(retaining=retaining)

    def savepoint(self, name):
        DEBUG_OUTPUT("Connection::savepoint()", name)
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

    def ping(self, reconnect=True):
        try:
            self._op_ping()
            (h, oid, buf) = self._op_response()
            return h == 0
        except:
            if reconnect:
                self.reconnect()
                return self.ping(False)

    def __init__(
        self, dsn=None, user=None, password=None, role=None, host=None,
        database=None, charset=DEFAULT_CHARSET, port=None,
        page_size=4096, is_services=False, cloexec=False,
        timeout=None, isolation_level=None,
        auth_plugin_name=None, wire_crypt=True, create_new=False,
        timezone=None
    ):
        DEBUG_OUTPUT("Connection::__init__()", id(self))
        self.accept_plugin_name = ''
        self.auth_data = b''
        if auth_plugin_name is None:
            auth_plugin_name = 'Srp256'

        self.sock = None
        self.db_handle = None
        (self.hostname, self.port, self.filename, self.user, self.password) = \
            parse_dsn(dsn, host, port, database, user, password)
        self.role = role
        self.charset = charset
        self.timeout = float(timeout) if timeout is not None else None
        self.auth_plugin_name = auth_plugin_name
        self.wire_crypt = wire_crypt
        self.create_new = create_new
        self.page_size = page_size
        self.is_services = is_services
        self.cloexec = cloexec
        if isolation_level is None:
            self.isolation_level = ISOLATION_LEVEL_READ_COMMITED
        else:
            self.isolation_level = int(isolation_level)
        self.timezone = timezone


    def _initialize(self):
        self.last_event_id = 0
        self._autocommit = False
        self._transaction = None
        self._cursors = {}

        self.sock = SocketStream(self.hostname, self.port, self.timeout, self.cloexec)

        self._op_connect(self.auth_plugin_name, self.wire_crypt)
        try:
            self._parse_connect_response()
        except OperationalError as e:
            self.sock.close()
            self.sock = None
            raise e
        if self.create_new:                      # create database
            self._op_create(self.timezone, self.page_size)
        elif self.is_services:                  # service api
            self._op_service_attach()
        else:                                   # connect
            self._op_attach(self.timezone)
        (h, oid, buf) = self._op_response()
        self.db_handle = h
        DEBUG_OUTPUT("Connection::_initialize()", id(self), self.db_handle)

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        "On successful exit, commit. On exception, rollback. "
        if exc:
            self.rollback()
        else:
            self.commit()
        self.close()

    def reconnect(self):
        self._close()
        self._initialize()

    def set_isolation_level(self, isolation_level):
        self.isolation_level = int(isolation_level)

    def set_autocommit(self, is_autocommit):
        if self._autocommit != is_autocommit and self._transaction is not None:
            self.rollback()
            self._transaction = None
        self._autocommit = is_autocommit

    def _db_info(self, info_requests):
        if info_requests[-1] == isc_info_end:
            self._op_info_database(bytes(info_requests))
        else:
            self._op_info_database(bytes(info_requests+type(info_requests)([isc_info_end])))
        (h, oid, buf) = self._op_response()
        i = 0
        i_request = 0
        r = []
        while i < len(buf):
            req = buf[i]
            if req == isc_info_end:
                break
            assert req == info_requests[i_request] or req == isc_info_error
            if req == isc_info_user_names:
                user_names = []
                while req == isc_info_user_names:
                    ln = bytes_to_int(buf[i+1:i+3])
                    user_names.append(buf[i+3:i+3+ln])
                    i = i + 3 + ln
                    req = buf[i]
                r.append((req, user_names))
            else:
                ln = bytes_to_int(buf[i+1:i+3])
                r.append((req, buf[i+3:i+3+ln]))
                i = i + 3 + ln
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
            return v[1]
        elif info_request in (isc_info_db_id, ):
            # IB6 API guide p52
            conn_code = v[0]
            len1 = v[1]
            filename = self.bytes_to_str(v[2:2+len1])
            len2 = v[2+len1]
            sitename = self.bytes_to_str(v[3+len1:3+len1+len2])
            return (conn_code, filename, sitename)
        elif info_request in (isc_info_implementation, ):
            return (v[1], v[2])
        elif info_request in (isc_info_version, isc_info_firebird_version):
            # IB6 API guide p53
            return self.bytes_to_str(v[2:2+v[1]])
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

    def _close(self):
        if self.sock is None:
            return
        if self.db_handle is not None:
            # cleanup transaction
            for trans in self._cursors.keys():
                trans.close()
            if self.is_services:
                self._op_service_detach()
            else:
                self._op_detach()
            (h, oid, buf) = self._op_response()
        self.sock.close()
        self.sock = None
        self.db_handle = None

    def close(self):
        DEBUG_OUTPUT("Connection::close()", id(self), self.db_handle)
        self._close()

    def drop_database(self):
        DEBUG_OUTPUT("Connection::drop_database()")
        self._op_drop_database()
        (h, oid, buf) = self._op_response()
        self.sock.close()
        self.sock = None
        self.db_handle = None

    def event_conduit(self, event_count, event_id=None):
        return EventConduit(self, event_count, event_id, self.timeout)

    def __del__(self):
        if self.sock:
            self.close()

    def is_disconnect(self):
        return self.sock is None


class Connection(ConnectionBase, ConnectionResponseMixin):
    def __init__(self, *args, **kwargs):
        ConnectionBase.__init__(self, *args, **kwargs)
