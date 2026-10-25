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
import struct
import datetime
import decimal
import itertools
import hashlib
import select
import weakref
from collections.abc import Iterable, Iterator, Sequence
from typing import Any
from firebirdsql.fberrmsgs import messages
from firebirdsql.err import InternalError, OperationalError, NotSupportedError, IntegrityError, DataError, ProgrammingError, DatabaseError
from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql.wireprotocol import WireProtocol, get_crypt
from firebirdsql.stream import SocketStream
from firebirdsql.xsqlvar import calc_blr, parse_xsqlda
from firebirdsql.event_conduit import EventConduit
from firebirdsql import srp
try:    # C implementations when pycryptodome is installed
    from Crypto.Cipher import ARC4
except ImportError:
    from firebirdsql.arc4 import ARC4
try:
    from Crypto.Cipher import ChaCha20
except ImportError:
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
        self.bind_xsqlda = []

    def fetch_generator(self, rows, more_data, fetch_count):
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
                    elif x.sqltype == SQL_TYPE_ARRAY:
                        if not r[i]:
                            continue
                        if x.array_meta:
                            r[i] = connection._op_get_slice(self.trans.trans_handle, r[i], x.array_meta)
                        else:
                            raise OperationalError(
                                f"cannot fetch array column '{x.aliasname or x.fieldname}': array metadata unavailable"
                            )
                yield tuple(r)
            if more_data:
                connection._op_fetch(self.handle, calc_blr(self.xsqlda), fetch_count)
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
        self.stmt_type, self.xsqlda, self.bind_xsqlda = parse_xsqlda(buf[i:], self.trans.connection, self.handle)
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
    def __init__(self, cur: 'Cursor', sql: str, explain_plan: bool = False) -> None:
        DEBUG_OUTPUT("PreparedStatement::__init__()")
        cur.transaction.check_trans_handle()
        self.stmt = Statement(cur.transaction)
        self.stmt.prepare(sql, explain_plan)
        self.sql = sql

    def __getattr__(self, attrname: str) -> Any:
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

    def close(self) -> None:
        DEBUG_OUTPUT("PreparedStatement::close()")
        self.stmt.close()


class Cursor(object):
    def __init__(self, obj: 'ConnectionBase | Transaction', timeout: float | int | None = None) -> None:
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
            conn._cursors[self._transaction] = weakref.WeakSet()
        conn._cursors[self._transaction].add(self)
        self.stmt: Statement | None = None
        self.arraysize: int = 1
        self.rowcount: int = -1
        self.statement_timeout: float | int | None = timeout if timeout is not None else getattr(conn, 'statement_timeout', None)
        self._fetch_records = None
        self._callproc_result = None

    @property
    def timeout(self) -> float | int | None:
        return self.statement_timeout

    @timeout.setter
    def timeout(self, value: float | int | None) -> None:
        self.statement_timeout = value

    def __enter__(self) -> 'Cursor':
        return self

    def __exit__(self, exc: Any, value: Any, traceback: Any) -> None:
        self.close()

    @property
    def transaction(self) -> 'Transaction':
        return self._transaction

    def _convert_params(self, params: Sequence[Any]) -> list[Any]:
        cooked_params = []
        for param in params:
            if isinstance(param, str):
                param = self.transaction.connection.str_to_bytes(param)
            cooked_params.append(param)
        return cooked_params

    def _get_stmt(self, query: str | PreparedStatement) -> Statement:
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

    def prep(self, query: str, explain_plan: bool = False) -> PreparedStatement:
        DEBUG_OUTPUT("Cursor::prep()")
        prepared_statement = PreparedStatement(self, query, explain_plan=explain_plan)
        return prepared_statement

    def _execute(self, query: str | PreparedStatement, params: Sequence[Any] | None, timeout: float | int | None = None) -> 'Cursor':
        if params is None:
            params = []
        self.transaction.check_trans_handle()
        stmt = self._get_stmt(query)
        cooked_params = self._convert_params(params)
        if stmt.stmt_type == isc_info_sql_stmt_exec_procedure:
            self.transaction.connection._op_execute2(
                stmt.handle,
                self.transaction.trans_handle, cooked_params,
                calc_blr(stmt.xsqlda),
                input_xsqlda=stmt.bind_xsqlda,
                timeout=timeout)
            self._callproc_result = self.transaction.connection._op_sql_response(stmt.xsqlda)
            self.transaction.connection._op_response()
            if self._callproc_result:
                for i in range(len(stmt.xsqlda)):
                    x = stmt.xsqlda[i]
                    if x.sqltype == SQL_TYPE_ARRAY and self._callproc_result[i]:
                        if x.array_meta:
                            self._callproc_result[i] = self.transaction.connection._op_get_slice(
                                self.transaction.trans_handle, self._callproc_result[i], x.array_meta
                            )
                        else:
                            raise OperationalError(
                                f"cannot fetch array column '{x.aliasname or x.fieldname}': array metadata unavailable"
                            )
            self._fetch_records = None
        else:
            self.transaction.connection._op_execute(
                stmt.handle, self.transaction.trans_handle, cooked_params,
                input_xsqlda=stmt.bind_xsqlda,
                timeout=timeout)
            (h, oid, buf) = self.transaction.connection._op_response()

            if stmt.stmt_type == isc_info_sql_stmt_select:
                fetch_count = max(self.arraysize, DEFAULT_FETCH_COUNT)
                self.transaction.connection._op_fetch(stmt.handle, calc_blr(stmt.xsqlda), fetch_count)
                (rows, more_data) = self.transaction.connection._op_fetch_response(stmt.handle, stmt.xsqlda)
                self._fetch_records = stmt.fetch_generator(rows, more_data, fetch_count)
            else:
                self._fetch_records = None
            self._callproc_result = None

        return self

    def _is_execute_procedure_query(self) -> bool:
        """Return True if the current query is EXECUTE PROCEDURE / EXECUTE BLOCK."""
        q = self.query
        if isinstance(q, PreparedStatement):
            q = q.sql
        return isinstance(q, str) and q.lstrip().upper().startswith('EXECUTE')

    def execute(self, query: str | PreparedStatement, params: Sequence[Any] | None = None, timeout: float | int | None = None) -> 'Cursor':
        DEBUG_OUTPUT("Cursor::execute()", query, params)
        eff_timeout = timeout if timeout is not None else self.statement_timeout
        try:
            self._execute(query, params, timeout=eff_timeout)
            self.rowcount = self._rowcount()
            # DML with RETURNING returns a phantom row of NULLs when no rows
            # are affected. Clear it so fetchone()/fetchall() return nothing,
            # matching PEP 249. Don't clear for EXECUTE PROCEDURE statements.
            if (self.rowcount == 0 and self._callproc_result is not None
                    and self.stmt and self.stmt.xsqlda
                    and self.stmt.stmt_type == isc_info_sql_stmt_exec_procedure
                    and not self._is_execute_procedure_query()):
                self._callproc_result = None
            return self
        finally:
            self.transaction.is_dirty = True

    def callproc(self, procname: str, params: Sequence[Any] | None = None) -> tuple[Any, ...] | None:
        if params is None:
            params = []
        DEBUG_OUTPUT("Cursor::callproc()")
        query = 'EXECUTE PROCEDURE ' + procname + ' ' + ','.join('?'*len(params))
        self.execute(query, params)
        return tuple(self._callproc_result) if self._callproc_result else None

    def _is_safe_batch_row(self, bind_xsqlda: Sequence[Any], params: Sequence[Any]) -> bool:
        if len(params) != len(bind_xsqlda):
            return False
        for i, x in enumerate(bind_xsqlda):
            val = params[i]
            if val is None:
                continue
            if x.sqltype == SQL_TYPE_BOOLEAN:
                if type(val) is not bool:
                    return False
            elif x.sqltype == SQL_TYPE_DATE:
                if type(val) is not datetime.date or isinstance(val, datetime.datetime):
                    return False
            elif x.sqltype == SQL_TYPE_TIME:
                if type(val) is not datetime.time or val.tzinfo is not None:
                    return False
            elif x.sqltype == SQL_TYPE_TIMESTAMP:
                if type(val) is not datetime.datetime or val.tzinfo is not None:
                    return False
            elif x.sqltype == SQL_TYPE_TIME_TZ:
                if type(val) is not datetime.time or val.tzinfo is None:
                    return False
            elif x.sqltype == SQL_TYPE_TIMESTAMP_TZ:
                if type(val) is not datetime.datetime or val.tzinfo is None:
                    return False
            elif x.sqltype in (SQL_TYPE_TEXT, SQL_TYPE_VARYING):
                if isinstance(val, str):
                    b = self.transaction.connection.str_to_bytes(val)
                elif isinstance(val, bytes):
                    b = val
                else:
                    return False
                if x.sqllen and len(b) > x.sqllen:
                    return False
            elif x.sqltype == SQL_TYPE_SHORT:
                if not isinstance(val, (int, float, decimal.Decimal)):
                    return False
                v = self._batch_integer_value(x, val)
                if v is None:
                    return False
                if v < -32768 or v > 32767:
                    return False
            elif x.sqltype == SQL_TYPE_LONG:
                if not isinstance(val, (int, float, decimal.Decimal)):
                    return False
                v = self._batch_integer_value(x, val)
                if v is None:
                    return False
                if v < -2147483648 or v > 2147483647:
                    return False
            elif x.sqltype in (SQL_TYPE_INT64, SQL_TYPE_QUAD):
                if not isinstance(val, (int, float, decimal.Decimal)):
                    return False
                v = self._batch_integer_value(x, val)
                if v is None:
                    return False
                if v < -9223372036854775808 or v > 9223372036854775807:
                    return False
            elif x.sqltype in (SQL_TYPE_FLOAT, SQL_TYPE_DOUBLE, SQL_TYPE_D_FLOAT):
                if not isinstance(val, (int, float, decimal.Decimal)):
                    return False
        return True

    @staticmethod
    def _batch_integer_value(x: Any, val: int | float | decimal.Decimal) -> int | None:
        try:
            number = val if isinstance(val, decimal.Decimal) else decimal.Decimal(str(val))
            if x.sqlscale < 0:
                number *= decimal.Decimal(10) ** (-x.sqlscale)
            if number != number.to_integral_value():
                return None
            return int(number)
        except Exception:
            return None

    def _can_use_batch(self, stmt: Statement, seq_of_params: Any = None) -> bool:
        if self.statement_timeout:
            return False
        conn = self.transaction.connection
        if conn.accept_version < PROTOCOL_VERSION16:
            return False
        if stmt.stmt_type not in (isc_info_sql_stmt_insert, isc_info_sql_stmt_update, isc_info_sql_stmt_delete):
            return False
        if len(stmt.xsqlda) > 0:
            return False
        if len(stmt.bind_xsqlda) == 0:
            return False
        if any(x.sqltype not in BATCH_SUPPORTED_SQL_TYPES for x in stmt.bind_xsqlda):
            return False
        if seq_of_params:
            for params in seq_of_params:
                if not self._is_safe_batch_row(stmt.bind_xsqlda, params):
                    return False
        return True

    def _executemany_batch(self, stmt: Statement, seq_of_params: Iterable[Sequence[Any]]) -> int:
        self._fetch_records = None
        self._callproc_result = None
        conn = self.transaction.connection
        flush_threshold = getattr(self, "batch_flush_threshold", 128 * 1024)
        native_msg_len = conn._calculate_batch_msg_len(stmt.bind_xsqlda)
        batch_created = False
        rows_buf = bytearray()
        batch_row_count = 0
        total_affected = 0

        def _flush_and_exec():
            nonlocal rows_buf, batch_row_count, batch_created, total_affected
            if batch_row_count > 0:
                conn._op_batch_msg(stmt.handle, rows_buf, batch_row_count)
                rows_buf = bytearray()
                batch_row_count = 0
            if batch_created:
                affected = conn._op_batch_exec(stmt.handle, self.transaction.trans_handle)
                if affected > 0:
                    total_affected += affected
                conn._op_batch_rls(stmt.handle)
                batch_created = False

        try:
            for params in seq_of_params:
                if not batch_created:
                    conn._op_batch_create(stmt.handle, stmt.bind_xsqlda)
                    batch_created = True

                row_bytes = conn._encode_batch_row(stmt.bind_xsqlda, params)
                rows_buf.extend(row_bytes)
                batch_row_count += 1

                if len(rows_buf) >= flush_threshold or (batch_row_count * native_msg_len) >= flush_threshold:
                    _flush_and_exec()

            _flush_and_exec()
            return total_affected
        finally:
            if batch_created:
                try:
                    conn._op_batch_rls(stmt.handle)
                except Exception:
                    pass

    def executemany(self, query: str | PreparedStatement, seq_of_params: Sequence[Sequence[Any]]) -> None:
        self.transaction.check_trans_handle()
        stmt = self._get_stmt(query)
        self._fetch_records = None
        self._callproc_result = None

        if isinstance(seq_of_params, (list, tuple)):
            param_list = seq_of_params
            can_batch = self._can_use_batch(stmt, param_list)
        else:
            param_list = seq_of_params
            can_batch = False

        if can_batch:
            try:
                self.rowcount = self._executemany_batch(stmt, param_list)
                return
            finally:
                self.transaction.is_dirty = True

        total = 0
        for params in param_list:
            self.execute(query, params)
            if self.rowcount > 0:
                total += self.rowcount
        self.rowcount = total

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self.transaction.is_dirty:
            DEBUG_OUTPUT("Cursor::fetchone() not dirty")
            return None
        # callproc or not select statement
        if not self._fetch_records:
            if self._callproc_result:
                r = tuple(self._callproc_result)
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

    def __iter__(self) -> 'Cursor':
        return self

    def __next__(self) -> tuple[Any, ...]:
        r = self.fetchone()
        if not r:
            raise StopIteration()
        return r

    def next(self) -> tuple[Any, ...]:
        return self.__next__()

    def fetchall(self) -> list[tuple[Any, ...]] | None:
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

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
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
    def fetchonemap(self) -> RowMapping | dict[str, Any]:
        r = self.fetchone()
        if r is None:
            return {}
        return RowMapping(r, self.description)

    def fetchallmap(self) -> list[RowMapping] | None:
        desc = self.description
        rows = self.fetchall()
        if rows is None:
            return None
        return [RowMapping(row, desc) for row in rows]

    def fetchmanymap(self, size: int | None = None) -> list[RowMapping]:
        desc = self.description
        return [RowMapping(row, desc) for row in self.fetchmany(size)]

    def itermap(self) -> Iterator[RowMapping]:
        r = self.fetchonemap()
        while r:
            yield r
            r = self.fetchonemap()

    def close(self) -> None:
        DEBUG_OUTPUT("Cursor::close()")
        if self._transaction in self.transaction.connection._cursors:
            self.transaction.connection._cursors[self._transaction].discard(self)
        if not self.stmt:
            return
        self.stmt.drop()
        self.stmt = None

    def __del__(self) -> None:
        try:
            if hasattr(self, 'stmt') and self.stmt and self.stmt.handle != -1:
                if hasattr(self, '_transaction') and self._transaction:
                    if self._transaction.connection and self._transaction.connection.sock is not None:
                        self.close()
        except Exception:
            pass

    def nextset(self) -> bool:
        raise NotSupportedError()

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass

    @property
    def description(self) -> list[tuple[str, int, int, int, int, int, bool]] | None:
        if not self.stmt:
            return None
        if not self.stmt.xsqlda:
            return None
        return [(
            x.aliasname, x.sqltype, x.display_length(), x.io_length(),
            x.precision(), x.sqlscale, True if x.null_ok else False
        ) for x in self.stmt.xsqlda]

    def _rowcount(self) -> int:
        DEBUG_OUTPUT("Cursor::rowcount()")
        if not self.stmt or self.stmt.handle == -1:
            return -1

        self.transaction.connection._op_info_sql(self.stmt.handle, bytes([isc_info_sql_records]))
        (h, oid, buf) = self.transaction.connection._op_response()
        if buf[:3] != bytes([0x17, 0x1d, 0x00]):    # isc_info_sql_records
            count = -1
        elif self.stmt.stmt_type == isc_info_sql_stmt_select:
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

    def __init__(self, connection: 'ConnectionBase', is_autocommit: bool = False, isolation_level: int | None = None) -> None:
        DEBUG_OUTPUT("Transaction::__init__()")
        self._connection = connection
        self._trans_handle: int | None = None
        self._autocommit = is_autocommit
        self._isolation_level = isolation_level

    def _begin(self) -> None:
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

    def close(self) -> None:
        if self._trans_handle is None:
            return
        if not self.is_dirty:
            return
        DEBUG_OUTPUT("Transaction::close()", self._trans_handle, self.connection.db_handle)
        self.connection._op_rollback(self._trans_handle)
        (h, oid, buf) = self.connection._op_response()
        self._trans_handle = None
        self.is_dirty = False

    def begin(self) -> None:
        DEBUG_OUTPUT("Transaction::begin()")
        self._begin()

    def savepoint(self, name: str) -> None:
        DEBUG_OUTPUT("Transaction::savepoint()", name)
        if self._trans_handle is None:
            return
        self.connection._op_exec_immediate(self._trans_handle, query='SAVEPOINT '+name)
        (h, oid, buf) = self.connection._op_response()

    def commit(self, retaining: bool = False) -> None:
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

    def rollback(self, retaining: bool = False, savepoint: str | None = None) -> None:
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

    def _trans_info(self, info_requests: Sequence[int]) -> list[tuple[int, bytes]]:
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

    def trans_info(self, info_requests: int | Sequence[int]) -> dict[int, Any]:
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

    def check_trans_handle(self) -> None:
        if self._trans_handle is None:
            self._begin()

    @property
    def connection(self) -> 'ConnectionBase':
        return self._connection

    @property
    def trans_handle(self) -> int:
        assert(self._trans_handle is not None)
        return self._trans_handle


class ConnectionResponseMixin:
    def _recv_channel(self, nbytes, word_alignment=False):
        n = nbytes
        if word_alignment and (n % 4):
            n += 4 - nbytes % 4  # 4 bytes word alignment
        r = bytearray()
        if hasattr(self, '_recv_buf') and self._recv_buf:
            take = min(n, len(self._recv_buf))
            r.extend(self._recv_buf[:take])
            del self._recv_buf[:take]
            n -= take
        while n:
            if (self.timeout is not None and select.select([self.sock._sock], [], [], self.timeout)[0] == []):
                raise TimeoutError("Pakcet recv timeout error")
            b = self.sock.recv(n)
            if not b:
                break
            r.extend(b)
            n -= len(b)
        if len(r) < nbytes:
            raise OperationalError('Can not recv() packets')
        return bytes(r[:nbytes])

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

    def _receive_two_responses(self):
        first_exc = None
        for _ in range(2):
            try:
                self._op_response()
            except (IntegrityError, DataError, OperationalError, ProgrammingError, DatabaseError) as e:
                if first_exc is None:
                    first_exc = e
        if first_exc is not None:
            raise first_exc

    def _op_batch_cs(self):
        b = self._recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self._recv_channel(4)
        while bytes_to_bint(b) == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            self._parse_op_response()
            b = self._recv_channel(4)
        op_code = bytes_to_bint(b)
        if op_code != self.op_batch_cs:
            if op_code == self.op_response:
                self._parse_op_response()
            raise InternalError(f"_op_batch_cs: op_code = {op_code}")

        stmt_h = bytes_to_bint(self._recv_channel(4))
        total_records = bytes_to_bint(self._recv_channel(4))
        update_count = bytes_to_bint(self._recv_channel(4))
        detailed_count = bytes_to_bint(self._recv_channel(4))
        simplified_count = bytes_to_bint(self._recv_channel(4))
        update_counts = [bytes_to_bint(self._recv_channel(4)) for _ in range(update_count)]
        detailed_errors = []
        for _ in range(detailed_count):
            row_num = bytes_to_bint(self._recv_channel(4))
            gds_codes, sql_code, message = self._parse_status_vector()
            detailed_errors.append((row_num, gds_codes, sql_code, message))
        simplified_errors = [bytes_to_bint(self._recv_channel(4)) for _ in range(simplified_count)]

        if detailed_errors:
            row_num, gds_codes, sql_code, message = detailed_errors[0]
            if gds_codes.intersection([
                335544838, 335544879, 335544880, 335544466, 335544665, 335544347, 335544558
            ]):
                raise IntegrityError(f"Row {row_num}: {message}", gds_codes, sql_code)
            elif gds_codes.intersection([335544321]):
                raise DataError(f"Row {row_num}: {message}", gds_codes, sql_code)
            elif (sql_code or message) and not gds_codes.intersection([335544434]):
                raise OperationalError(f"Row {row_num}: {message}", gds_codes, sql_code)
            else:
                raise OperationalError(f"Row {row_num}: batch execution failed")
        if simplified_errors:
            raise OperationalError(f"Row {simplified_errors[0]}: batch execution failed")

        return sum(u for u in update_counts if u > 0) if update_counts else total_records

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

        if self.accept_type & pflag_compress:
            self.sock.enable_compression()
            self.accept_type &= ptype_MASK

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
                        ChaCha20.new(key=k, nonce=nonce),
                        ChaCha20.new(key=k, nonce=nonce),
                    )
                elif enc_plugin == b'Arc4':
                    self.sock.set_translator(
                        ARC4.new(key=session_key), ARC4.new(key=session_key))
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

    def _op_slice_response(self, meta):
        from firebirdsql.array import (
            array_get_element_stride, slice_wire_element_size, decode_slice_elements
        )
        b = self._recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self._recv_channel(4)
        op_code = bytes_to_bint(b)
        while op_code == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            h, oid, buf = self._parse_op_response()
            b = self._recv_channel(4)
        if op_code == self.op_response:
            self._parse_op_response()
            raise InternalError("_op_slice_response: error")
        if op_code not in (self.op_slice, self.op_slice_response):
            raise InternalError("_op_slice_response: unexpected op_code = %d" % (op_code,))

        b = self._recv_channel(8)
        slr_len = bytes_to_bint(b[:4])
        lstr_len = bytes_to_bint(b[4:8])
        if lstr_len == 0:
            return []

        stride = array_get_element_stride(meta)
        if stride < 1:
            raise OperationalError(f"invalid array element stride {stride}")
        count = lstr_len // stride

        elem_len, varying = slice_wire_element_size(meta)
        data = bytearray()
        for i in range(count):
            if varying:
                len_buf = self._recv_channel(4)
                l = bytes_to_bint(len_buf)
                elem_data = self._recv_channel(l, word_alignment=True)
                data.extend(len_buf)
                data.extend(elem_data)
            else:
                chunk = self._recv_channel(elem_len, word_alignment=True)
                data.extend(chunk)

        return decode_slice_elements(meta, bytes(data), self.bytes_to_str)

    def _op_fetch_response(self, stmt_handle, xsqlda):
        if not hasattr(self, '_recv_buf'):
            self._recv_buf = bytearray()
        buf = self._recv_buf
        pos = 0
        mv = None

        def ensure(needed):
            nonlocal pos, mv, raw_value
            if len(buf) - pos < needed:
                raw_value = None
                mv = None
                if pos > 0:
                    del buf[:pos]
                    pos = 0
                while len(buf) < needed:
                    if self.timeout is not None and select.select([self.sock._sock], [], [], self.timeout)[0] == []:
                        raise TimeoutError("Pakcet recv timeout error")
                    is_compressed = getattr(self.sock, '_decompressor', None) is not None
                    read_size = (needed - len(buf)) if is_compressed else max(65536, needed - len(buf))
                    chunk = self.sock.recv(read_size)
                    if not chunk:
                        raise OperationalError('Can not recv() packets')
                    buf.extend(chunk)
                mv = memoryview(buf)
            elif mv is None:
                mv = memoryview(buf)

        try:
            ensure(4)
            op_code = struct.unpack_from('>i', mv, pos)[0]
            pos += 4
            while op_code == self.op_dummy:
                ensure(4)
                op_code = struct.unpack_from('>i', mv, pos)[0]
                pos += 4

            while op_code == self.op_response and self.lazy_response_count:
                self.lazy_response_count -= 1
                mv = None
                del buf[:pos]
                pos = 0
                h, oid, b = self._parse_op_response()
                ensure(4)
                op_code = struct.unpack_from('>i', mv, pos)[0]
                pos += 4

            if op_code != self.op_fetch_response:
                if op_code == self.op_response:
                    mv = None
                    del buf[:pos]
                    pos = 0
                    self._parse_op_response()  # raises the actual Firebird error
                raise InternalError("op_fetch_response:op_code = %d" % (op_code,))

            ensure(8)
            status = struct.unpack_from('>i', mv, pos)[0]
            count = struct.unpack_from('>i', mv, pos + 4)[0]
            pos += 8

            rows = []
            num_cols = len(xsqlda)
            n_null = num_cols // 8 + (1 if num_cols % 8 != 0 else 0)
            aligned_null = n_null + (4 - n_null % 4 if n_null % 4 else 0)

            while count:
                r = [None] * num_cols
                if self.accept_version >= PROTOCOL_VERSION13:
                    ensure(aligned_null)
                    null_indicator = int.from_bytes(mv[pos : pos + n_null], byteorder='little')
                    pos += aligned_null
                    for i in range(num_cols):
                        if null_indicator & (1 << i):
                            continue
                        x = xsqlda[i]
                        io_len = x.io_length()
                        if io_len < 0:
                            ensure(4)
                            ln = struct.unpack_from('>i', mv, pos)[0]
                            pos += 4
                        else:
                            ln = io_len
                        aligned_ln = ln + (4 - ln % 4 if ln % 4 else 0)
                        ensure(aligned_ln)
                        raw_value = mv[pos : pos + ln]
                        pos += aligned_ln
                        if x.sqltype in (SQL_TYPE_BLOB, SQL_TYPE_ARRAY):
                            r[i] = bytes(raw_value)
                        else:
                            r[i] = x.value(raw_value)
                        raw_value = None
                else:
                    for i in range(num_cols):
                        x = xsqlda[i]
                        io_len = x.io_length()
                        if io_len < 0:
                            ensure(4)
                            ln = struct.unpack_from('>i', mv, pos)[0]
                            pos += 4
                        else:
                            ln = io_len
                        aligned_ln = ln + (4 - ln % 4 if ln % 4 else 0)
                        ensure(aligned_ln + 4)
                        raw_value = mv[pos : pos + ln]
                        pos += aligned_ln
                        is_not_null = (struct.unpack_from('>i', mv, pos)[0] == 0)
                        pos += 4
                        if is_not_null:
                            if x.sqltype in (SQL_TYPE_BLOB, SQL_TYPE_ARRAY):
                                r[i] = bytes(raw_value)
                            else:
                                r[i] = x.value(raw_value)
                        raw_value = None
                rows.append(r)

                raw_value = None
                ensure(12)
                next_op = struct.unpack_from('>i', mv, pos)[0]
                pos += 4
                if next_op == self.op_response:
                    raw_value = None
                    mv = None
                    del buf[:pos]
                    pos = 0
                    self._parse_op_response()  # raises the actual Firebird error
                    raise InternalError("op_fetch_response:Internal Error")
                if next_op != self.op_fetch_response:
                    raise InternalError("op_fetch_response:op_code = %d" % (next_op,))
                status = struct.unpack_from('>i', mv, pos)[0]
                count = struct.unpack_from('>i', mv, pos + 4)[0]
                pos += 8

            return rows, status != 100
        finally:
            raw_value = None
            mv = None
            if pos > 0:
                try:
                    del buf[:pos]
                except BufferError:
                    self._recv_buf = bytearray(buf[pos:])


class ConnectionBase(WireProtocol):
    def cursor(self, factory: type[Cursor] = Cursor, timeout: float | int | None = None) -> Cursor:
        DEBUG_OUTPUT("Connection::cursor()")
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
        if timeout is not None:
            return factory(self, timeout=timeout)
        return factory(self)

    def begin(self) -> None:
        DEBUG_OUTPUT("Connection::begin()")
        if not self.sock:
            raise InternalError("Missing socket")
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
        self._cursors[self._transaction] = weakref.WeakSet()
        self._transaction.begin()

    def commit(self, retaining: bool = False) -> None:
        DEBUG_OUTPUT("Connection::commit()")
        if self._transaction:
            self._transaction.commit(retaining=retaining)

    def savepoint(self, name: str) -> None:
        DEBUG_OUTPUT("Connection::savepoint()", name)
        return self._transaction.savepoint(name)

    def rollback(self, retaining: bool = False, savepoint: str | None = None) -> None:
        DEBUG_OUTPUT("Connection::rollback()")
        if self._transaction:
            self._transaction.rollback(retaining=retaining, savepoint=savepoint)

    def execute_immediate(self, query: str) -> None:
        if self._transaction is None:
            self._transaction = Transaction(self, self._autocommit)
            self._transaction.begin()
        self._transaction.check_trans_handle()
        self._op_exec_immediate(
            self._transaction.trans_handle, query=query)
        (h, oid, buf) = self._op_response()
        self._transaction.is_dirty = True

    def ping(self, reconnect: bool = True) -> bool:
        try:
            self._op_ping()
            (h, oid, buf) = self._op_response()
            return h == 0
        except:
            if reconnect:
                self.reconnect()
                return self.ping(False)
            return False

    def __init__(
        self,
        dsn: str | None = None,
        user: str | None = None,
        password: str | None = None,
        role: str | None = None,
        host: str | None = None,
        database: str | None = None,
        charset: str = DEFAULT_CHARSET,
        port: int | None = None,
        page_size: int = 4096,
        is_services: bool = False,
        cloexec: bool = False,
        timeout: float | None = None,
        isolation_level: int | None = None,
        auth_plugin_name: str | None = None,
        wire_crypt: bool = True,
        create_new: bool = False,
        timezone: str | None = None,
        wire_compress: bool = False,
        readonly: bool = False,
        statement_timeout: float | int | None = None,
    ) -> None:
        DEBUG_OUTPUT("Connection::__init__()", id(self))
        self.accept_plugin_name = ''
        self.auth_data = b''
        if auth_plugin_name is None:
            auth_plugin_name = 'Srp256'

        self.sock: SocketStream | None = None
        self.db_handle: int | None = None
        (self.hostname, self.port, self.filename, self.user, self.password) = \
            parse_dsn(dsn, host, port, database, user, password)
        self.role = role
        self.charset = charset
        self.timeout = float(timeout) if timeout is not None else None
        self.statement_timeout = float(statement_timeout) if statement_timeout is not None else None
        self.auth_plugin_name = auth_plugin_name
        self.wire_crypt = wire_crypt
        self.wire_compress = wire_compress
        self.create_new = create_new
        self.page_size = page_size
        self.is_services = is_services
        self.cloexec = cloexec
        if readonly:
            self.isolation_level = ISOLATION_LEVEL_READ_COMMITED_RO
        elif isolation_level is None:
            self.isolation_level = ISOLATION_LEVEL_READ_COMMITED
        else:
            self.isolation_level = int(isolation_level)
        self.timezone = timezone

    def _initialize(self) -> None:
        self.last_event_id = 0
        self._autocommit = False
        self._transaction = None
        self._cursors = {}

        self.sock = SocketStream(self.hostname, self.port, self.timeout, self.cloexec)

        self._op_connect(self.auth_plugin_name, self.wire_crypt, self.wire_compress)
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

    def __enter__(self) -> 'ConnectionBase':
        return self

    def __exit__(self, exc: Any, value: Any, traceback: Any) -> None:
        "On successful exit, commit. On exception, rollback. "
        if exc:
            self.rollback()
        else:
            self.commit()
        self.close()

    def reconnect(self) -> None:
        self._close()
        self._initialize()

    def set_isolation_level(self, isolation_level: int) -> None:
        self.isolation_level = int(isolation_level)

    def set_autocommit(self, is_autocommit: bool) -> None:
        if self._autocommit != is_autocommit and self._transaction is not None:
            self.rollback()
            self._transaction = None
        self._autocommit = is_autocommit

    def _db_info(self, info_requests: Sequence[int]) -> list[tuple[int, Any]]:
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

    def _db_info_convert_type(self, info_request: int, v: Any) -> Any:
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

    def db_info(self, info_requests: int | Sequence[int]) -> Any:
        DEBUG_OUTPUT("Connection::db_info()")
        if isinstance(info_requests, int):  # singleton
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

    def trans_info(self, info_requests: int | Sequence[int]) -> dict[int, Any]:
        if self._transaction:
            return self._transaction.trans_info(info_requests)
        return {}

    def _close(self) -> None:
        if self.sock is None:
            return
        try:
            if self.db_handle is not None:
                # cleanup transaction
                for trans in self._cursors.keys():
                    trans.close()
                if self.is_services:
                    self._op_service_detach()
                else:
                    self._op_detach()
                (h, oid, buf) = self._op_response()
        except (OSError, ConnectionError):
            pass
        finally:
            self.sock.close()
            self.sock = None
            self.db_handle = None

    def close(self) -> None:
        DEBUG_OUTPUT("Connection::close()", id(self), self.db_handle)
        self._close()

    def drop_database(self) -> None:
        DEBUG_OUTPUT("Connection::drop_database()")
        self._op_drop_database()
        (h, oid, buf) = self._op_response()
        if self.sock is not None:
            self.sock.close()
            self.sock = None
        self.db_handle = None

    def event_conduit(self, event_count: Sequence[str], event_id: int | None = None) -> EventConduit:
        return EventConduit(self, event_count, event_id, self.timeout)

    def __del__(self) -> None:
        if self.sock:
            self.close()

    def is_disconnect(self) -> bool:
        return self.sock is None


class Connection(ConnectionBase, ConnectionResponseMixin):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ConnectionBase.__init__(self, *args, **kwargs)
