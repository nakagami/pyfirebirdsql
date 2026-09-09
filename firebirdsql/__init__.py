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
import datetime
import decimal
from firebirdsql.consts import *    # noqa
from firebirdsql.fbcore import Connection
import firebirdsql.services
from firebirdsql.err import (
    Warning, Error, InterfaceError, DatabaseError, DisconnectByPeer, InternalError,
    OperationalError, ProgrammingError, IntegrityError, DataError, NotSupportedError
)


from firebirdsql import aio

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

    def __eq__(self, other):
        if isinstance(other, DBAPITypeObject):
            return self.values == other.values
        return other in self.values

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self.values)

    def __repr__(self):
        return f"<DBAPITypeObject {self.values}>"


STRING = DBAPITypeObject(
    str,
    SQL_TYPE_TEXT, SQL_TYPE_TEXT + 1,
    SQL_TYPE_VARYING, SQL_TYPE_VARYING + 1,
)
BINARY = DBAPITypeObject(
    bytes, bytearray, memoryview,
    SQL_TYPE_BLOB, SQL_TYPE_BLOB + 1,
    SQL_TYPE_ARRAY, SQL_TYPE_ARRAY + 1,
    SQL_TYPE_QUAD, SQL_TYPE_QUAD + 1,
)
NUMBER = DBAPITypeObject(
    int, float, decimal.Decimal,
    SQL_TYPE_SHORT, SQL_TYPE_SHORT + 1,
    SQL_TYPE_LONG, SQL_TYPE_LONG + 1,
    SQL_TYPE_INT64, SQL_TYPE_INT64 + 1,
    SQL_TYPE_INT128, SQL_TYPE_INT128 + 1,
    SQL_TYPE_FLOAT, SQL_TYPE_FLOAT + 1,
    SQL_TYPE_DOUBLE, SQL_TYPE_DOUBLE + 1,
    SQL_TYPE_D_FLOAT, SQL_TYPE_D_FLOAT + 1,
    SQL_TYPE_DEC_FIXED, SQL_TYPE_DEC_FIXED + 1,
    SQL_TYPE_DEC64, SQL_TYPE_DEC64 + 1,
    SQL_TYPE_DEC128, SQL_TYPE_DEC128 + 1,
    SQL_TYPE_BOOLEAN, SQL_TYPE_BOOLEAN + 1,
)
DATE = DBAPITypeObject(
    datetime.date,
    SQL_TYPE_DATE, SQL_TYPE_DATE + 1,
)
DATETIME = DBAPITypeObject(
    datetime.datetime, datetime.date, datetime.time,
    SQL_TYPE_DATE, SQL_TYPE_DATE + 1,
    SQL_TYPE_TIME, SQL_TYPE_TIME + 1,
    SQL_TYPE_TIME_TZ, SQL_TYPE_TIME_TZ + 1,
    SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP + 1,
    SQL_TYPE_TIMESTAMP_TZ, SQL_TYPE_TIMESTAMP_TZ + 1,
)
TIMESTAMP = DBAPITypeObject(
    datetime.datetime,
    SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP + 1,
    SQL_TYPE_TIMESTAMP_TZ, SQL_TYPE_TIMESTAMP_TZ + 1,
)
TIME = DBAPITypeObject(
    datetime.time,
    SQL_TYPE_TIME, SQL_TYPE_TIME + 1,
    SQL_TYPE_TIME_TZ, SQL_TYPE_TIME_TZ + 1,
)
ROWID = DBAPITypeObject()


__version__ = '1.4.6'
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'


def connect(*args, **kwargs):
    conn = Connection(*args, **kwargs)
    conn._initialize()
    return conn


def create_database(*args, **kwargs):
    kwargs['create_new'] = True
    return connect(*args, **kwargs)
