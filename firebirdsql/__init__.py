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

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


STRING = DBAPITypeObject(str)
BINARY = DBAPITypeObject(bytes)
NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATE = DBAPITypeObject(datetime.date)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
TIMESTAMP = DBAPITypeObject(datetime.datetime)
TIME = DBAPITypeObject(datetime.time)
ROWID = DBAPITypeObject()


__version__ = '1.4.1'
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
