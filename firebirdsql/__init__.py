##############################################################################
# Copyright (c) 2009-2016, Hajime Nakagami<nakagami@gmail.com>
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


class Error(Exception):
    def __init__(self, message, gds_codes=set(), sql_code=0):
        self._message = message
        self.gds_codes = gds_codes
        self.sql_code = sql_code
        self.args = [message, sql_code]

    def __repr__(self):
        return "%d:%s" % (self.sql_code, self._message)

    def __str__(self):
        return self._message


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DisconnectByPeer(Warning):
    pass


class InternalError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'InternalError')


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'NotSupportedError')


from firebirdsql.fbcore import (
    __version__, apilevel, threadsafety,
    paramstyle, Transaction, Cursor, Connection,
    ISOLATION_LEVEL_READ_COMMITED_LEGACY, ISOLATION_LEVEL_READ_COMMITED,
    ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SNAPSHOT,
    ISOLATION_LEVEL_SERIALIZABLE, ISOLATION_LEVEL_READ_COMMITED_RO,
    BINARY, Binary, DATE, Date,
    Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks,
)
from firebirdsql.consts import *
import firebirdsql.services


def connect(**kwargs):
    return Connection(**kwargs)


def create_database(**kwargs):
    kwargs['create_new'] = True
    return Connection(**kwargs)
