##############################################################################
# Copyright (c) 2009,2010 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################

class Error(Exception):
    def __init__(self, message, gds_codes, sql_code=0):
        self.message = message
        self.gds_codes = gds_codes
        self.sql_code = sql_code
    def __str__(self):
        return self.message

class Warning(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
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
        NotSupportedError.__init__(self, 'NotSupportedError')

from firebirdsql.fbcore import ( __version__, apilevel, threadsafety, 
    paramstyle, cursor, connect, create_database, service_mgr,
    ISOLATION_LEVEL_READ_UNCOMMITTED, ISOLATION_LEVEL_READ_COMMITED,
    ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SERIALIZABLE,
)
