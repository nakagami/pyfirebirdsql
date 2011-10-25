#!/usr/bin/env python
##############################################################################
# Copyright (c) 2009-2011 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import os,sys
sys.path.append('./../')
import firebirdsql

def debug_print(msg):
    print(msg)

if __name__ == '__main__':
    if sys.platform in ('win32', 'darwin'):
        fbase = os.path.abspath('.') + '/test'
    else:
        import tempfile
        fbase = tempfile.mktemp()
    TEST_HOST = 'localhost'
    TEST_PORT = 3050
    TEST_DATABASE = fbase + '.fdb'
    TEST_DSN = TEST_HOST + '/' + str(TEST_PORT) + ':' + TEST_DATABASE
    TEST_BACKUP_FILE = fbase + '.fbk'
    TEST_RESTORE_DATABASE = fbase + '_restore.fdb'
    print('dsn=', TEST_DSN)
    TEST_USER = 'sysdba'
    TEST_PASS = 'masterkey'

    conn = firebirdsql.create_database(dsn=TEST_DSN, user=TEST_USER, password=TEST_PASS)
    conn.cursor().execute('''
        CREATE TABLE foo (
            a INTEGER NOT NULL,
            b VARCHAR(30) NOT NULL UNIQUE,
            c VARCHAR(1024),
            d DECIMAL(16,3) DEFAULT -0.123,
            e DATE DEFAULT '1967-08-11',
            f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
            g TIME DEFAULT '23:45:01',
            h BLOB SUB_TYPE 0, 
            i DOUBLE PRECISION DEFAULT 0.0,
            j FLOAT DEFAULT 0.0,
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )
    ''')
    conn.commit()

    print('backup database')    
    svc = firebirdsql.services.connect(host=TEST_HOST, user=TEST_USER, 
        password=TEST_PASS)
    svc.backup_database(TEST_DATABASE, TEST_BACKUP_FILE, callback=debug_print)
    svc.close()
    print('restore database')    
    svc = firebirdsql.services.connect(host=TEST_HOST, user=TEST_USER, password=TEST_PASS)
    svc.restore_database(TEST_BACKUP_FILE, TEST_RESTORE_DATABASE, callback=debug_print)
    svc.close()

    svc = firebirdsql.services.connect(host=TEST_HOST, user=TEST_USER, password=TEST_PASS)
    print('getServiceManagerVersion()')
    print(svc.getServiceManagerVersion())

    print('getServerVersion()')
    print(svc.getServerVersion())

    print('getArchitecture()')
    print(svc.getArchitecture())

    print('getHomeDir()')
    print(svc.getHomeDir())

    print('getSecurityDatabasePath()')
    print(svc.getSecurityDatabasePath())

    print('getLockFileDir()')
    print(svc.getLockFileDir())

    print('getCapabilityMask()')
    print(svc.getCapabilityMask())

    print('getMessageFileDir()')
    print(svc.getMessageFileDir())

    print('getConnectionCount()')
    print(svc.getConnectionCount())

    print('getAttachedDatabaseNames()')
    print(svc.getAttachedDatabaseNames())

#    print('getLog()')
#    print(svc.getLog())

    print('getStatistics()')
    print(svc.getStatistics(TEST_DATABASE))

    svc.close()
