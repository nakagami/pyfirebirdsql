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
from firebirdsql import services

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
    print('dsn=', TEST_DSN)
    TEST_USER = 'sysdba'
    TEST_PASS = 'masterkey'

    svc = services.connect(host=TEST_HOST, user=TEST_USER, password=TEST_PASS)
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
    print(svc.getStatistics())

    svc.close()
