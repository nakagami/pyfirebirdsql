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
from firebirdsql import *

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
    TEST_USER = 'sysdba'
    TEST_PASS = 'masterkey'

    print('dsn=', TEST_DSN)

    conn = connect(host=TEST_HOST, database=TEST_DATABASE,
                        port=TEST_PORT, user=TEST_USER, password=TEST_PASS)
    conn.begin()

    print('isc_info_ods_version =', conn.db_info(isc_info_ods_version))
    print('isc_info_base_level =', conn.db_info(isc_info_base_level))
    print('isc_info_db_id =', conn.db_info(isc_info_db_id))
    print('isc_info_implementation =', conn.db_info(isc_info_implementation))
    print('isc_info_firebird_version =', conn.db_info(isc_info_firebird_version))
    print('isc_info_user_names =', conn.db_info(isc_info_user_names))
    print('isc_info_reqd_idx_count =', conn.db_info(isc_info_read_idx_count))

    requests = [isc_info_ods_version, 
                isc_info_user_names,
                isc_info_ods_minor_version,
    ]
    print(conn.db_info(requests))

    conn.close()
