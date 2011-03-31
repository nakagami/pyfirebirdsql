#!/usr/bin/env python
##############################################################################
# Copyright (c) 2009,2010 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import os,sys
sys.path.append('./../')
import firebirdsql

if __name__ == '__main__':
    if sys.platform in ('win32', 'darwin'):
        fbase = os.path.abspath('.') + '/test'
    else:
        import tempfile
        fbase = tempfile.mktemp()
    TEST_DSN = 'localhost:' + fbase + '.fdb'
    TEST_BACKUP_FILE = fbase + '.fbk'
    TEST_RESTORE_DSN = 'localhost:' + fbase + '_restore.fdb'
    print('dsn=', TEST_DSN)
    TEST_USER = 'sysdba'
    TEST_PASS = 'masterkey'
    conn = firebirdsql.create_database(TEST_DSN, TEST_USER, TEST_PASS, port=3050)
    print(conn.info_database(['isc_info_ods_version', 
                            'isc_info_ods_minor_version',
                            'isc_info_user_names']))
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
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )
    ''')
    conn.commit()
    conn.cursor().execute("insert into foo(a, b, c) values (1, 'a', 'b')")
    conn.cursor().execute("""insert into foo(a, b, c, e, g, i) 
        values (2, 'A', 'B', '1999-01-25', '00:00:01', 0.1)""")
    conn.cursor().execute("""insert into foo(a, b, c, e, g, i) 
        values (3, 'X', 'Y', '2001-07-05', '00:01:02', 0.2)""")
    conn.commit()
    conn.cursor().execute("update foo set c='Hajime' where a=1")
    conn.cursor().execute("update foo set c=? where a=2", ['Nakagami'])
    conn.commit()

    cur = conn.cursor()
    try:
        conn.cursor().execute("insert into foo(a, b, c) values (1, 'a', 'b')")
    except firebirdsql.IntegrityError:
        pass
    try:
        conn.cursor().execute("bad sql")
    except firebirdsql.OperationalError as e:
        assert e.sql_code == -104

    cur = conn.cursor()
    cur.execute("select * from foo")
    print(cur.description)
    for c in cur.fetchall():
        print(c)
    conn.close()

    conn = firebirdsql.connect(TEST_DSN, TEST_USER, TEST_PASS, port=3050)
    conn.set_isolation_level(firebirdsql.ISOLATION_LEVEL_SERIALIZABLE)
    cur = conn.cursor()
    cur.execute("select * from foo")
    print(cur.description)
    for c in cur.fetchall():
        print(c)
    conn.close()

    print('backup database')    
    svc = firebirdsql.service_mgr(TEST_DSN, TEST_USER, TEST_PASS, port=3050)
    svc.backup_database(TEST_BACKUP_FILE, sys.stdout)
    svc.close()
    print('restore database')    
    svc = firebirdsql.service_mgr(TEST_RESTORE_DSN, TEST_USER, TEST_PASS, port=3050)
    svc.restore_database(TEST_BACKUP_FILE, sys.stdout)
    svc.close()
