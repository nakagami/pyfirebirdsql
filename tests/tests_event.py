#!/usr/bin/env python
##############################################################################
# Copyright (c) 2012 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import os
import sys
import time
sys.path.append('./../')
from firebirdsql import *

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

def create():
    conn = create_database(dsn=TEST_DSN, user=TEST_USER, password=TEST_PASS)

    conn.cursor().execute('CREATE TABLE test_table (a integer)')
    conn.cursor().execute('''
        CREATE TRIGGER trig_test_insert_event
            FOR test_table
                after insert
        AS BEGIN
            post_event 'event_a';
            post_event 'event_b';
            post_event 'event_c';
            post_event 'event_a';
        END''')
    conn.commit()

def handler():
    conn = connect(dsn=TEST_DSN, user=TEST_USER, password=TEST_PASS)
    conduit = conn.event_conduit(['event_a', 'event_b', 'event_d'])
    result = conduit.wait()
    print('HANDLER: An event notification has arrived:')
    print(result)
    conduit.close()

def producer():
    conn = connect(dsn=TEST_DSN, user=TEST_USER, password=TEST_PASS)
    conn.cursor().execute('insert into test_table values (1)')
    conn.commit()

if __name__ == '__main__':
    create()
    pid = os.fork()
    if pid == 0:
        handler()
    else:
        time.sleep(1)
        producer()
