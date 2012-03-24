#!/usr/bin/env python
##############################################################################
# Copyright (c) 2012 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import os,sys
sys.path.append('./../')
import kinterbasdb

TEST_HOST = 'localhost'
TEST_PORT = 3050
TEST_DATABASE = '/tmp/test_event.fdb'
TEST_DSN = TEST_HOST + '/' + str(TEST_PORT) + ':' + TEST_DATABASE
TEST_USER = 'sysdba'
TEST_PASS = 'masterkey'

def create():
    conn = kinterbasdb.create_database(
        "create database '%s' user '%s' password '%s' " % (
                                        TEST_DATABASE, TEST_USER, TEST_PASS))

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
    conn = kinterbasdb.connect(dsn=TEST_DSN, user=TEST_USER, password=TEST_PASS)
    conduit = conn.event_conduit(['event_a', 'event_b', 'event_d'])
    result = conduit.wait()
    print('HANDLER: An event notification has arrived:')
    print(result)
    conduit.close()

def producer():
    conn = kinterbasdb.connect(dsn=TEST_DSN, user=TEST_USER, password=TEST_PASS)
    conn.cursor().execute('insert into test_table values (1)')
    conn.commit()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(sys.argv[0] + ' create|handler|producer')
        sys.exit()
    if sys.argv[1] == 'create':
        create()
    elif sys.argv[1] == 'handler':
        handler()
    elif sys.argv[1] == 'producer':
        producer()
    else:
        print('Bad argument')
