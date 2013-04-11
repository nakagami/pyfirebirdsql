import os
import sys
import firebirdsql
import unittest

class TestBasic(unittest.TestCase):
    def setUp(self):
        if sys.platform in ('win32', 'darwin'):
            fbase = os.path.abspath('.') + '/test_basic'
        else:
            import tempfile
            fbase = tempfile.mktemp()
        TEST_HOST = 'localhost'
        TEST_PORT = 3050
        TEST_DATABASE = fbase + '.fdb'
        TEST_DSN = TEST_HOST + '/' + str(TEST_PORT) + ':' + TEST_DATABASE
        TEST_USER = 'sysdba'
        TEST_PASS = 'masterkey'
        self.connection = firebirdsql.create_database(dsn=TEST_DSN,
                            user=TEST_USER, password=TEST_PASS, page_size=2<<13)
        cur = self.connection.cursor()
        cur.execute('''
            CREATE TABLE foo (
                a INTEGER NOT NULL,
                b VARCHAR(30) NOT NULL UNIQUE,
                c VARCHAR(1024),
                d DECIMAL(16,3) DEFAULT -0.123,
                e DATE DEFAULT '1967-08-11',
                f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
                g TIME DEFAULT '23:45:01',
                h BLOB SUB_TYPE 1, 
                i DOUBLE PRECISION DEFAULT 0.0,
                j FLOAT DEFAULT 0.0,
                PRIMARY KEY (a),
                CONSTRAINT CHECK_A CHECK (a <> 0)
            )
        ''')
        cur.execute('''
            CREATE TABLE bar_empty (
                k INTEGER NOT NULL,
                abcdefghijklmnopqrstuvwxyz INTEGER
            )
        ''')
        cur.execute('''
            CREATE PROCEDURE foo_proc
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                out1 = 1;
                out2 = 'ABC';
              END
        ''')
        cur.execute('''
            CREATE PROCEDURE bar_proc (param_a INTEGER, param_b VARCHAR(30))
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                out1 = param_a;
                out2 = param_b;
              END
        ''')
        cur.execute('''
            CREATE PROCEDURE baz_proc(param_a INTEGER)
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                SELECT a, b FROM foo
                  WHERE a= :param_a
                  INTO :out1, :out2;
                SUSPEND;
              END
        ''')
        self.connection.commit()


    def tearDown(self):
        self.connection.close()
