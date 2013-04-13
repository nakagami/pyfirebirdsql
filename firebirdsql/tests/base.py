import firebirdsql
import unittest
import tempfile

class TestBase(unittest.TestCase):
    host='localhost'
    port=3050
    user='sysdba'
    password='masterkey'
    page_size=2<<13

    def setUp(self):
        self.database=tempfile.mktemp()
        self.connection = firebirdsql.create_database(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)

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
