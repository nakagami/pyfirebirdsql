import sys
import unittest
import tempfile
import firebirdsql
from firebirdsql.tests import base

class TestServices(base.TestBase):
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
                h BLOB SUB_TYPE 0, 
                i DOUBLE PRECISION DEFAULT 0.0,
                j FLOAT DEFAULT 0.0,
                PRIMARY KEY (a),
                CONSTRAINT CHECK_A CHECK (a <> 0)
            )
        ''')
        self.connection.commit()

    def test_services(self):
        conn = self.connection

        svc = firebirdsql.services.connect(
            host=self.host,
            user=self.user,
            password=self.password)
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
    
        print('getLog()')
        print(svc.getLog())
    
        print('getStatistics()')
        print(svc.getStatistics(self.database))
    
        svc.close()
    
