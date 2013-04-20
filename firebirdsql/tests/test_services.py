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

    def test_services(self):
        conn = self.connection

        svc = firebirdsql.services.connect(
            host=self.host,
            user=self.user,
            password=self.password)
        self.assertNotEqual(None, svc.getServiceManagerVersion())
        self.assertNotEqual(None, svc.getServerVersion())
        self.assertNotEqual(None, svc.getArchitecture())
        self.assertNotEqual(None, svc.getHomeDir())
        self.assertNotEqual(None, svc.getSecurityDatabasePath())
        self.assertNotEqual(None, svc.getLockFileDir())
        self.assertNotEqual(None, svc.getCapabilityMask())
        self.assertNotEqual(None, svc.getMessageFileDir())
        self.assertNotEqual(None, svc.getConnectionCount())
        self.assertNotEqual(None, svc.getAttachedDatabaseNames())
        self.assertNotEqual(None, svc.getLog())
        self.assertNotEqual(None, svc.getStatistics(self.database))
    
        svc.close()
    
