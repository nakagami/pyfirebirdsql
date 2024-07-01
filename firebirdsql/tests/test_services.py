import unittest
import tempfile
import firebirdsql
from firebirdsql.tests.base import *    # noqa


class TestServices(TestBase):
    def setUp(self):
        self.database = tempfile.mktemp()
        conn = firebirdsql.create_database(
                auth_plugin_name=self.auth_plugin_name,
                wire_crypt=self.wire_crypt,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)
        conn.close()

    def tearDown(self):
        pass

    def test_services(self):
        svc = firebirdsql.services.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
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

    def shutdown(self):
        svc = firebirdsql.services.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            user=self.user,
            password=self.password)
        svc.shutdown(self.database, 0)
        svc.close()

    def bringOnline(self):
        svc = firebirdsql.services.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            user=self.user,
            password=self.password)
        svc.bringOnline(self.database)
        svc.close()
