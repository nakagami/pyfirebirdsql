import os
import tempfile
import firebirdsql
from firebirdsql.tests.base import *    # noqa


def _test_callback(s):
    """
    It is not need in backup_database() and restore_database() parameter.
    """
    # print(s)
    return


class TestBackup(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        cur = self.connection.cursor()

        cur.execute(
            "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database"
        )
        self.server_version = tuple(
            [int(n) for n in cur.fetchone()[0].split('.')]
        )

    def test_backup(self):
        """
        backup & restore
        """
        if self.server_version[0] < 3:
            return

        BACKUP_FILE = self.database + '.fbk'
        RESTORE_DATABASE = tempfile.mktemp()

        # backup
        svc = firebirdsql.services.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            user=self.user,
            password=self.password)
        svc.backup_database(self.database,
                            BACKUP_FILE,
                            callback=_test_callback)
        svc.close()
        self.assertEqual(True, os.access(BACKUP_FILE, os.F_OK))

        # restore
        svc = firebirdsql.services.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            user=self.user,
            password=self.password)
        svc.restore_database(
            BACKUP_FILE,
            RESTORE_DATABASE,
            replace=True,
            pageSize=4096,
            callback=_test_callback
        )
        svc.close()
        self.assertEqual(True, os.access(RESTORE_DATABASE, os.F_OK))

        # drop database
        conn = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            database=RESTORE_DATABASE,
            port=self.port,
            user=self.user,
            password=self.password)

        conn.drop_database()
        conn.close()
        self.assertEqual(False, os.access(RESTORE_DATABASE, os.F_OK))
