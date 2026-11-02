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

    def _services(self):
        return firebirdsql.services.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password)

    def test_verbose_output(self):
        """The callback gets every output line of a verbose backup and restore."""
        if self.server_version[0] < 3:
            return
        cur = self.connection.cursor()
        for i in range(50):
            cur.execute("CREATE TABLE VERBOSE_OUTPUT_%d (ID INTEGER)" % i)
        self.connection.commit()

        backup_file = self.database + '.fbk'
        restored = tempfile.mktemp()
        svc = self._services()
        backup_lines = []
        svc.backup_database(self.database, backup_file, callback=backup_lines.append)
        restore_lines = []
        svc.restore_database(backup_file, restored, replace=True, callback=restore_lines.append)
        svc.close()

        for lines in (backup_lines, restore_lines):
            self.assertGreater(len(lines), 50)
            self.assertTrue(all(isinstance(line, str) and '\n' not in line for line in lines))
            self.assertTrue(all(line.startswith('gbak:') for line in lines if line))
            for i in range(50):
                self.assertTrue(any(line.endswith('VERBOSE_OUTPUT_%d' % i) for line in lines), i)

        conn = firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            host=self.host,
            database=restored,
            port=self.port,
            user=self.user,
            password=self.password)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME STARTING WITH 'VERBOSE_OUTPUT_'")
        self.assertEqual(cur.fetchone()[0], 50)
        conn.drop_database()
        conn.close()

    def test_get_log(self):
        svc = self._services()
        log = svc.getLog()
        svc.close()
        self.assertIsInstance(log, str)
        self.assertTrue(log == '' or log.endswith('\n'))
