import sys
import os
import time
import unittest
import tempfile
import firebirdsql
from firebirdsql.tests import base
from firebirdsql.consts import PYTHON_MAJOR_VER
if PYTHON_MAJOR_VER == 3:
    import _thread as thread
else:
    import thread

class TestEvent(base.TestBase):
    def setUp(self):
        self.database=tempfile.mktemp()
        conn = firebirdsql.create_database(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                page_size=self.page_size)

        cur = conn.cursor()

        cur.execute('CREATE TABLE test_table (a integer)')
        cur.execute('''
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
        conn.close()

    def tearDown(self):
        pass

    def _produce(self):
        conn = firebirdsql.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password)
        cur = conn.cursor()
        cur.execute('insert into test_table values (1)')
        conn.commit()
        conn.close()

    def _handle_event(self):
        conn = firebirdsql.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password)
        conduit = conn.event_conduit(['event_a', 'event_b', 'event_d'], timeout=1)
        result = conduit.wait()
        assert result == {'event_b': 0, 'event_a': 0, 'event_d': 0}
    
        result = conduit.wait()
        assert result == {'event_b': 1, 'event_a': 2, 'event_d': 0}
    
        self._produce()
    
        while result == {'event_b': 0, 'event_a': 0, 'event_d': 0}:
            result = conduit.wait()
        assert result == {'event_b': 1, 'event_a': 2, 'event_d': 0}
    
        conduit.close()
        conn.close()

    def test_event(self):
        thread.start_new_thread(self._handle_event, ())
        time.sleep(3)
        self._produce()
