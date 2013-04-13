import sys
import os
import time
import unittest
import tempfile
import firebirdsql
from firebirdsql.tests import base

class TestEvent(base.TestBase):
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
        self.connection.commit()

    def _produce(self):
        self.connection.cursor().execute('insert into test_table values (1)')
        self.connection.commit()

    def _handle_event(self):
        conduit = self.connection.event_conduit(['event_a', 'event_b', 'event_d'])
    
        result = conduit.wait(timeout=1)
        assert result == {'event_b': 0, 'event_a': 0, 'event_d': 0}
    
        result = conduit.wait()
        assert result == {'event_b': 1, 'event_a': 2, 'event_d': 0}
    
        self._produce()
    
        while result == {'event_b': 0, 'event_a': 0, 'event_d': 0}:
            result = conduit.wait()
        assert result == {'event_b': 1, 'event_a': 2, 'event_d': 0}
    
        conduit.close()

    def test_event(self):
        pid = os.fork()
        if pid == 0:
            self._handle_event()
        else:
            time.sleep(3)
            self._produce()
