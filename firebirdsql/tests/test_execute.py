from firebirdsql.tests.base import *    # noqa
from firebirdsql.fbcore import PreparedStatement
from firebirdsql.err import DatabaseError, IntegrityError


class TestExecute(TestBase):
    """execute() sends the execute, the first fetch and the row count request
    together; errors must still surface and leave the connection usable."""

    def setUp(self):
        TestBase.setUp(self)
        cur = self.connection.cursor()
        cur.execute("CREATE TABLE exec_t (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(20))")
        self.connection.commit()
        cur.execute("INSERT INTO exec_t VALUES (1, 'a')")
        cur.execute("INSERT INTO exec_t VALUES (2, 'b')")
        cur.execute("INSERT INTO exec_t VALUES (3, 'c')")
        self.connection.commit()
        cur.close()

    def _alive(self):
        cur = self.connection.cursor()
        cur.execute("SELECT COUNT(*) FROM exec_t")
        n = cur.fetchone()[0]
        cur.close()
        return n

    def test_rowcount(self):
        cur = self.connection.cursor()
        cur.execute("INSERT INTO exec_t VALUES (4, 'd')")
        self.assertEqual(cur.rowcount, 1)
        cur.execute("UPDATE exec_t SET name = 'x' WHERE id > 1")
        self.assertEqual(cur.rowcount, 3)
        cur.execute("DELETE FROM exec_t WHERE id = 4")
        self.assertEqual(cur.rowcount, 1)
        cur.execute("UPDATE exec_t SET name = 'y' WHERE id = 99")
        self.assertEqual(cur.rowcount, 0)
        cur.execute("SELECT id FROM exec_t ORDER BY id")
        self.assertEqual(cur.fetchall(), [(1,), (2,), (3,)])
        cur.close()

    def test_prepared_rowcount(self):
        cur = self.connection.cursor()
        cur.execute("SELECT id FROM exec_t")    # the cursor's own statement differs
        ps = PreparedStatement(cur, "UPDATE exec_t SET name = ? WHERE id <= ?")
        cur.execute(ps, ('p', 2))
        self.assertEqual(cur.rowcount, 2)
        cur.execute(ps, ('q', 3))
        self.assertEqual(cur.rowcount, 3)
        cur.close()

    def test_execute_error(self):
        cur = self.connection.cursor()
        with self.assertRaises(IntegrityError):
            cur.execute("INSERT INTO exec_t VALUES (1, 'duplicate')")
        self.assertEqual(self._alive(), 3)
        with self.assertRaises(DatabaseError):
            cur.execute("SELECT 1 / 0 FROM exec_t")
            cur.fetchall()
        self.assertEqual(self._alive(), 3)
        cur.execute("SELECT name FROM exec_t WHERE id = ?", (2,))
        self.assertEqual(cur.fetchall(), [('b',)])
        cur.close()

    def test_reexecute_prepared_select(self):
        cur = self.connection.cursor()
        ps = PreparedStatement(cur, "SELECT id FROM exec_t WHERE id <= ? ORDER BY id")
        cur.execute(ps, (1,))
        self.assertEqual(cur.fetchall(), [(1,)])
        cur.execute(ps, (2,))       # all rows read before
        self.assertEqual(cur.fetchall(), [(1,), (2,)])
        cur.execute(ps, (3,))
        self.assertEqual(cur.fetchone(), (1,))
        cur.execute(ps, (3,))       # only one row read before
        self.assertEqual(cur.fetchall(), [(1,), (2,), (3,)])
        self.connection.commit()    # closes the cursor on the server
        cur.execute(ps, (2,))
        self.assertEqual(cur.fetchall(), [(1,), (2,)])
        self.connection.rollback()
        cur.execute(ps, (1,))
        self.assertEqual(cur.fetchall(), [(1,)])
        self.assertEqual(self._alive(), 3)
        cur.close()

    def test_executemany(self):
        cur = self.connection.cursor()
        rows = [(10 + i, 'm%d' % i) for i in range(50)]
        cur.executemany("INSERT INTO exec_t VALUES (?, ?)", rows)
        self.assertEqual(cur.rowcount, 50)
        cur.execute("SELECT COUNT(*) FROM exec_t WHERE id >= 10")
        self.assertEqual(cur.fetchone()[0], 50)
        # an error part way: the rows before it are inserted, the connection stays usable
        with self.assertRaises(IntegrityError):
            cur.executemany("INSERT INTO exec_t VALUES (?, ?)", [(100, 'x'), (1, 'duplicate')])
        cur.execute("SELECT COUNT(*) FROM exec_t WHERE id = 100")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.close()
