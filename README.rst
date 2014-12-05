================
PyfirebirdSQL
================

firebirdsql package is a set of Firebird RDBMS bindings for Python (Written in pure Python : no C compiler needed or fbclient library) 

It works on Python 2.6+ (including Python 3.x)

see https://pyfirebirdsql.readthedocs.org/en/latest/

-----------
Example
-----------
::

   import firebirdsql
   conn = firebirdsql.connect(dsn='localhost:3050/foo/bar.fdb',
                               user='alice',
                               password='secret')
   cur = conn.cursor()
   cur.execute("select * from baz")
   for c in cur.fetchall():
       print(c)
       conn.close()
