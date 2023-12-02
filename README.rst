================
PyfirebirdSQL
================

.. image:: https://img.shields.io/pypi/v/firebirdsql.png
   :target: https://pypi.python.org/pypi/firebirdsql

.. image:: https://img.shields.io/pypi/l/firebirdsql.png

firebirdsql package is a set of Firebird RDBMS (https://firebirdsql.org/) bindings for Python (Written in pure Python : no C compiler needed or fbclient library)


see https://pyfirebirdsql.readthedocs.io/en/latest/

It works on Python 2.7 and 3.7+.

But if you want to use the timezone feature of Firebird 4.0 ...

- Not supported by python2.7
- Python 3.7, 3.8 requires backports.zoneinfo https://pypi.org/project/backports.zoneinfo/ install

Example
-----------

::

   import firebirdsql
   conn = firebirdsql.connect(
       host='localhost',
       database='/foo/bar.fdb',
       port=3050,
       user='alice',
       password='secret'
   )
   cur = conn.cursor()
   cur.execute("select * from baz")
   for c in cur.fetchall():
       print(c)
   conn.close()
