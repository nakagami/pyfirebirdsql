================
PyfirebirdSQL
================

.. image:: https://travis-ci.org/nakagami/pyfirebirdsql.svg?branch=master
   :target: https://travis-ci.org/nakagami/pyfirebirdsql
    
.. image:: https://img.shields.io/pypi/v/firebirdsql.png
   :target: https://pypi.python.org/pypi/firebirdsql

.. image:: https://img.shields.io/pypi/dm/firebirdsql.png
   :target: https://pypi.python.org/pypi/firebirdsql

.. image:: https://img.shields.io/pypi/l/firebirdsql.png

firebirdsql package is a set of Firebird RDBMS bindings for Python (Written in pure Python : no C compiler needed or fbclient library) 

It works on Python 2.6+ (including Python 3.x)

see https://pyfirebirdsql.readthedocs.org/en/latest/


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

You can use it with Firebird3 wire protocol 13 (Srp)
----------------------

firebird.conf::

   AuthServer = Srp, Legacy_Auth
   WireCrypt = Enabled

