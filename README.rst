================
PyfirebirdSQL
================

.. image:: https://travis-ci.org/nakagami/pyfirebirdsql.svg?branch=master
   :target: https://travis-ci.org/nakagami/pyfirebirdsql
    
.. image:: https://img.shields.io/pypi/v/firebirdsql.png
   :target: https://pypi.python.org/pypi/firebirdsql

.. image:: https://img.shields.io/pypi/l/firebirdsql.png

firebirdsql package is a set of Firebird RDBMS (https://firebirdsql.org/) bindings for Python (Written in pure Python : no C compiler needed or fbclient library)

It works on Python 2.7+ and 3.5+.

see https://pyfirebirdsql.readthedocs.io/en/latest/


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

Test with Firebird3
----------------------

You can unit test with Firebird3.

Set firebird.conf like this ::

   AuthServer = Srp, Legacy_Auth
   WireCrypt = Enabled

and execute bellow command:

   $ python setup.py test

On the other hand, you can use it with Firebird3 wire protocol 13
(Srp authentication and wire encryption) with default firebird.conf
