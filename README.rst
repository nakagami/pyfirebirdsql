================
PyfirebirdSQL
================

.. image:: https://img.shields.io/pypi/v/firebirdsql.png
   :target: https://pypi.python.org/pypi/firebirdsql

.. image:: https://img.shields.io/pypi/l/firebirdsql.png

firebirdsql package is a set of Firebird RDBMS (https://firebirdsql.org/) bindings for Python (Written in pure Python : no C compiler needed or fbclient library)

Sorry, it support Firebird 2.5+.

It works on Python 2.7 and 3.9+.
But Python 2.7 is not suported following features.

- TimeZone feature in Firebird 4.0+
- asyncio API

Example
-----------

Python Database API Specification v2.0
+++++++++++++++++++++++++++++++++++++++++

https://peps.python.org/pep-0249/
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


asyncio
++++++++++++++++++++++++++++++++++++++

::

   import asyncio
   import firebirdsql

   async def conn_example():
       conn = await firebirdsql.aio.connect(
           host='localhost',
           database='/foo/bar.fdb',
           port=3050,
           user='alice',
           password='secret'
       )
       cur = conn.cursor()
       await cur.execute("select * from baz")
       print(await cur.fetchall())
   asyncio.run(conn_example())

Use pool
::

   import asyncio
   import firebirdsql

   async def pool_example(loop):
       pool = await firebirdsql.aio.create_pool(
           host='localhost',
           database='/foo/bar.fdb',
           port=3050,
           user='alice',
           password='secret'
           loop=loop,
       )
       async with pool.acquire() as conn:
           async with conn.cursor() as cur:
               await cur.execute("select * from baz")
               print(await cur.fetchall())
       pool.close()
       await pool.wait_closed()

   loop = asyncio.get_event_loop()
   loop.run_until_complete(pool_example(loop))
   loop.close()
