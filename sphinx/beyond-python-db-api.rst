#######################################################################
Native Database Engine Features and Extensions Beyond the Python DB API
#######################################################################

.. currentmodule:: firebirdsql

Programmatic Database Creation and Deletion
===========================================

The Firebird engine stores a database in a fairly straightforward
manner: as a single file or, if desired, as a segmented group of
files.

The engine supports dynamic database creation via the SQL statement
`CREATE DATABASE`.

The engine also supports dropping (deleting) databases dynamically,
but dropping is a more complicated operation than creating, for
several reasons: an existing database may be in use by users other
than the one who requests the deletion, it may have supporting objects
such as temporary sort files, and it may even have dependent shadow
databases. Although the database engine recognizes a `DROP DATABASE`
SQL statement, support for that statement is limited to the `isql`
command-line administration utility. However, the engine supports the
deletion of databases via an API call, which pyfirebirdsql exposes to
Python (see below).

pyfirebirdsql supports dynamic database creation and deletion via the
module-level function :func:`firebirdsql.create_database` and the method
:meth:`~firebirdsql.Connection.drop_database`. These are documented below,
then demonstrated by a brief example.

.. function:: create_database()

   Creates a new database and returns an open connection to it.
   `create_database()` accepts the same positional and keyword arguments as
   :func:`firebirdsql.connect()`, then enables database creation internally.

   Typical arguments include `host`, `database`, `user`, `password`,
   `charset`, `page_size`, and other connection options accepted by
   :func:`firebirdsql.connect()`.

.. method:: Connection.drop_database()

   Deletes the database to which the connection is attached.

   This method performs the database deletion in a responsible fashion.
   Specifically, it:

   + raises an `OperationalError` instead of deleting the database if
     there are other active connections to the database
   + deletes supporting files and logs in addition to the primary
     database file(s)

   This method has no arguments.

   Example program:

   .. sourcecode:: python

      import firebirdsql

      con = firebirdsql.create_database(
          host='localhost', database='/temp/db.db',
          user='sysdba', password='pass'
      )
      con.drop_database()



Advanced Transaction Control
============================

For the sake of simplicity, pyfirebirdsql lets the Python programmer
ignore transaction management to the greatest extent allowed by the
Python Database API Specification 2.0. The specification says, "if the
database supports an auto-commit feature, this must be initially off".
At a minimum, therefore, it is necessary to call the `commit` method
of the connection in order to persist any changes made to the
database. Transactions left unresolved by the programmer will be
`rollback`ed when the connection is garbage collected.

Remember that because of `ACID 
<http://philip.greenspun.com/panda/databases-choosing#acid>`__,
every data manipulation operation in the Firebird database engine
takes place in the context of a transaction, including operations
that are conceptually "read-only", such as a typical `SELECT`.
The client programmer of pyfirebirdsql establishes a transaction
implicitly by using any SQL execution method, such as 
:meth:`~Connection.execute_immediate()`,
:meth:`Cursor.execute()`, or :meth:`Cursor.callproc()`.

Although pyfirebirdsql allows the programmer to pay little attention to
transactions, it also exposes the full complement of the database
engine's advanced transaction control features: transaction
parameters, retaining transactions, savepoints, and distributed
transactions.

Explicit transaction start
--------------------------

In addition to the implicit transaction initiation required by
Python Database API, pyfirebirdsql allows the programmer to
start transactions explicitly via the `Connection.begin` method.

.. method:: Connection.begin()

   Starts a transaction explicitly. This is never *required*; a
   transaction will be started implicitly if necessary.

Retaining Operations
--------------------

The `commit` and `rollback` methods of `firebirdsql.Connection` accept
an optional boolean parameter `retaining` (default `False`) to
indicate whether to recycle the transactional context of the
transaction being resolved by the method call.

If `retaining` is `True`, the infrastructural support for the
transaction active at the time of the method call will be "retained"
(efficiently and transparently recycled) after the database server has
committed or rolled back the conceptual transaction.

In code that commits or rolls back frequently, "retaining" the
transaction yields considerably better performance. However, retaining
transactions must be used cautiously because they can interfere with
the server's ability to garbage collect old record versions. For
details about this issue, read the "Garbage" section of `this document
<http://www.ibphoenix.com/main.nfs?a=ibphoenix&s=1123236035:18161&page
=ibp_expert4>`__ by Ann Harrison.

For more information about retaining transactions, see Firebird documentation.


Savepoints
----------

Firebird 1.5 introduced support for transaction savepoints. Savepoints
are named, intermediate control points within an open transaction that
can later be rolled back to, without affecting the preceding work.
Multiple savepoints can exist within a single unresolved transaction,
providing "multi-level undo" functionality.

Although Firebird savepoints are fully supported from SQL alone via
the `SAVEPOINT 'name'` and `ROLLBACK TO 'name'` statements,
pyfirebirdsql also exposes savepoints at the Python API level for the
sake of convenience. 

.. method::  Connection.savepoint(name)

   Establishes a savepoint with the specified `name`. To roll back to a
   specific savepoint, call the :meth:`~firebirdsql.Connection.rollback()`
   method and provide a value (the name of the savepoint) for the optional
   `savepoint` parameter. If the `savepoint` parameter of 
   :meth:`~firebirdsql.Connection.rollback()` is not specified, the active
   transaction is cancelled in its entirety, as required by the Python
   Database API Specification.

The following program demonstrates savepoint manipulation via the
pyfirebirdsql API, rather than raw SQL.

.. sourcecode:: python

    import firebirdsql

    con = firebirdsql.connect(dsn='localhost:/temp/test.db', user='sysdba', password='pass')
    cur = con.cursor()

    cur.execute("recreate table test_savepoints (a integer)")
    con.commit()

    print('Before the first savepoint, the contents of the table are:')
    cur.execute("select * from test_savepoints")
    print(' ', cur.fetchall())

    cur.execute("insert into test_savepoints values (?)", [1])
    con.savepoint('A')
    print('After savepoint A, the contents of the table are:')
    cur.execute("select * from test_savepoints")
    print(' ', cur.fetchall())

    cur.execute("insert into test_savepoints values (?)", [2])
    con.savepoint('B')
    print('After savepoint B, the contents of the table are:')
    cur.execute("select * from test_savepoints")
    print(' ', cur.fetchall())

    cur.execute("insert into test_savepoints values (?)", [3])
    con.savepoint('C')
    print('After savepoint C, the contents of the table are:')
    cur.execute("select * from test_savepoints")
    print(' ', cur.fetchall())

    con.rollback(savepoint='A')
    print('After rolling back to savepoint A, the contents of the table are:')
    cur.execute("select * from test_savepoints")
    print(' ', cur.fetchall())

    con.rollback()
    print('After rolling back entirely, the contents of the table are:')
    cur.execute("select * from test_savepoints")
    print(' ', cur.fetchall())


The output of the example program is shown below.

.. sourcecode:: python

    Before the first savepoint, the contents of the table are:
      []
    After savepoint A, the contents of the table are:
      [(1,)]
    After savepoint B, the contents of the table are:
      [(1,), (2,)]
    After savepoint C, the contents of the table are:
      [(1,), (2,), (3,)]
    After rolling back to savepoint A, the contents of the table are:
      [(1,)]
    After rolling back entirely, the contents of the table are:
      []


Prepared Statements
===================

When you define a Python function, the interpreter initially parses
the textual representation of the function and generates a binary
equivalent called bytecode. The bytecode representation can then be
executed directly by the Python interpreter any number of times and
with a variety of parameters, but the human-oriented textual
definition of the function never need be parsed again.

Database engines perform a similar series of steps when executing a
SQL statement. Consider the following series of statements:

.. sourcecode:: python

    cur.execute("insert into the_table (a,b,c) values ('aardvark', 1, 0.1)")
    ...
    cur.execute("insert into the_table (a,b,c) values ('zymurgy', 2147483647, 99999.999)")


If there are many statements in that series, wouldn't it make sense to
"define a function" to insert the provided "parameters" into the
predetermined fields of the predetermined table, instead of forcing
the database engine to parse each statement anew and figure out what
database entities the elements of the statement refer to? In other
words, why not take advantage of the fact that the form of the
statement ("the function") stays the same throughout, and only the
values ("the parameters") vary? Prepared statements deliver that
performance benefit and other advantages as well.

The following code is semantically equivalent to the series of insert
operations discussed previously, except that it uses a single SQL
statement that contains Firebird's parameter marker ( `?`) in the
slots where values are expected, then supplies those values as Python
tuples instead of constructing a textual representation of each value
and passing it to the database engine for parsing:

.. sourcecode:: python

    insertStatement = "insert into the_table (a,b,c) values (?,?,?)"
    cur.execute(insertStatement, ('aardvark', 1, 0.1))
    ...
    cur.execute(insertStatement, ('zymurgy', 2147483647, 99999.999))


Only the values change as each row is inserted; the statement remains
the same. For many years, pyfirebirdsql has recognized situations
similar to this one and automatically reused the same prepared
statement in each :meth:`Cursor.execute` call. In pyfirebirdsql 3.2, the
scheme for automatically reusing prepared statements has become more
sophisticated, and the API has been extended to offer the client
programmer manual control over prepared statement creation and use.

The entry point for manual statement preparation is the `Cursor.prep`
method.

.. method:: Cursor.prep(sql)

   :sql:  string parameter that contains the SQL statement to be prepared.
          Returns a :class:`PreparedStatement` instance.

.. class:: PreparedStatement

   `PreparedStatement` has no public methods, but does have the following
   public read-only properties:

   .. attribute:: sql

      A reference to the string that was passed to 
      :meth:`~Cursor.prep()` to create this `PreparedStatement`.

   .. attribute:: statement_type

      An integer code that can be matched against the statement type
      constants in the `firebirdsql.isc_info_sql_stmt_*` series.
      The following statement type codes are currently available: 

        + `isc_info_sql_stmt_commit`
        + `isc_info_sql_stmt_ddl`
        + `isc_info_sql_stmt_delete`
        + `isc_info_sql_stmt_exec_procedure`
        + `isc_info_sql_stmt_get_segment`
        + `isc_info_sql_stmt_insert`
        + `isc_info_sql_stmt_put_segment`
        + `isc_info_sql_stmt_rollback`
        + `isc_info_sql_stmt_savepoint`
        + `isc_info_sql_stmt_select`
        + `isc_info_sql_stmt_select_for_upd`
        + `isc_info_sql_stmt_set_generator`
        + `isc_info_sql_stmt_start_trans`
        + `isc_info_sql_stmt_update`

   .. attribute:: n_input_params

      The number of input parameters the statement requires.

   .. attribute:: n_output_params

      The number of output fields the statement produces.

   .. attribute:: plan

      A string representation of the execution plan generated for this
      statement by the database engine's optimizer. This property can
      be used, for example, to verify that a statement is using the
      expected index.

   .. attribute:: description

      A Python DB API 2.0 description sequence (of the same format as
      :attr:`Cursor.description`) that describes the statement's output
      parameters. Statements without output parameters have a `description`
      of `None`.

In addition to programmatically examining the characteristics of a SQL
statement via the properties of `PreparedStatement`, the client
programmer can submit a `PreparedStatement` to :meth:`Cursor.execute` or
:meth:`Cursor.executemany` for execution. The code snippet below is
semantically equivalent to both of the previous snippets in this
section, but it explicitly prepares the `INSERT` statement in advance,
then submits it to :meth:`Cursor.executemany` for execution:

.. sourcecode:: python

    insertStatement = cur.prep("insert into the_table (a,b,c) values (?,?,?)")
    inputRows = [
        ('aardvark', 1, 0.1),
        ...
        ('zymurgy', 2147483647, 99999.999)
      ]
    cur.executemany(insertStatement, inputRows)


**Example Program**

The following program demonstrates the explicit use of
PreparedStatements. It also benchmarks explicit `PreparedStatement`
reuse against pyfirebirdsql's automatic `PreparedStatement` reuse, and
against an input strategy that prevents `PreparedStatement` reuse.

.. sourcecode:: python

    import time
    import firebirdsql

    con = firebirdsql.connect(dsn=r'localhost:D:\temp\test-20.firebird',
        user='sysdba', password='masterkey'
      )

    cur = con.cursor()

    # Create supporting database entities:
    cur.execute("recreate table t (a int, b varchar(50))")
    con.commit()
    cur.execute("create unique index unique_t_a on t(a)")
    con.commit()

    # Explicitly prepare the insert statement:
    psIns = cur.prep("insert into t (a,b) values (?,?)")
    print('psIns.sql: "%s"' % psIns.sql)
    print(
        'psIns.statement_type == firebirdsql.isc_info_sql_stmt_insert:',
        psIns.statement_type == firebirdsql.isc_info_sql_stmt_insert
    )
    print('psIns.n_input_params: %d' % psIns.n_input_params)
    print('psIns.n_output_params: %d' % psIns.n_output_params)
    print('psIns.plan: %s' % psIns.plan)

    print()

    N = 10000
    iStart = 0

    # The client programmer uses a PreparedStatement explicitly:
    startTime = time.time()
    for i in range(iStart, iStart + N):
        cur.execute(psIns, (i, str(i)))
    print(
        'With explicit prepared statement, performed'
        '\n  %0.2f insertions per second.' % (N / (time.time() - startTime))
    )
    con.commit()

    iStart += N

    # pyfirebirdsql automatically uses a PreparedStatement "under the hood":
    startTime = time.time()
    for i in range(iStart, iStart + N):
        cur.execute("insert into t (a,b) values (?,?)", (i, str(i)))
    print(
        'With implicit prepared statement, performed'
        '\n  %0.2f insertions per second.' % (N / (time.time() - startTime))
    )
    con.commit()

    iStart += N

    # A new SQL string containing the inputs is submitted every time, so
    # pyfirebirdsql is not able to implicitly reuse a PreparedStatement.  Also, in a
    # more complicated scenario where the end user supplied the string input
    # values, the program would risk SQL injection attacks:
    startTime = time.time()
    for i in range(iStart, iStart + N):
        cur.execute("insert into t (a,b) values (%d,'%s')" % (i, str(i)))
    print(
        'When unable to reuse prepared statement, performed'
        '\n  %0.2f insertions per second.' % (N / (time.time() - startTime))
    )
    con.commit()

    # Prepare a SELECT statement and examine its properties.  The optimizer's plan
    # should use the unique index that we created at the beginning of this program.
    print()
    psSel = cur.prep("select * from t where a = ?")
    print('psSel.sql: "%s"' % psSel.sql)
    print(
        'psSel.statement_type == firebirdsql.isc_info_sql_stmt_select:',
        psSel.statement_type == firebirdsql.isc_info_sql_stmt_select
    )
    print('psSel.n_input_params: %d' % psSel.n_input_params)
    print('psSel.n_output_params: %d' % psSel.n_output_params)
    print('psSel.plan: %s' % psSel.plan)

    # The current implementation does not allow PreparedStatements to be prepared
    # on one Cursor and executed on another:
    print()
    print('Note that PreparedStatements are not transferrable from one cursor to another:')
    cur2 = con.cursor()
    cur2.execute(psSel)

Output:

.. sourcecode:: python

    psIns.sql: "insert into t (a,b) values (?,?)"
    psIns.statement_type == firebirdsql.isc_info_sql_stmt_insert: True
    psIns.n_input_params: 2
    psIns.n_output_params: 0
    psIns.plan: None

    With explicit prepared statement, performed
      9551.10 insertions per second.
    With implicit prepared statement, performed
      9407.34 insertions per second.
    When unable to reuse prepared statement, performed
      1882.53 insertions per second.

    psSel.sql: "select * from t where a = ?"
    psSel.statement_type == firebirdsql.isc_info_sql_stmt_select: True
    psSel.n_input_params: 1
    psSel.n_output_params: 2
    psSel.plan: PLAN (T INDEX (UNIQUE_T_A))

    Note that PreparedStatements are not transferrable from one cursor to another:
    Traceback (most recent call last):
      File "adv_prepared_statements__overall_example.py", line 86, in ?
        cur2.execute(psSel)
    firebirdsql.ProgrammingError: (0, 'A PreparedStatement can only be used with the
     Cursor that originally prepared it.')


As you can see, the version that prevents the reuse of prepared
statements is about five times slower -- *for a trivial statement*. In
a real application, SQL statements are likely to be far more
complicated, so the speed advantage of using prepared statements would
only increase.

As the timings indicate, pyfirebirdsql does a good job of reusing
prepared statements even if the client program is written in a style
strictly compatible with the Python DB API 2.0 (which accepts only
strings -- not :class:`PreparedStatement` objects -- to the :meth:`Cursor.execute()`
method). The performance loss in this case is less than one percent.



Named Cursors
=============

To allow the Python programmer to perform scrolling `UPDATE` or
`DELETE` via the "`SELECT ... FOR UPDATE`" syntax, pyfirebirdsql
provides the read/write property `Cursor.name`.

.. attribute:: Cursor.name

   Name for the SQL cursor. This property can be ignored entirely
   if you don't need to use it.

**Example Program**

.. sourcecode:: python

    import firebirdsql

    con = firebirdsql.connect(dsn='localhost:/temp/test.db', user='sysdba', password='pass')
    curScroll = con.cursor()
    curUpdate = con.cursor()

    curScroll.execute("select city from addresses for update")
    curScroll.name = 'city_scroller'
    update = "update addresses set city=? where current of " + curScroll.name

    for (city,) in curScroll:
        city = ... # make some changes to city
        curUpdate.execute( update, (city,) )

    con.commit()



.. _blob-conversion:

Blobs
=====

BLOB fields are returned as Python `bytes` objects.

Use ordinary parameter binding to insert BLOB data:

.. sourcecode:: python

   import firebirdsql

   con = firebirdsql.connect(dsn='localhost:/temp/test.db', user='sysdba', password='pass')
   cur = con.cursor()

   cur.execute("recreate table blob_test (a blob)")
   con.commit()

   cur.execute("insert into blob_test values (?)", (b'abcdef',))
   cur.execute("select a from blob_test")
   blob_value = cur.fetchone()[0]

   print(blob_value)

.. _connection-timeout:

Connection Timeouts
===================

The `timeout` parameter to :func:`firebirdsql.connect()` accepts a float
value representing the socket timeout in seconds. When set, operations
that take longer than this time will raise a timeout error.

.. sourcecode:: python

   import firebirdsql

   con = firebirdsql.connect(
       dsn='localhost:/temp/test.db',
       user='sysdba', password='masterkey',
       timeout=30.0  # socket timeout of 30 seconds
   )



Database Event Notification
===========================

Firebird can notify client applications when a trigger or stored
procedure posts an event with `POST_EVENT`.

.. method:: Connection.event_conduit(event_names, event_id=None)

   Creates an :class:`~firebirdsql.EventConduit` for the specified
   sequence of event name strings. Notifications are delivered only for
   the event names listed in `event_names`.

   :event_names: Sequence of event name strings.
   :event_id: (`Optional`) Event identifier to reuse.

.. class:: EventConduit

   .. method:: wait(timeout=None)

      Blocks until one of the registered events occurs, or until the
      optional timeout expires. Returns `None` on timeout; otherwise
      returns a dictionary mapping `event_name -> event_occurrence_count`.

   .. method:: close()

      Closes the conduit and cancels further event notifications.

Example Program
---------------

SQL trigger definition:

.. sourcecode:: sql

    create trigger trig_test_insert_event
      for test_table
        after insert
    as
    begin
      post_event 'test_event_a';
      post_event 'test_event_b';
      post_event 'test_event_a';
    end

Python event *handler* program:

.. sourcecode:: python

    import firebirdsql

    relevant_events = ['test_event_a', 'test_event_b']

    con = firebirdsql.connect(dsn='localhost:/temp/test.db', user='sysdba', password='pass')
    conduit = con.event_conduit(relevant_events)

    print('HANDLER: About to wait for the occurrence of one of %s...\n' % relevant_events)
    result = conduit.wait()
    print('HANDLER: An event notification has arrived:')
    print(result)
    conduit.close()

Python event *producer* program:

.. sourcecode:: python

    import firebirdsql

    con = firebirdsql.connect(dsn='localhost:/temp/test.db', user='sysdba', password='pass')
    cur = con.cursor()

    cur.execute("insert into test_table values (1)")
    print('PRODUCER: Committing transaction that will cause event notification to be sent.')
    con.commit()

The handler receives a dictionary such as `{'test_event_a': 2,
'test_event_b': 1}`.

.. note:: Event notifications are delivered when the posting transaction
   commits. Close conduits explicitly when they are no longer needed.


Database Information
====================

pyfirebirdsql exposes high-level wrappers for Firebird database and
transaction information requests.

.. method:: Connection.db_info(info_requests)

   Returns database information for one `isc_info_*` request code or for
   a sequence of request codes. When a sequence is supplied, the return
   value is a mapping of request code to parsed result.

   .. sourcecode:: python

      import firebirdsql

      con = firebirdsql.connect(dsn='localhost:/temp/test.db', user='sysdba', password='pass')
      info = con.db_info([
          firebirdsql.isc_info_page_size,
          firebirdsql.isc_info_allocation,
      ])
      print(info)

.. method:: Connection.trans_info(info_requests)

   Returns transaction information for one `isc_info_*` transaction
   request code or for a sequence of request codes.

Use :meth:`~Connection.db_info()` instead of the old low-level
`database_info` API that appeared in earlier versions of this
documentation.


Using Firebird Services API
===========================

.. module:: firebirdsql.services
   :synopsis: Access to Firebird Services API

Use :mod:`firebirdsql.services` to connect to the Firebird services
manager for administrative tasks such as backup, restore, tracing, and
server information queries.

Establishing Services API Connections
-------------------------------------

.. function:: connect(**kwargs)

   Connects to the Firebird services manager and returns a
   :class:`Services` instance.

   Common keyword arguments are `host`, `user`, and `password`.

   .. sourcecode:: python

      from firebirdsql import services

      svc = services.connect(host='localhost', user='sysdba', password='masterkey')

.. class:: Services

   .. method:: close()

      Closes the services manager connection.

Server Configuration and Activity Levels
----------------------------------------

.. method:: Services.getServiceManagerVersion()
.. method:: Services.getServerVersion()
.. method:: Services.getArchitecture()
.. method:: Services.getHomeDir()
.. method:: Services.getSecurityDatabasePath()
.. method:: Services.getLockFileDir()
.. method:: Services.getCapabilityMask()
.. method:: Services.getMessageFileDir()
.. method:: Services.getConnectionCount()
.. method:: Services.getAttachedDatabaseNames()
.. method:: Services.getLog()

   These methods return server and environment information.

   .. sourcecode:: python

      from firebirdsql import services

      svc = services.connect(host='localhost', user='sysdba', password='masterkey')
      print(svc.getServerVersion())
      print(svc.getAttachedDatabaseNames())

Database Statistics
-------------------

.. method:: Services.getStatistics(database_name,
                                   showOnlyDatabaseLogPages=False,
                                   showOnlyDatabaseHeaderPages=False,
                                   showUserDataPages=True,
                                   showUserIndexPages=True,
                                   showSystemTablesAndIndexes=False)

   Returns `gstat`-style textual statistics for the specified database.

   .. sourcecode:: python

      from firebirdsql import services

      svc = services.connect(user='sysdba', password='masterkey')
      print(svc.getStatistics('C:/temp/test.db'))


Backup and Restoration
----------------------

.. method:: Services.backup_database(database_name, backup_filename,
                                     transportable=True,
                                     metadataOnly=False,
                                     garbageCollect=True,
                                     ignoreLimboTransactions=False,
                                     ignoreChecksums=False,
                                     convertExternalTablesToInternalTables=True,
                                     expand=False,
                                     callback=None)

   Creates a backup of `database_name` in `backup_filename`.

   If `callback` is supplied, it is called with progress lines produced
   by the server.

   .. sourcecode:: python

      from firebirdsql import services

      svc = services.connect(user='sysdba', password='masterkey')
      svc.backup_database(
          'C:/temp/test.db',
          'C:/temp/test.fbk',
          callback=print,
      )

.. method:: Services.restore_database(restore_filename, database_name,
                                      replace=False,
                                      create=False,
                                      deactivateIndexes=False,
                                      doNotRestoreShadows=False,
                                      doNotEnforceConstraints=False,
                                      commitAfterEachTable=False,
                                      useAllPageSpace=False,
                                      pageSize=None,
                                      cacheBuffers=None,
                                      callback=None)

   Restores `restore_filename` into `database_name`.

   .. sourcecode:: python

      from firebirdsql import services

      svc = services.connect(user='sysdba', password='masterkey')
      svc.restore_database(
          'C:/temp/test.fbk',
          'C:/temp/test_restored.db',
          replace=True,
          callback=print,
      )

Trace Sessions
--------------

.. method:: Services.trace_start(name=None, cfg=None, callback=None)
.. method:: Services.trace_stop(id, callback=None)
.. method:: Services.trace_suspend(id, callback=None)
.. method:: Services.trace_resume(id, callback=None)
.. method:: Services.trace_list(callback=None)

   Controls Firebird trace sessions. The optional `callback` receives
   textual output from the services manager.

Database Maintenance
--------------------

.. method:: Services.sweep(database_name, callback=None)
.. method:: Services.bringOnline(database_name, callback=None)
.. method:: Services.shutdown(database_name,
                              timeout=0,
                              shutForce=True,
                              shutDenyNewAttachments=False,
                              shutDenyNewTransactions=False,
                              callback=None)

   Maintenance helpers exposed by the current codebase.

   .. sourcecode:: python

      from firebirdsql import services

      svc = services.connect(user='sysdba', password='masterkey')
      svc.sweep('C:/temp/test.db')
