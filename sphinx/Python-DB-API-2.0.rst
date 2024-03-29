
#####################################
Python Database API Specification 2.0
#####################################

pyfirebirdsql is the Python Database API 2.0 compliant driver
for Firebird. The `Reference / Usage Guide` is therefore divided
into three parts:

  * Python Database API 2.0 specification
  * pyfirebirdsql Compliance to Python DB 2.0 API specification.
  * pyfirebirdsql features beyond Python DB 2.0 API specification.

If you're familiar to Python DB 2.0 API specification, you may skip
directly to the next topic.

.. note::

   This is a local copy of the specification. The online source copy is
   available at `http://www.python.org/topics/database/DatabaseAPI-2.0.html
   <http://www.python.org/topics/database/DatabaseAPI-2.0.html>`__

Introduction
============

This API has been defined to encourage similarity between the Python
modules that are used to access databases. By doing this, we hope to
achieve a consistency leading to more easily understood modules, code
that is generally more portable across databases, and a broader reach
of database connectivity from Python.

The interface specification consists of several sections:

+ Module Interface
+ Connection Objects
+ Cursor Objects
+ Type Objects and Constructors
+ Implementation Hints
+ Major Changes from 1.0 to 2.0

Comments and questions about this specification may be directed to the
`SIG for Database Interfacing with Python <mailto:db-sig@python.org>`__.

For more information on database interfacing with Python and available
packages see the `Database Topics Guide
<http://www.python.org/topics/database/>`__ on `www.python.org
<http://www.python.org/>`__.

This document describes the Python Database API Specification 2.0. The
previous `version 1.0 version
<http://www.python.org/topics/database/DatabaseAPI-1.0.html>`__ is
still available as reference. Package writers are encouraged to use
this version of the specification as basis for new interfaces.

Module Interface
================

Access to the database is made available through connection objects.
The module must provide the following constructor for these:

.. function::  connect(parameters...)

   Constructor for creating a connection to the database. Returns a Connection Object . 
   It takes a number of parameters which are database dependent. [#f1]_

These module globals must be defined:

.. data:: apilevel

   String constant stating the supported DB API level.
   Currently only the strings `'1.0'` and `'2.0'` are allowed. If not
   given, a `Database API 1.0 <http://www.python.org/topics/database/DatabaseAPI-1.0.html>`__ level
   interface should be assumed.

.. data:: threadsafety

   Integer constant stating the level of thread safety the interface supports. 
   Possible values are: 

     - `0` = Threads may not share the module. 
     - `1` = Threads may share the module, but not connections. 
     - `2` = Threads may share the module and connections. 
     - `3` = Threads may share the module, connections and cursors. Sharing in the
       above context means that two threads may use a resource without
       wrapping it using a mutex semaphore to implement resource locking.

    Note that you cannot always make external resources thread safe by
    managing access using a mutex: the resource may rely on global
    variables or other external sources that are beyond your control.

.. data:: paramstyle

   String constant stating the type of parameter marker
   formatting expected by the interface. Possible values are [#f2]_:

   - `'qmark'`    = Question mark style, e.g. '...WHERE name=?' 
   - `'numeric'`  = Numeric, positional style, e.g. '...WHERE name=:1' 
   - `'named'`    = Named style, e.g. '...WHERE name=:name' 
   - `'format'`   = ANSI C printf format codes, e.g. '...WHERE name=%s' 
   - `'pyformat'` = Python extended format codes, e.g. '...WHERE name=%(name)s'

The module should make all error information available through these
exceptions or subclasses thereof:

.. exception:: Warning

   Exception raised for important warnings like data truncations while inserting, 
   etc. It must be a subclass of the Python StandardError (defined in the module 
   exceptions).

.. exception:: Error

   Exception that is the base class of all other error exceptions. You can use this 
   to catch all errors with one single 'except' statement. Warnings are not considered
   errors and thus should not use this class as base. It must be a subclass of
   the Python StandardError (defined in the module exceptions).

.. exception:: InterfaceError

   Exception raised for errors that are related to the database interface rather
   than the database itself. It must be a subclass of Error.

.. exception:: DatabaseError

   Exception raised for errors that are related to the database. It must be a subclass
   of Error.

.. exception:: DataError

   Exception raised for errors that are due to problems with the processed data 
   like division by zero, numeric value out of range, etc. It must be a subclass
   of DatabaseError.

.. exception:: OperationalError

   Exception raised for errors that are related to the database's operation and
   not necessarily under the control of the programmer, e.g. an unexpected disconnect
   occurs, the data source name is not found, a transaction could not be processed,
   a memory allocation error occurred during processing, etc. It must be a subclass
   of DatabaseError.

.. exception:: IntegrityError

   Exception raised when the relational integrity of the database is affected,
   e.g. a foreign key check fails. It must be a subclass of DatabaseError.

.. exception:: InternalError

   Exception raised when the database encounters an internal error, e.g. the cursor
   is not valid anymore, the transaction is out of sync, etc. It must be a subclass
   of DatabaseError.

.. exception:: ProgrammingError

   Exception raised for programming errors, e.g. table not found or already exists,
   syntax error in the SQL statement, wrong number of parameters specified, etc.
   It must be a subclass of DatabaseError.

.. exception:: NotSupportedError

   Exception raised in case a method or database API was used which is not supported
   by the database, e.g. requesting a .rollback() on a connection that does not support
   transaction or has transactions turned off. It must be a subclass of DatabaseError.


This is the exception inheritance layout:

.. sourcecode:: python

    StandardError
    |__Warning
    |__Error
       |__InterfaceError
       |__DatabaseError
          |__DataError
          |__OperationalError
          |__IntegrityError
          |__InternalError
          |__ProgrammingError
          |__NotSupportedError

  Note: The values of these exceptions are not defined. They should give
  the user a fairly good idea of what went wrong though.



Connection Objects
==================

Connections Objects should respond to the following methods:

.. class:: Connection

   .. method:: close() 

      Close the connection now (rather than whenever __del__ is called). The connection
      will be unusable from this point forward; an `Error` (or subclass) exception will
      be raised if any operation is attempted with the connection. The same applies to 
      all cursor objects trying to use the connection.

   .. method:: commit() 

      Commit any pending transaction to the database. Note
      that if the database supports an auto-commit feature, this must be
      initially off. An interface method may be provided to turn it back on.
      Database modules that do not support transactions should implement
      this method with void functionality.

   .. method:: rollback()

      This method is optional since not all databases
      provide transaction support. [#f3]_ In case a database does provide
      transactions this method causes the the database to roll back to the
      start of any pending transaction. Closing a connection without
      committing the changes first will cause an implicit rollback to be
      performed.

   .. method:: cursor() 

      Return a new Cursor Object using the connection. If
      the database does not provide a direct cursor concept, the module will
      have to emulate cursors using other means to the extent needed by this
      specification. [#f4]_


Cursor Objects
==============

These objects represent a database cursor, which is used to manage the
context of a fetch operation. Cursor Objects should respond to the
following methods and attributes:

.. class:: Cursor

   .. attribute:: description 

      This read-only attribute is a sequence of 7-item
      sequences. Each of these sequences contains information describing one
      result column: `(name, type_code, display_size, internal_size,
      precision, scale, null_ok)`. This attribute will be `None` for
      operations that do not return rows or if the cursor has not had an
      operation invoked via the `executeXXX()` method yet. The `type_code`
      can be interpreted by comparing it to the Type Objects specified in
      the section below.

   .. attribute:: rowcount

      This read-only attribute specifies the number of rows
      that the last `executeXXX()` produced (for DQL statements like select) 
      or affected (for DML statements like update or insert ). The
      attribute is -1 in case no `executeXXX()` has been performed on the
      cursor or the rowcount of the last operation is not determinable by
      the interface. [#f7]_

   .. method:: callproc(procname[,parameters]) 

      This method is optional since not all databases provide stored procedures. 
      [#f3]_ Call a stored database procedure with the given name. The sequence of parameters
      must contain one entry for each argument that the procedure expects.
      The result of the call is returned as modified copy of the input
      sequence. Input parameters are left untouched, output and input/output
      parameters replaced with possibly new values. The procedure may also
      provide a result set as output. This must then be made available
      through the standard `fetchXXX()` methods.

   .. method:: close() 

      Close the cursor now (rather than whenever __del__ is
      called). The cursor will be unusable from this point forward; an
      `Error` (or subclass) exception will be raised if any operation is
      attempted with the cursor.

   .. method:: execute(operation[,parameters]) 

      Prepare and execute a database operation (query or command). 
      Parameters may be provided as sequence or mapping and will be bound to 
      variables in the operation. Variables are specified in a database-specific
      notation (see the module's `paramstyle` attribute for details). 
      [#f5]_ A reference to the operation will be retained by the cursor. If the same
      operation object is passed in again, then the cursor can optimize its behavior.
      This is most effective for algorithms where the same operation is used, but
      different parameters are bound to it (many times). For maximum
      efficiency when reusing an operation, it is best to use the
      setinputsizes() method to specify the parameter types and sizes ahead
      of time. It is legal for a parameter to not match the predefined
      information; the implementation should compensate, possibly with a
      loss of efficiency. The parameters may also be specified as list of
      tuples to e.g. insert multiple rows in a single operation, but this
      kind of usage is depreciated: `executemany()` should be used instead.
      Return values are not defined.

   .. method:: executemany(operation,seq_of_parameters) 

      Prepare a database operation (query or command) and then execute it against all parameter
      sequences or mappings found in the sequence `seq_of_parameters`.
      Modules are free to implement this method using multiple calls to the
      `execute()` method or by using array operations to have the database
      process the sequence as a whole in one call. The same comments as for
      `execute()` also apply accordingly to this method. Return values are
      not defined.

   .. method:: fetchone() 

      Fetch the next row of a query result set, returning
      a single sequence, or `None` when no more data is available. [#f6]_ An
      `Error` (or subclass) exception is raised if the previous call to
      `executeXXX()` did not produce any result set or no call was issued yet.

   .. method:: fetchmany([size=cursor.arraysize]) 

      Fetch the next set of rows of a query result, returning a sequence of sequences
      (e.g. a list of tuples). An empty sequence is returned when no more rows are
      available. The number of rows to fetch per call is specified by the
      parameter. If it is not given, the cursor's `arraysize` determines the
      number of rows to be fetched. The method should try to fetch as many
      rows as indicated by the size parameter. If this is not possible due
      to the specified number of rows not being available, fewer rows may be
      returned. An `Error` (or subclass) exception is raised if the previous
      call to `executeXXX()` did not produce any result set or no call was
      issued yet. Note there are performance considerations involved with
      the size parameter. For optimal performance, it is usually best to use
      the arraysize attribute. If the size parameter is used, then it is
      best for it to retain the same value from one `fetchmany()` call to the next.

   .. method:: fetchall() 

      Fetch all (remaining) rows of a query result, returning them as a sequence
      of sequences (e.g. a list of tuples).
      Note that the cursor's `arraysize` attribute can affect the
      performance of this operation. An `Error` (or subclass) exception is
      raised if the previous call to `executeXXX()` did not produce any
      result set or no call was issued yet.

   .. method:: nextset() 

      This method is optional since not all databases
      support multiple result sets. [#f3]_ This method will make the cursor
      skip to the next available set, discarding any remaining rows from the
      current set. If there are no more sets, the method returns `None`.
      Otherwise, it returns a true value and subsequent calls to the fetch
      methods will return rows from the next result set. An `Error` (or
      subclass) exception is raised if the previous call to `executeXXX()`
      did not produce any result set or no call was issued yet.

   .. attaribute arraysize

      This read/write attribute specifies the number of
      rows to fetch at a time with `fetchmany()`. It defaults to 1 meaning
      to fetch a single row at a time. Implementations must observe this
      value with respect to the `fetchmany()` method, but are free to
      interact with the database a single row at a time. It may also be used
      in the implementation of `executemany()`.

   .. method:: setinputsizes(sizes) 

      This can be used before a call to `executeXXX()` to predefine memory areas
      for the operation's parameters. `sizes` is specified as a sequence -- one 
      item for each input parameter. The item should be a Type Object that corresponds 
      to the input that will be used, or it should be an integer specifying the
      maximum length of a string parameter. If the item is `None`, then no
      predefined memory area will be reserved for that column (this is
      useful to avoid predefined areas for large inputs). This method would
      be used before the `executeXXX()` method is invoked. Implementations
      are free to have this method do nothing and users are free to not use it.

   .. method:: setoutputsize(size[,column]) 

      Set a column buffer size for fetches of large columns (e.g. LONGs, BLOBs, etc.).
      The column is specified as an index into the result sequence. Not specifying the
      column will set the default size for all large columns in the cursor.
      This method would be used before the `executeXXX()` method is invoked.
      Implementations are free to have this method do nothing and users are
      free to not use it.


Type Objects and Constructors
=============================

Many databases need to have the input in a particular format for
binding to an operation's input parameters. For example, if an input
is destined for a DATE column, then it must be bound to the database
in a particular string format. Similar problems exist for "Row ID"
columns or large binary items (e.g. blobs or RAW columns). This
presents problems for Python since the parameters to the
`executeXXX()` method are untyped. When the database module sees a
Python string object, it doesn't know if it should be bound as a
simple CHAR column, as a raw BINARY item, or as a DATE. To overcome
this problem, a module must provide the constructors defined below to
create objects that can hold special values. When passed to the cursor
methods, the module can then detect the proper type of the input
parameter and bind it accordingly. A Cursor Object's `description`
attribute returns information about each of the result columns of a
query. The `type_code` must compare equal to one of Type Objects
defined below. Type Objects may be equal to more than one type code
(e.g. DATETIME could be equal to the type codes for date, time and
timestamp columns; see the Implementation Hints below for details).
The module exports the following constructors and singletons:

.. function:: Date(year,month,day) 

   This function constructs an object holding a date value.

.. function:: Time(hour,minute,second) 

   This function constructs an object holding a time value.

.. function:: Timestamp(year,month,day,hour,minute,second) 

   This function constructs an object holding a time stamp value.

.. function:: DateFromTicks(ticks) 

   This function constructs an object holding a date value from the given
   ticks value (number of seconds since the epoch; see the documentation
   of the standard Python time module for details).

.. function:: TimeFromTicks(ticks) 

   This function constructs an object holding a time value from the given
   ticks value (number of seconds since the epoch; see the documentation
   of the standard Python time module for details).

.. function:: TimestampFromTicks(ticks) 

   This function constructs an object holding a time stamp value from the given
   ticks value (number of seconds since the epoch; see the documentation
   of the standard Python time module for details).

.. function:: Binary(string) 

   This function constructs an object capable of holding a binary (long) string value.

.. data:: STRING 

   This type object is used to describe columns in a database that are string-based
   (e.g. CHAR).

.. data:: BINARY

   This type object is used to describe (long) binary columns in a database
   (e.g. LONG, RAW, BLOBs).

.. data:: NUMBER

   This type object is used to describe numeric columns in a database.

.. data:: DATETIME

   This type object is used to describe date/time columns in a database.

.. data:: ROWID

   This type object is used to describe the "Row ID" column in a database.

SQL NULL values are represented by the Python `None` singleton on
input and output. Note: Usage of Unix ticks for database interfacing
can cause troubles because of the limited date range they cover.


Implementation Hints
====================

+ The preferred object types for the date/time objects are those
  defined in the `mxDateTime
  <http://starship.python.net/%7Elemburg/mxDateTime.html>`__ package. It
  provides all necessary constructors and methods both at Python and C
  level.
+ The preferred object type for Binary objects are the buffer types
  available in standard Python starting with version 1.5.2. Please see
  the Python documentation for details. For information about the the C
  interface have a look at Include/bufferobject.h and
  Objects/bufferobject.c in the Python source distribution.
+ Here is a sample implementation of the Unix ticks based constructors
  for date/time delegating work to the generic constructors:

.. sourcecode:: python

    import time
    
    def DateFromTicks(ticks):
    
        return apply(Date,time.localtime(ticks)[:3])
    
    def TimeFromTicks(ticks):
    
        return apply(Time,time.localtime(ticks)[3:6])
    
    def TimestampFromTicks(ticks):
    
        return apply(Timestamp,time.localtime(ticks)[:6])


+ This Python class allows implementing the above type objects even
  though the description type code field yields multiple values for on
  type object:

.. sourcecode:: python

    class DBAPITypeObject:
        def __init__(self,*values):
        self.values = values
        def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

  The resulting type object compares equal to all values passed to the
  constructor.

+ Here is a snippet of Python code that implements the exception
  hierarchy defined above:

.. sourcecode:: python

    import exceptions
    
    class Error(exceptions.StandardError):
        pass
    
    class Warning(exceptions.StandardError):
        pass
    
    class InterfaceError(Error):
        pass
    
    class DatabaseError(Error):
        pass
    
    class InternalError(DatabaseError):
        pass
    
    class OperationalError(DatabaseError):
        pass
    
    class ProgrammingError(DatabaseError):
        pass
    
    class IntegrityError(DatabaseError):
        pass
    
    class DataError(DatabaseError):
        pass
    
    class NotSupportedError(DatabaseError):
        pass

  In C you can use the `PyErr_NewException(fullname, base, NULL)` API to
  create the exception objects.


Major Changes from Version 1.0 to Version 2.0
=============================================

  The Python Database API 2.0 introduces a few major changes compared to
  the 1.0 version. Because some of these changes will cause existing `DB
  API 1.0
  <http://www.python.org/topics/database/DatabaseAPI-1.0.html>`__ based
  scripts to break, the major version number was adjusted to reflect
  this change. These are the most important changes from 1.0 to 2.0:

    + The need for a separate dbi module was dropped and the functionality
      merged into the module interface itself.
    + New constructors and Type Objects were added for date/time values,
      the RAW Type Object was renamed to BINARY. The resulting set should
      cover all basic data types commonly found in modern SQL databases.
    + New constants (apilevel, threadlevel, paramstyle) and methods
      (executemany, nextset) were added to provide better database bindings.
    + The semantics of .callproc() needed to call stored procedures are
      now clearly defined.
    + The definition of the .execute() return value changed. Previously,
      the return value was based on the SQL statement type (which was hard
      to implement right) -- it is undefined now; use the more flexible
      .rowcount attribute instead. Modules are free to return the old style
      return values, but these are no longer mandated by the specification
      and should be considered database interface dependent.
    + Class based exceptions were incorporated into the specification.
      Module implementors are free to extend the exception layout defined in
      this specification by subclassing the defined exception classes.


Open Issues
===========

Although the version 2.0 specification clarifies a lot of questions
that were left open in the 1.0 version, there are still some remaining
issues:

+ Define a useful return value for .nextset() for the case where a new
  result set is available.
+ Create a fixed point numeric type for use as loss-less monetary and
  decimal interchange format.


Footnotes
=========

.. [#f1] As a guideline the connection constructor parameters should be
   implemented as keyword parameters for more intuitive use and follow
   this order of parameters: `dsn` = Data source name as string `user` =
   User name as string (optional) `password` = Password as string
   (optional) `host` = Hostname (optional) `database` = Database name
   (optional) E.g. a connect could look like this:
   `connect(dsn='myhost:MYDB',user='guido',password='234$?')`
.. [#f2] Module implementors should prefer 'numeric', 'named' or 'pyformat'
   over the other formats because these offer more clarity and
   flexibility.
.. [#f3] If the database does not support the functionality required by the
   method, the interface should throw an exception in case the method is
   used. The preferred approach is to not implement the method and thus
   have Python generate an `AttributeError` in case the method is
   requested. This allows the programmer to check for database
   capabilities using the standard `hasattr()` function. For some
   dynamically configured interfaces it may not be appropriate to require
   dynamically making the method available. These interfaces should then
   raise a `NotSupportedError` to indicate the non-ability to perform the
   roll back when the method is invoked.
.. [#f4] A database interface may choose to support named cursors by
   allowing a string argument to the method. This feature is not part of
   the specification, since it complicates semantics of the `.fetchXXX()`
   methods.
.. [#f5] The module will use the __getitem__ method of the parameters object
   to map either positions (integers) or names (strings) to parameter
   values. This allows for both sequences and mappings to be used as
   input. The term "bound" refers to the process of binding an input
   value to a database execution buffer. In practical terms, this means
   that the input value is directly used as a value in the operation. The
   client should not be required to "escape" the value so that it can be
   used -- the value should be equal to the actual database value.
.. [#f6] Note that the interface may implement row fetching using arrays and
   other optimizations. It is not guaranteed that a call to this method
   will only move the associated cursor forward by one row.
.. [#f7] The `rowcount` attribute may be coded in a way that updates its
   value dynamically. This can be useful for databases that return
   usable rowcount values only after the first call to a `.fetchXXX()`
   method.



