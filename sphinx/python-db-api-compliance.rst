#####################################
Compliance to Python Database API 2.0 
#####################################

.. currentmodule:: firebirdsql


Unsupported Optional Features
=============================

.. method:: Cursor.nextset()

   This method is not implemented because the database engine does not support
   opening multiple result sets simultaneously with a single cursor.


Nominally Supported Optional Features
=====================================

.. class:: Cursor

        .. attribute:: arraysize

        As required by the spec, the value of this attribute is observed with
        respect to the `fetchmany` method. However, changing the value of this
        attribute does not make any difference in fetch efficiency because
        the database engine only supports fetching a single row at a time.

        .. method:: setinputsizes()

        Although this method is present, it does nothing, as allowed by the spec.

        .. method:: setoutputsize() 

        Although this method is present, it does nothing, as allowed by the spec.


Extensions and Caveats
======================

pyfirebirdsql offers a large feature set beyond the minimal requirements
of the Python DB API. Most of these extensions are documented in the
section of this document entitled `Native Database Engine Features and
Extensions Beyond the Python DB API`.

This section attempts to document only those features that overlap
with the DB API, or are too insignificant to warrant their own
subsection elsewhere.


.. function:: connect() 

   This function supports the following optional keyword arguments in addition
   to those required by the spec:

   :role:  For connecting to a database with a specific SQL role.

   *Example:*

   .. sourcecode:: python

      firebirdsql.connect(dsn='host:/path/database.db', user='limited_user',
         password='pass', role='MORE_POWERFUL_ROLE')

   :charset:  For explicitly specifying the character set of the connection.
              See Firebird Documentation for a list of available character sets, and
              `Unicode Fields and pyfirebirdsql` section for information on handling
              extended character sets with pyfirebirdsql.

   *Example:*

   .. sourcecode:: python

      firebirdsql.connect(dsn='host:/path/database.db', user='sysdba',
          password='pass', charset='UTF8')

   :timeout:  (`Optional`) Dictionary with timeout and action specification. See section
              about `Connection Timeouts <beyond-python-db-api.html#connection-timeout>`_ for details.

.. class:: Connection

   .. attribute:: charset 

      *(read-only)* The character set of the connection (set via the `charset`
      parameter of :func:`firebirdsql.connect()`). See Firebird Documentation for a list
      of available character sets, and `Unicode Fields and pyfirebirdsql` section
      for information on handling extended character sets with pyfirebirdsql.

   .. attribute:: server_version

      *(read-only)* The version string of the database server to which this connection
      is connected. For example, a connection to Firebird 1.0 on Windows has the following
      `server_version`: `WI-V6.2.794 Firebird 1.0`

   .. method:: execute_immediate

      Executes a statement without caching its prepared form. The statement must *not* be
      of a type that returns a result set. In most cases (especially cases in which the same
      statement -- perhaps a parameterized statement -- is executed repeatedly),
      it is better to create a cursor using the connection's `cursor` method, then execute
      the statement using one of the cursor's execute methods. 

      Arguments:

      :sql:  String containing the SQL statement to execute.

   .. method:: commit(retaining=False)
   .. method:: rollback(retaining=False)

      The `commit` and `rollback` methods accept an optional boolean parameter `retaining`
      (default `False`) that indicates whether the transactional context of the transaction
      being resolved should be recycled. For details, see the Advanced
      Transaction Control: Retaining Operations section of this document.
      The `rollback` method accepts an optional string parameter `savepoint`
      that causes the transaction to roll back only as far as the designated
      savepoint, rather than rolling back entirely. For details, see the
      Advanced Transaction Control: Savepoints section of this document.


.. class:: Cursor

   .. attribute:: description

      pyfirebirdsql makes absolutely no guarantees about `description` except those
      required by the Python Database API Specification 2.0 (that is, `description`
      is either `None` or a sequence of 7-element sequences). Therefore, client
      programmers should *not* rely on `description` being an instance of a particular
      class or type. pyfirebirdsql provides several named positional constants to be
      used as indices into a given element of `description` . The contents
      of all `description` elements are defined by the DB API spec; these
      constants are provided merely for convenience.

      .. sourcecode:: python

         DESCRIPTION_NAME
         DESCRIPTION_TYPE_CODE
         DESCRIPTION_DISPLAY_SIZE
         DESCRIPTION_INTERNAL_SIZE
         DESCRIPTION_PRECISION
         DESCRIPTION_SCALE
         DESCRIPTION_NULL_OK

      Here is an example of accessing the *name* of the first field in the
      `description` of cursor `cur`:

      .. sourcecode:: python

         nameOfFirstField = cur.description[0][firebirdsql.DESCRIPTION_NAME]

      For more information, see the documentation of Cursor.description in
      the `DB API Specification <Python-DB-API-2.0.html>`__.

   .. attribute:: rowcount

      Although pyfirebirdsql's `Cursor`s implement this
      attribute, the database engine's own support for the determination of
      "rows affected"/"rows selected" is quirky. The database engine only
      supports the determination of rowcount for `INSERT`, `UPDATE`,
      `DELETE`, and `SELECT` statements. When stored procedures become
      involved, row count figures are usually not available to the client.
      Determining rowcount for `SELECT` statements is problematic: the
      rowcount is reported as zero until at least one row has been fetched
      from the result set, and the rowcount is misreported if the result set
      is larger than 1302 rows. The server apparently marshals result sets
      internally in batches of 1302, and will misreport the rowcount for
      result sets larger than 1302 rows until the 1303rd row is fetched,
      result sets larger than 2604 rows until the 2605th row is fetched, and
      so on, in increments of 1302. As required by the Python DB API Spec,
      the rowcount attribute "is -1 in case no executeXX() has been
      performed on the cursor or the rowcount of the last operation is not
      determinable by the interface".

   .. method:: fetchone()
   .. method:: fetchmany()
   .. method:: fetchall()

      pyfirebirdsql makes absolutely no guarantees about
      the return value of the `fetchone` / `fetchmany` / `fetchall` methods
      except that it is a sequence indexed by field position. pyfirebirdsql
      makes absolutely no guarantees about the return value of the
      `fetchonemap` / `fetchmanymap` / `fetchallmap` methods (documented
      below) except that it is a mapping of field name to field value.
      Therefore, client programmers should *not* rely on the return value
      being an instance of a particular class or type.

   .. method:: fetchonemap()

      This method is just like the standard
      `fetchone` method of the DB API, except that it returns a mapping of
      field name to field value, rather than a sequence.

   .. method:: fetchmanymap()

      This method is just like the standard
      `fetchmany` method of the DB API, except that it returns a sequence of
      mappings of field name to field value, rather than a sequence of
      sequences.

   .. method:: fetchallmap()

      This method is just like the standard
      `fetchall` method of the DB API, except that it returns a sequence of
      mappings of field name to field value, rather than a sequence of
      sequences.

   .. method:: iter()
   .. method:: itermap()

      These methods are equivalent to the
      `fetchall` and `fetchallmap` methods, respectively, except that they
      return iterators rather than materialized sequences. `iter` and
      `itermap` are exercised in this example.

