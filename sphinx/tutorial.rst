
############################
Quick-start Guide / Tutorial
############################

This brief tutorial aims to get the reader started by demonstrating
elementary usage of pyfirebirdsql. It is not a comprehensive Python
Database API tutorial, nor is it comprehensive in its coverage of
anything else.

The numerous advanced features of pyfirebirdsql are covered in another
section of this documentation, which is not in a tutorial format, though it
is replete with examples.


Connecting to a Database
========================


**Example 1**

A database connection is typically established with code such as this:

.. sourcecode:: python

    import firebirdsql
    
    # The server is named 'bison'; the database file is at '/temp/test.fdb'.
    con = firebirdsql.connect(dsn='bison:/temp/test.fdb', user='sysdba', password='pass')
    
    # Or, equivalently:
    con = firebirdsql.connect(
        host='bison', database='/temp/test.fdb',
        user='sysdba', password='pass'
      )


**Example 2**

Suppose we want to connect to the database in SQL Dialect 1 and specifying
UTF-8 as the character set of the connection:

.. sourcecode:: python

    import firebirdsql
    
    con = firebirdsql.connect(
        dsn='bison:/temp/test.fdb',
        user='sysdba', password='pass',
        charset='UTF8' # specify a character set for the connection
      )


Executing SQL Statements
========================

For this section, suppose we have a table defined and populated by the
following SQL code:

.. sourcecode:: sql

    create table languages
    (
      name               varchar(20),
      year_released      integer
    );

    insert into languages (name, year_released) values ('C',        1972);
    insert into languages (name, year_released) values ('Python',   1991);


**Example 1**

This example shows the *simplest* way to print the entire contents of
the `languages` table:

.. sourcecode:: python

    import firebirdsql

    con = firebirdsql.connect(dsn='/temp/test.fdb', user='sysdba', password='masterkey')

    # Create a Cursor object that operates in the context of Connection con:
    cur = con.cursor()

    # Execute the SELECT statement:
    cur.execute("select * from languages order by year_released")

    # Retrieve all rows as a sequence and print that sequence:
    print cur.fetchall()


Sample output:

.. sourcecode:: python

    [('C', 1972), ('Python', 1991)]


**Example 2**

Here's another trivial example that demonstrates various ways of
fetching a single row at a time from a `SELECT`-cursor:

.. sourcecode:: python

    import firebirdsql

    con = firebirdsql.connect(dsn='/temp/test.fdb', user='sysdba', password='masterkey')

    cur = con.cursor()
    SELECT = "select name, year_released from languages order by year_released"

    # 1. Iterate over the rows available from the cursor, unpacking the
    # resulting sequences to yield their elements (name, year_released):
    cur.execute(SELECT)
    for (name, year_released) in cur:
        print '%s has been publicly available since %d.' % (name, year_released)

    # 2. Equivalently:
    cur.execute(SELECT)
    for row in cur:
        print '%s has been publicly available since %d.' % (row[0], row[1])


Sample output:

.. sourcecode:: python

    C has been publicly available since 1972.
    Python has been publicly available since 1991.
    C has been publicly available since 1972.
    Python has been publicly available since 1991.


**Example 3**

The following program is a simplistic table printer (applied in this
example to `languages`):

.. sourcecode:: python

    import firebirdsql as fb

    TABLE_NAME = 'languages'
    SELECT = 'select * from %s order by year_released' % TABLE_NAME

    con = fb.connect(dsn='/temp/test.fdb', user='sysdba', password='masterkey')

    cur = con.cursor()
    cur.execute(SELECT)

    # Print a header.
    for fieldDesc in cur.description:
        # Description name
        print fieldDesc[0] ,
    print # Finish the header with a newline.
    print '-' * 78

    # For each row, print the value of each field left-justified within
    # the maximum possible width of that field.
    fieldIndices = range(len(cur.description))
    for row in cur:
        for fieldIndex in fieldIndices:
            fieldValue = str(row[fieldIndex])
            #DESCRIPTION_DISPLAY_SIZE
            fieldMaxWidth = cur.description[fieldIndex][2]

            print fieldValue.ljust(fieldMaxWidth) ,

        print # Finish the row with a newline.


Sample output:

.. sourcecode:: python

    NAME                 YEAR_RELEASED
    ------------------------------------------------------------------------------
    C                    1972
    Python               1991


**Example 4**

Let's insert more languages:

.. sourcecode:: python

    import firebirdsql

    con = firebirdsql.connect(dsn='/temp/test.fdb', user='sysdba', password='masterkey')

    cur = con.cursor()

    newLanguages = [
        ('Lisp',  1958),
        ('Dylan', 1995),
      ]

    cur.executemany("insert into languages (name, year_released) values (?, ?)",
        newLanguages
      )

    # The changes will not be saved unless the transaction is committed explicitly:
    con.commit()


Note the use of a *parameterized* SQL statement above. When dealing
with repetitive statements, this is much faster and less error-prone
than assembling each SQL statement manually. (You can read more about
parameterized SQL statements in the section on `Prepared Statements`.)

After running Example 4, the table printer from Example 3 would print:

.. sourcecode:: python

    NAME                 YEAR_RELEASED
    ------------------------------------------------------------------------------
    Lisp                 1958
    C                    1972
    Python               1991
    Dylan                1995


Calling Stored Procedures
=========================

Firebird supports stored procedures written in a proprietary procedural
SQL language. Firebird stored procedures can have *input* parameters and/or
*output* parameters. Some databases support *input/output* parameters,
where the same parameter is used for both input and output; Firebird does
not support this.

It is important to distinguish between procedures that *return a
result set* and procedures that *populate and return their output
parameters exactly once*. Conceptually, the latter "return their
output parameters" like a Python function, whereas the former "yield
result rows" like a Python generator.

Firebird's *server-side* procedural SQL syntax makes no such distinction,
but *client-side* SQL code (and C API code) must. A result set is
retrieved from a stored procedure by `SELECT`ing from the procedure,
whereas output parameters are retrieved with an `EXECUTE PROCEDURE`
statement.

To *retrieve a result set* from a stored procedure with pyfirebirdsql,
use code such as this:

.. sourcecode:: python

    cur.execute("select output1, output2 from the_proc(?, ?)", (input1, input2))

    # Ordinary fetch code here, such as:
    for row in cur:
        ... # process row

    con.commit() # If the procedure had any side effects, commit them.

To *execute* a stored procedure and *access its output parameters*,
use code such as this:

.. sourcecode:: python

    cur.callproc("the_proc", (input1, input2))

    # If there are output parameters, retrieve them as though they were the
    # first row of a result set.  For example:
    outputParams = cur.fetchone()

    con.commit() # If the procedure had any side effects, commit them.


This latter is not very elegant; it would be preferable to access the
procedure's output parameters as the return value of
`Cursor.callproc()`. The Python DB API specification requires the
current behavior, however.
