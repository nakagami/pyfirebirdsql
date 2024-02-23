#######################
pyfirebirdsql Changelog
#######################

Version 0.6.5
=============

   - callproc() is implemented.
   - document is prepared.

Version 0.6.6
=============

   - fix issue #28 InternalError after commit
   - fix issue #30 Incorrect handling empty row for RowMapping

Version 0.7.0
=============

   - change parameter name 'explain_plan' in class PrepareStatement.__init__().
   - issue #32 don't decode character set OCTETS
   - issue #33 op_allocate_statement do not require a transaction handle.
   - add event notification

Version 0.7.1
=============

   - fix fetchonemap()
   - issue #35 add timeout parameter to conduit.wait()

Version 0.7.2
=============

   - issue #36 add Cur.itermap() method
   - run tests in FB 1.5 (error treatment db_info(), trans_info())
   - issue #37 truncate field name from system table in FB 1.5

Version 0.7.3
=============

   - issue #38 fix some problems (thanx sergyp)
   - issue #39 AttributeError when accessing cursor.description
   - issue #41 fix int parameter bug
   - issue #42 Implemented Cursor.rowcount() (thanx jtasker)
   - setup.py test command

Version 0.7.4
=============

   - refactoring
   - call socket.close()

Version 0.8.0
=============

   - refactoring
   - buf fix
   - add 'role' parameter to connection 
   - fetch right striped string if CHAR type 
   - fetch string if BLOB subtype 1
   - boolean type support (Firebird 3)
   - connection keep only one transaction
   - 'INSERT ... RETURNING ...' statement support

Version 0.8.1
=============

   - bug fix (send all packets)

Version 0.8.2
=============

   - support 32k bytes over execute() parameter.

Version 0.8.3
=============

   - refactoring
   - add repair() method in Services
   - add is_disconnect() method in Connection

Version 0.8.4
=============

   - fix release bug (add insufficient files)

Version 0.8.5
=============

   - add bringOnline(), shutdown() methods in Services

Version 0.8.6
=============

   - fix exception when fetch after insert. return None

Version 0.9.0
=============

   - support Firebird 3 (experimental)

Version 0.9.1
=============

   - Refactoring
   - bugfixes
   - Modify isolation level. Similar to fdb.
   - Add Connection.set_autocommit() autocommit mode.

Version 0.9.2
=============

   - fix Binary() function
   - return recordset as tuple not list

Version 0.9.3
=============

   - refactoring
   - fix issue #50 alternative to crypt on windows

Version 0.9.4
=============

   - fix Cursor.rowcount.
   - Cursor.callproc() return out parameters.
   - Cursor.execute() return cursor instance itself.

Version 0.9.5
=============

   - Protocol version 11 support

Version 0.9.6
=============

   - support Firebird 3 (CORE-2897)

Version 0.9.7
=============

   - fix null indicator for Firebird 3
   - PyCrypto support for Firebird 3

Version 0.9.8
=============

   - fix issue #58 wrong logic for handling lage BLOBs.
   - update error messages.

Version 0.9.9
=============

   - refactoring
   - fix issue #60

Version 0.9.10
==============

   - fix bug for non posix (windows) environment. issue #62

Version 0.9.11
==============

   - fix issue #60 (again)

Version 0.9.12
==============

   - Enable Srp authentication and disable Wireprotocol for Firebird 3
   - fix a bug about srp authentication
   - refactoring and flake8

Version 0.9.13
==============

   - PEP 479 issue #66

Version 1.0.0
==============

   - refactoring
   - Add license file.
   - Documents update.

Version 1.0.1
==============

   - IPv6 support

Version 1.1.0
==============

   - Firebird4 DecFloat support
   - Modify statement allocate, drop treatment
   - Add factory parameter in cursor() method.
   - Add client process pid and name to op_attach https://github.com/nakagami/firebirdsql/pull/60

Version 1.1.0
==============

   - fix parse decfloat value

Version 1.1.1
==============

   - fix parse decfloat value
   - Srp256 support
   - fix issue #74 #75 #76 #77

Version 1.1.2
==============

   - refactoring
   - Add experimental Firebird4 timezone support

Version 1.1.3
==============

   - refactoring
   - Drop python 2.6 & 3.4 support
   - fix parse decfloat value
   - Srp256 is default authentication method
   - Support timezone aware datatype
   - Update error messages

Version 1.1.4
==============

   - Add int128 datatype (Firebird4)
   - timezone information hold as const values (Firebird4)
   - fix dsn paramerter parsing
   - Update error messages

Version 1.2.0
==============

   - refactoring
   - Drop python 3.5 support
   - Firebird4 timezone support

Version 1.2.1
==============

   - accept protocol version 16

Version 1.2.2
==============

   - refactoring
   - update error messages
   - fix typo
   - fix MANIFEST.in

Version 1.2.3
==============

   - refactoring
   - Drop python 3.6 support
   - update error messages

Version 1.2.4
==============

   - fix windows and LegacyAuth problem #99 #104

Version 1.2.5
==============

   - fix crypt with passlib
