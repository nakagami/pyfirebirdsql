################################
pyfirebirdsql Installation Guide
################################

Dependencies
************

pyfirebirdsql requires a valid combination of the dependencies in
the list below.

Detailed instructions on how to install each dependency are beyond the
scope of this document; consult the dependency distributor for
installation instructions.

Satisfying the dependencies is not difficult! For mainstream
operating systems -- including Windows , Macosx and Linux -- easily installable
binary distributions are available for *all* of pyfirebirdsql's
dependencies (see the download links below).


#. Operating System - one of:

    + Windows (32/64)
    + Linux Any Cpu 
    + FreeBSD
    + Macosx 
    + Other Unix or Unix-like operating system 

#. `Firebird <http://www.firebirdsql.org>`__ 2.1 or later server version 
   installation [`download here <http://www.firebirdsql.org/en/server-packages/>`__] (Firebird client is not necessary to connect to a server)

#. `Python <http://www.python.org>`__ [`download here
   <http://www.python.org/download/>`__] 2.6 or later (including Python 3.x) It was tested with cpython , ironpython and pypy


How to install
=============================

- `Install by pip`_
- Install from `FreeBSD ports collection`_
- Install from `source distribution`_

.. _`Install by pip`:

Install by pip
*************************************

::

  pip install firebirdsql

.. _`FreeBSD ports collection`:

Installation from FreeBSD ports collection
*********************************************

FreeBSD has it's port now::

  # cd /usr/ports/databases/py-firebirdsql/
  # make install clean

.. _`source distribution`:

Installation from source distribution
*************************************

Shortcut for the Experienced and Impatient::

  (decompress pyfirebirdsql into *temp_dir*)
  cd *temp_dir*
  python setup.py install
  python -c "import firebirdsql"
  (delete *temp_dir*)

Then hit the `Usage Guide <usage.html>`__.


Test your pyfirebirdsql installation
************************************

pyfirebirdsql has an extensive test suite, but it is not really intended for
routine public use.

To verify that pyfirebirdsql is installed properly, switch to a
directory *other than the temporary directory into which you
decompressed the source distribution* (to avoid conflict between the
copy of firebirdsql in that directory and the copy placed under the
standard Python `site-packages` directory), then verify the
importability of your pyfirebirdsql installation by issuing the
following command::

  python -c "import firebirdsql as fb; print fb.__version__"

If the import attempt does not encounter any errors and the version
number is what you expected, you are finished. Next, consider reading
the pyfirebirdsql Usage Guide.

You should not encounter any errors at this stage since you have
already completed the installation steps successfully.
If you do, please report them to the `firebird-python  support list
<http://tech.groups.yahoo.com/group/firebird-python/>`__.


