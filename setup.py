#!/usr/bin/env python
""" firebirdsql package is a set of Firebird RDBMS bindings for python.
It works on Python 2.4+ (include Python 3.x).

import firebirdsql
conn = firebirdsql.connect(dsn='localhost/3050:/foo/bar.fdb', user='alice', password='secret')
cur = conn.cursor()
cur.execute("select * from baz")
for c in cur.fetchall():
    print(c)
conn.close()
"""
from distutils.core import setup
import firebirdsql


classifiers = [
    'Development Status :: 4 - Beta',
    'Topic :: Database',
    'License :: OSI Approved :: BSD License',
]

setup(name='firebirdsql', 
        version=firebirdsql.__version__,
        description = 'Firebird RDBMS bindings for python.', 
        url='http://github.com/nakagami/pyfirebirdsql/',
        classifiers=classifiers,
        keywords=['Firebird'],
        license='BSD',
        author='Hajime Nakagami',
        author_email='nakagami@gmail.com',
        packages = ['firebirdsql'],
        long_description=__doc__,
)
