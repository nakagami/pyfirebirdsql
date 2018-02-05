#!/usr/bin/env python
import sys
from distutils.core import setup, Command
import firebirdsql


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        '''
        Finds all the tests modules in tests/, and runs them.
        '''
        from firebirdsql import tests
        import unittest
        unittest.main(tests, argv=sys.argv[:1])

cmdclass = {'test': TestCommand}


classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name='firebirdsql',
    version=firebirdsql.__version__,
    description='Firebird RDBMS bindings for python.',
    long_description=open('README.rst').read(),
    url='http://github.com/nakagami/pyfirebirdsql/',
    classifiers=classifiers,
    keywords=['Firebird'],
    license='BSD',
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    packages=['firebirdsql'],
    cmdclass=cmdclass,
)
