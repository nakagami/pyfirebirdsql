from distutils.core import setup
import firebirdsql

classifiers = [
    'Development Status :: 3 - Alpha',
    'Topic :: Database',
    'License :: OSI Approved :: BSD License',
]

setup(name='firebirdsql', 
        version=firebirdsql.__version__,
        description = 'Python dbapi for Firebird RDBMS.', 
        url='http://github.com/nakagami/pyfirebirdsql/',
        classifiers=classifiers,
        keywords=['Firebird'],
        license='BSD',
        author='Hajime Nakagami',
        author_email='nakagami@gmail.com',
        packages = ['firebirdsql'],
)

