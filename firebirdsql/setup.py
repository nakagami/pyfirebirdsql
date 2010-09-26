from distutils.core import setup

classifiers = [
    'Development Status :: 3 - Alpha',
    'Topic :: Database',
    'License :: OSI Approved :: BSD License',
]

setup(name='firebirdsql', 
        version='0.1.0',
        description = 'python3 dbapi for firebird rdbms.', 
        url='http://github.com/nakagami/pyfirebirdsql/',
        classifiers=classifiers,
        keyword=['Firebird'],
        license='BSD',
        author='Hajime Nakagami',
        author_email='nakagami@gmail.com',
)

