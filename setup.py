from distutils.core import setup

classifiers = [
    'Development Status :: 3 - Alpha',
    'Topic :: Database',
    'License :: OSI Approved :: BSD License',
]

setup(name='firebirdsql', 
        version='0.3.3',
        description = 'python dbapi for firebird rdbms.', 
        url='http://github.com/nakagami/pyfirebirdsql/',
        classifiers=classifiers,
        keywords=['Firebird'],
        license='BSD',
        author='Hajime Nakagami',
        author_email='nakagami@gmail.com',
        packages = ['firebirdsql'],
)

