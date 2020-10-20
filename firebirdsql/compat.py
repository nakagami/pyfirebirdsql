import sys
import firebirdsql


def register():
    sys.modules['fdb'] = firebirdsql
