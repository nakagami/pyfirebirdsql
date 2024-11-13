import sys
from firebirdsql.tests.test_basic import *      # noqa
from firebirdsql.tests.test_proc import *       # noqa
from firebirdsql.tests.test_issues import *     # noqa
from firebirdsql.tests.test_backup import *     # noqa
from firebirdsql.tests.test_services import *   # noqa
from firebirdsql.tests.test_event import *      # noqa
from firebirdsql.tests.test_arc4 import *       # noqa
from firebirdsql.tests.test_chacha import *     # noqa
from firebirdsql.tests.test_auth import *       # noqa
from firebirdsql.tests.test_srp import *        # noqa
from firebirdsql.tests.test_utils import *      # noqa

if sys.version_info[0] > 2:
    from firebirdsql.tests.test_async import *  # noqa

if sys.version_info >= (3, 6):
    from firebirdsql.tests.test_timezone import *   # noqa

if __name__ == "__main__":
    import unittest
    unittest.main()
