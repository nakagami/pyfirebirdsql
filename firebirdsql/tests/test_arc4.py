import unittest
from firebirdsql.tests import base
from firebirdsql.arc4 import Arc4

class TestArc4(base.TestBase):
    def test_arc4(self):
        a1 = Arc4(b'a key')
        enc = a1.translate(b'plain text')
        self.assertEqual(enc, b'\x4b\x4b\xdc\x65\x02\xb3\x08\x17\x48\x82')
        a2 = Arc4(b'a key')
        plain = a2.translate(enc)
        self.assertEqual(plain, b'plain text')

