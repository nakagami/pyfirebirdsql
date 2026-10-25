import unittest
from firebirdsql.arc4 import ARC4


class TestArc4(unittest.TestCase):
    def test_arc4(self):
        a1 = ARC4.new(b'a key')
        enc = a1.translate(b'plain text')
        self.assertEqual(enc, b'\x4b\x4b\xdc\x65\x02\xb3\x08\x17\x48\x82')
        a2 = ARC4.new(b'a key')
        plain = a2.translate(enc)
        self.assertEqual(plain, b'plain text')

    def test_chunks(self):
        data = bytes(i % 251 for i in range(5000))
        whole = ARC4.new(b'a key').translate(data)
        a = ARC4.new(b'a key')
        self.assertEqual(a.translate(data[:1]) + a.translate(data[1:1000]) + a.translate(data[1000:]), whole)
