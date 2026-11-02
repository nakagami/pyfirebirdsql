import unittest
from firebirdsql import decfloat


def encode_dpd(n):
    """Encode 0-999 as a 10-bit densely packed decimal declet (IEEE 754-2008 table)."""
    d2, d1, d0 = n // 100, n // 10 % 10, n % 10
    a, b, c, d = (d2 >> 3) & 1, (d2 >> 2) & 1, (d2 >> 1) & 1, d2 & 1
    e, f, g, h = (d1 >> 3) & 1, (d1 >> 2) & 1, (d1 >> 1) & 1, d1 & 1
    i, j, k, m = (d0 >> 3) & 1, (d0 >> 2) & 1, (d0 >> 1) & 1, d0 & 1
    bits = {
        (0, 0, 0): (b, c, d, f, g, h, 0, j, k, m),
        (0, 0, 1): (b, c, d, f, g, h, 1, 0, 0, m),
        (0, 1, 0): (b, c, d, j, k, h, 1, 0, 1, m),
        (1, 0, 0): (j, k, d, f, g, h, 1, 1, 0, m),
        (1, 1, 0): (j, k, d, 0, 0, h, 1, 1, 1, m),
        (1, 0, 1): (f, g, d, 0, 1, h, 1, 1, 1, m),
        (0, 1, 1): (b, c, d, 1, 0, h, 1, 1, 1, m),
        (1, 1, 1): (0, 0, d, 1, 1, h, 1, 1, 1, m),
    }[(a, e, i)]
    v = 0
    for x in bits:
        v = (v << 1) | x
    return v


class TestDecfloat(unittest.TestCase):
    def test_dpd(self):
        for n in range(1000):
            self.assertEqual(decfloat.dpd_to_int(encode_dpd(n)), n)
        # the lookup table matches the decoder for every 10-bit value,
        # including the non-canonical ones
        self.assertEqual(list(decfloat._DPD_TO_INT), [decfloat.dpd_to_int(i) for i in range(1024)])

    def test_calc_significand(self):
        # declets follow the prefix digit, most significant first
        bits = (encode_dpd(123) << 20) | (encode_dpd(989) << 10) | encode_dpd(7)
        self.assertEqual(decfloat.calc_significand(4, bits, 30), 4123989007)
        bits = 0
        for n in (1, 234, 567, 890, 123, 456, 789, 12, 345, 678, 901):
            bits = (bits << 10) | encode_dpd(n)
        self.assertEqual(decfloat.calc_significand(5, bits, 110), 5001234567890123456789012345678901)
