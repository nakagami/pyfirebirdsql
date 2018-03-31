##############################################################################
# Copyright (c) 2018, Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#  this list of conditions and the following disclaimer in the documentation
#  and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Python DB-API 2.0 module for Firebird.
##############################################################################
import sys
from decimal import Decimal

PYTHON_MAJOR_VER = sys.version_info[0]


if PYTHON_MAJOR_VER == 3:
    def ord(c):
        return c


def bytes2long(b):
    n = 0
    for c in b:
        n <<= 8
        n += ord(c)
    return n


def dpd_to_int(dpd):
    """
    Convert DPD encodined value to int (0-999)
    dpd: DPD encoded value. 10bit unsigned int
    """
    b = [None] * 10
    b[9] = 1 if dpd & 0b1000000000 else 0
    b[8] = 1 if dpd & 0b0100000000 else 0
    b[7] = 1 if dpd & 0b0010000000 else 0
    b[6] = 1 if dpd & 0b0001000000 else 0
    b[5] = 1 if dpd & 0b0000100000 else 0
    b[4] = 1 if dpd & 0b0000010000 else 0
    b[3] = 1 if dpd & 0b0000001000 else 0
    b[2] = 1 if dpd & 0b0000000100 else 0
    b[1] = 1 if dpd & 0b0000000010 else 0
    b[0] = 1 if dpd & 0b0000000001 else 0

    d = [None] * 3
    if b[3] == 0:
        d[2] = b[9] * 4 + b[8] * 2 + b[7]
        d[1] = b[6] * 4 + b[5] * 2 + b[4]
        d[0] = b[2] * 4 + b[1] * 2 + b[0]
    elif (b[3], b[2], b[1]) == (1, 0, 0):
        d[2] = b[9] * 4 + b[8] * 2 + b[7]
        d[1] = b[6] * 4 + b[5] * 2 + b[4]
        d[0] = 8 + b[0]
    elif (b[3], b[2], b[1]) == (1, 0, 1):
        d[2] = b[9] * 4 + b[8] * 2 + b[7]
        d[1] = 8 + b[4]
        d[0] = b[6] * 4 + b[5] * 2 + b[0]
    elif (b[3], b[2], b[1]) == (1, 1, 0):
        d[2] = 8 + b[7]
        d[1] = b[6] * 4 + b[5] * 2 + b[4]
        d[0] = b[9] * 4 + b[8] * 2 + b[0]
    elif (b[6], b[5], b[3], b[2], b[1]) == (0, 0, 1, 1, 1):
        d[2] = 8 + b[7]
        d[1] = 8 + b[4]
        d[0] = b[9] * 4 + b[8] * 2 + b[0]
    elif (b[6], b[5], b[3], b[2], b[1]) == (0, 1, 1, 1, 1):
        d[2] = 8 + b[7]
        d[1] = b[9] * 4 + b[8] * 2 + b[4]
        d[0] = 8 + b[0]
    elif (b[6], b[5], b[3], b[2], b[1]) == (1, 0, 1, 1, 1):
        d[2] = b[9] * 4 + b[8] * 2 + b[7]
        d[1] = 8 + b[4]
        d[0] = 8 + b[0]
    elif (b[6], b[5], b[3], b[2], b[1]) == (1, 1, 1, 1, 1):
        d[2] = 8 + b[7]
        d[1] = 8 + b[4]
        d[0] = 8 + b[0]
    else:
        raise ValueError('Invalid DPD encoding')

    return d[2] * 100 + d[1] * 10 + d[0]


def calc_significand(prefix, dpd_bits, num_bits):
    """
    prefix: High bits integer value
    dpd_bits: dpd encoded bits
    num_bits: bit length of dpd_bits
    """
    # https://en.wikipedia.org/wiki/Decimal128_floating-point_format#Densely_packed_decimal_significand_field
    num_segments = num_bits // 10
    segments = []
    for i in range(num_segments):
        segments.append(dpd_bits & 0b1111111111)
        dpd_bits >>= 10
    segments.reverse()

    v = prefix
    for dpd in segments:
        v = v * 1000 + dpd_to_int(dpd)

    return v


def _decimal128_to_sign_digits_exponent(b):
    # https://en.wikipedia.org/wiki/Decimal128_floating-point_format
    sign = 1 if ord(b[0]) & 0x80 else 0
    combination_field = ((ord(b[0]) & 0x7f) << 10) + (ord(b[1]) << 2) + (ord(b[2]) >> 6)
    if (combination_field & 0b11111000000000000) == 0b11111000000000000:
        if sign:
            return Decimal('-NaN')
        else:
            return Decimal('NaN')
    elif (combination_field & 0b11111000000000000) == 0b11110000000000000:
        if sign:
            return Decimal('-Infinity')
        else:
            return Decimal('Infinity')
    elif (combination_field & 0b11000000000000000) == 0b00000000000000000:
        exponent = 0b00000000000000 + (combination_field & 0b111111111111)
        significand_prefix = (combination_field >> 12) & 0b111
    elif (combination_field & 0b11000000000000000) == 0b01000000000000000:
        exponent = 0b01000000000000 + (combination_field & 0b111111111111)
        significand_prefix = (combination_field >> 12) & 0b111
    elif (combination_field & 0b11000000000000000) == 0b10000000000000000:
        exponent = 0b10000000000000 + (combination_field & 0b111111111111)
        significand_prefix = (combination_field >> 12) & 0b111
    elif (combination_field & 0b11110000000000000) == 0b11000000000000000:
        exponent = 0b00000000000000 + (combination_field & 0b111111111111)
        significand_prefix = 8 + (combination_field >> 12) & 0b1
    elif (combination_field & 0b11110000000000000) == 0b11010000000000000:
        exponent = 0b01000000000000 + (combination_field & 0b111111111111)
        significand_prefix = 8 + (combination_field >> 12) & 0b1
    elif (combination_field & 0b11110000000000000) == 0b11100000000000000:
        exponent = 0b10000000000000 + (combination_field & 0b111111111111)
        significand_prefix = 8 + (combination_field >> 12) & 0b1
    else:
        raise ValueError('decimal128 value error')
    exponent -= 6176

    dpd_bits = bytes2long(b) & 0b11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
    digits = calc_significand(significand_prefix, dpd_bits, 110)
    return sign, digits, exponent


def decimal_fixed_to_decimal(b, scale):
    v = _decimal128_to_sign_digits_exponent(b)
    if isinstance(v, Decimal):
        return v
    sign, digits, _ = v
    return Decimal((sign, Decimal(digits).as_tuple()[1], scale))


def decimal64_to_decimal(b):
    "decimal64 bytes to Decimal"
    # https://en.wikipedia.org/wiki/Decimal64_floating-point_format
    sign = 1 if ord(b[0]) & 0x80 else 0
    combination_field = (ord(b[0]) >> 2) & 0b11111
    exponent = ((ord(b[0]) & 0b11) << 6) + ((ord(b[1]) >> 2) & 0b111111)
    dpd_bits = bytes2long(b) & 0b11111111111111111111111111111111111111111111111111

    if combination_field == 0b11111:
        if sign:
            return Decimal('-NaN')
        else:
            return Decimal('NaN')
    elif combination_field == 0b11110:
        if sign:
            return Decimal('-Infinity')
        else:
            return Decimal('Infinity')
    elif (combination_field & 0b11000) == 0b00000:
        exponent = 0b0000000000 + exponent
        significand_prefix = combination_field & 0b111
    elif (combination_field & 0b11000) == 0b01000:
        exponent = 0b0100000000 + exponent
        significand_prefix = combination_field & 0b111
    elif (combination_field & 0b11000) == 0b10000:
        exponent = 0b1000000000 + exponent
        significand_prefix = combination_field & 0b111
    elif (combination_field & 0b11110) == 0b11000:
        exponent = 0b0000000000 + exponent
        significand_prefix = 8 + combination_field & 0b1
    elif (combination_field & 0b11110) == 0b11010:
        exponent = 0b0100000000 + exponent
        significand_prefix = 8 + combination_field & 0b1
    elif (combination_field & 0b11110) == 0b11100:
        exponent = 0b1000000000 + exponent
        significand_prefix = 8 + combination_field & 0b1
    else:
        raise ValueError('decimal64 value error')
    digits = calc_significand(significand_prefix, dpd_bits, 50)
    exponent -= 398
    return Decimal((sign, Decimal(digits).as_tuple()[1], exponent))


def decimal128_to_decimal(b):
    "decimal128 bytes to Decimal"
    v = _decimal128_to_sign_digits_exponent(b)
    if isinstance(v, Decimal):
        return v
    sign, digits, exponent = v
    return Decimal((sign, Decimal(digits).as_tuple()[1], exponent))
