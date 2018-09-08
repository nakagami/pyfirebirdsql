##############################################################################
# Copyright (c) 2014-2018, Hajime Nakagami<nakagami@gmail.com>
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
import binascii
import struct
from firebirdsql import InternalError

PYTHON_MAJOR_VER = sys.version_info[0]


def bs(byte_array):
    if PYTHON_MAJOR_VER == 2:
        return ''.join([chr(c) for c in byte_array])
    else:
        return bytes(byte_array)


def hex_to_bytes(s):
    """
    convert hex string to bytes
    """
    if len(s) % 2:
        s = b'0' + s
    ia = [int(s[i:i+2], 16) for i in range(0, len(s), 2)]   # int array
    return bs(ia) if PYTHON_MAJOR_VER == 3 else b''.join([chr(c) for c in ia])


def bytes_to_hex(b):
    """
    convert bytes to hex string
    """
    s = binascii.b2a_hex(b)

    return s


def bytes_to_bint(b, u=False):           # Read as big endian
    if u:
        fmtmap = {1: 'B', 2: '>H', 4: '>L', 8: '>Q'}
    else:
        fmtmap = {1: 'b', 2: '>h', 4: '>l', 8: '>q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bytes_to_int(b):        # Read as little endian.
    fmtmap = {1: 'b', 2: '<h', 4: '<l', 8: '<q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bint_to_bytes(val, nbytes):     # Convert int value to big endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8 * (nbytes - n - 1)) & 0xff))
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[-1] += 1
        for i in range(nbytes):
            if b[nbytes - i - 1] == 256:
                b[nbytes - i - 1] = 0
                b[nbytes - i - 2] += 1
    return bs(b)


def int_to_bytes(val, nbytes):  # Convert int value to little endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8 * n)) & 0xff)
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[0] += 1
        for i in range(nbytes):
            if b[i] == 256:
                b[i] = 0
                b[i+1] += 1
    return bs(b)


def byte_to_int(b):
    "byte to int"
    if PYTHON_MAJOR_VER == 3:
        return b
    else:
        return ord(b)
