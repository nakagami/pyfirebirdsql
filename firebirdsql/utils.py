##############################################################################
# Copyright (c) 2014 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import sys
import binascii
import struct

PYTHON_MAJOR_VER = sys.version_info[0]

if PYTHON_MAJOR_VER == 2:
    def bytes(byte_array):
        return ''.join([chr(c) for c in byte_array])


def hex_to_bytes(s):
    """
    convert hex string to bytes
    """
    if len(s) % 2:
        s = b'0' + s
    ia = [int(s[i:i+2], 16) for i in range(0, len(s), 2)]   # int array
    return bytes(ia) if PYTHON_MAJOR_VER == 3 else b''.join([chr(c) for c in ia])

def hex_to_bytes2(s):
    """
    convert hex string to bytes
    """
    ia = [int(s[i:i+2], 16) for i in range(0, len(s), 2)]   # int array
    b = bytes(ia) if PYTHON_MAJOR_VER == 3 else b''.join([chr(c) for c in ia])
    ia = [int(s[i:i+2], 16) for i in range(0, len(s), 2)]   # int array
    return bytes(ia) if PYTHON_MAJOR_VER == 3 else b''.join([chr(c) for c in ia])

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

def bytes_to_int(b, u=False):            # Read as little endian.
    if u:
        fmtmap = {1: 'B', 2: '<H', 4: '<L', 8: '<Q'}
    else:
        fmtmap = {1: 'b', 2: '<h', 4: '<l', 8: '<q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bint_to_bytes(val, nbytes): # Convert int value to big endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8*(nbytes - n - 1)) & 0xff))
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[-1] += 1
        for i in range(nbytes):
            if b[nbytes -i -1] == 256:
                b[nbytes -i -1] = 0
                b[nbytes -i -2] += 1
    return bytes(b)

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
    return bytes(b)

def byte_to_int(b):
    "byte to int"
    if PYTHON_MAJOR_VER == 3:
        return b
    else:
        return ord(b)

