##############################################################################
# Copyright (c) 2013 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################

def hex_to_bytes(s):
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
