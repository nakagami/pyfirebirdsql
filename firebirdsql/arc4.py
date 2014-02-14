##############################################################################
# Copyright (c) 2014 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import sys

PYTHON_MAJOR_VER = sys.version_info[0]

if PYTHON_MAJOR_VER == 3:
    def ord(c):
        return c

class Arc4:
    def __init__(self, key):
        state = list(range(256))
        index1 = 0
        index2 = 0

        for i in range(256):
            index2 = (ord(key[index1]) + state[i] + index2) % 256
            (state[i], state[index2]) = (state[index2], state[i])
            index1 = (index1 + 1) % len(key)

        self.state = state
        self.x = 0
        self.y = 0

    def translate(self, plain):
        state = self.state
        enc=b''
        for i in range(len(plain)):
            self.x = (self.x + 1) % 256
            self.y = (self.y + state[self.x]) % 256
            (state[self.x], state[self.y]) = (state[self.y], state[self.x])
            xorIndex = (state[self.x]+state[self.y]) % 256
            if PYTHON_MAJOR_VER == 3:
                enc += bytes([plain[i] ^ state[xorIndex]])
            else:
                enc += chr(ord(plain[i]) ^ state[xorIndex])
        return enc
