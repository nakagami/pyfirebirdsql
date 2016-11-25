##############################################################################
# Copyright (c) 2014-2016, Hajime Nakagami<nakagami@gmail.com>
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

PYTHON_MAJOR_VER = sys.version_info[0]

if PYTHON_MAJOR_VER == 3:
    def ord(c):
        return c


class ARC4:
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
        enc = b''
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

    # PyCrypto compatible method
    @staticmethod
    def new(key):
        return ARC4(key)
    encrypt = translate
    decrypt = translate
