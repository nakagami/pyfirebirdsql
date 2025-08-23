##############################################################################
# Copyright (c) 2021-2025 Hajime Nakagami<nakagami@gmail.com>
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
import struct
import copy

sigma = b"expand 32-byte k"


def bytes_to_uint(b):        # Read as little endian.
    fmt = {1: 'B', 2: '<H', 4: '<L', 8: '<Q'}[len(b)]
    return struct.unpack(fmt, b)[0]


def int_to_bytes(val, nbytes):  # Convert int value to little endian bytes.
    v = abs(val)
    byte_array = []
    for n in range(nbytes):
        byte_array.append((v >> (8 * n)) & 0xff)
    if val < 0:
        for i in range(nbytes):
            byte_array[i] = ~byte_array[i] + 256
        byte_array[0] += 1
        for i in range(nbytes):
            if byte_array[i] == 256:
                byte_array[i] = 0
                byte_array[i+1] += 1

    return bytes(byte_array)


def add_u32(x, y):
    return (x + y) & 0xffffffff


def rotate_u32(x, n):
    y = x << n
    z = x >> (32 - n)
    return (y | z) & 0xffffffff


def quaterround_u32(a, b, c, d):
    a = add_u32(a, b)
    d ^= a
    d = rotate_u32(d, 16)

    c = add_u32(c, d)
    b ^= c
    b = rotate_u32(b, 12)

    a = add_u32(a, b)
    d ^= a
    d = rotate_u32(d, 8)

    c = add_u32(c, d)
    b ^= c
    b = rotate_u32(b, 7)

    return a, b, c, d


class ChaCha20:
    def __init__(self, key, nonce, counter=0):
        assert len(key) == 32
        assert len(nonce) in (8, 12)
        self.nonce = nonce
        self.counter = counter
        counter_bytes = int_to_bytes(self.counter, 16 - len(self.nonce))
        block_bytes = sigma + key + counter_bytes + self.nonce
        assert len(block_bytes) == 64

        state = []
        for i in range(0, len(block_bytes), 4):
            state.append(bytes_to_uint(block_bytes[i:i+4]))
        self.state = state
        self.set_chacha20_round_block()

    def set_chacha20_round_block(self):
        x = copy.copy(self.state)

        for i in range(10):
            # column rounds
            x[0], x[4], x[8], x[12] = quaterround_u32(x[0], x[4], x[8], x[12])
            x[1], x[5], x[9], x[13] = quaterround_u32(x[1], x[5], x[9], x[13])
            x[2], x[6], x[10], x[14] = quaterround_u32(x[2], x[6], x[10], x[14])
            x[3], x[7], x[11], x[15] = quaterround_u32(x[3], x[7], x[11], x[15])
            # diagonal rounds
            x[0], x[5], x[10], x[15] = quaterround_u32(x[0], x[5], x[10], x[15])
            x[1], x[6], x[11], x[12] = quaterround_u32(x[1], x[6], x[11], x[12])
            x[2], x[7], x[8], x[13] = quaterround_u32(x[2], x[7], x[8], x[13])
            x[3], x[4], x[9], x[14] = quaterround_u32(x[3], x[4], x[9], x[14])

        for i in range(16):
            x[i] = add_u32(x[i], self.state[i])

        self.block =  b''.join([int_to_bytes(i, 4) for i in x])
        self.block_pos = 0

    def translate(self, plain):
        enc = b''

        for i in range(len(plain)):
            enc += bytes([plain[i] ^ self.block[self.block_pos]])
            self.block_pos += 1
            if len(self.block) == self.block_pos:
                # increment counter
                self.counter += 1
                counter_bytes = int_to_bytes(self.counter, 16 - len(self.nonce))
                self.state[12] = bytes_to_uint(counter_bytes[:4])
                if len(self.nonce) == 8:
                    # ChaCha64: 64 bit nonce, 64 bit counter
                    self.state[13] = bytes_to_uint(counter_bytes[4:])
                self.set_chacha20_round_block()

        return enc

    # PyCrypto compatible method
    @staticmethod
    def new(key, nonce):
        return ChaCha20(key, nonce)
    encrypt = translate
    decrypt = translate
