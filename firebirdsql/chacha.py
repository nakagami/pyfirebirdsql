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

sigma = b"expand 32-byte k"

_SIGMA = struct.unpack('<4L', sigma)

_AHEAD_BLOCKS = 64    # key stream computed ahead: 4 KB, about 0.25 ms


def chacha20_blocks(head, counters, nonce):
    """Return the key stream blocks for ``counters`` (block numbers, already
    reduced to the counter size) as bytes. ``head`` is the 12 state words
    before the counter and ``nonce`` the nonce words.

    All blocks are computed at once: each Python int holds the same state word
    of every block, one block per 64-bit lane. Words stay below 2**32, so
    additions cannot carry into the next lane, and the mask keeps rotations
    inside each lane. Every operation then runs over all blocks in C.
    """
    n = len(counters)
    ones = int.from_bytes(b'\x01\x00\x00\x00\x00\x00\x00\x00' * n, 'little')
    m = ones * 0xffffffff
    ctr = int.from_bytes(struct.pack('<%dQ' % n, *counters), 'little')
    if len(nonce) == 3:      # 32-bit counter
        state = [w * ones for w in head] + [ctr & m] + [w * ones for w in nonce]
    else:                    # 64-bit counter
        state = [w * ones for w in head] + [ctr & m, (ctr >> 32) & m] + [w * ones for w in nonce]
    (x0, x1, x2, x3, x4, x5, x6, x7,
     x8, x9, x10, x11, x12, x13, x14, x15) = state
    for _ in range(10):
        # column rounds
        x0 = (x0 + x4) & m
        x12 ^= x0
        x12 = ((x12 << 16) | (x12 >> 16)) & m
        x8 = (x8 + x12) & m
        x4 ^= x8
        x4 = ((x4 << 12) | (x4 >> 20)) & m
        x0 = (x0 + x4) & m
        x12 ^= x0
        x12 = ((x12 << 8) | (x12 >> 24)) & m
        x8 = (x8 + x12) & m
        x4 ^= x8
        x4 = ((x4 << 7) | (x4 >> 25)) & m
        x1 = (x1 + x5) & m
        x13 ^= x1
        x13 = ((x13 << 16) | (x13 >> 16)) & m
        x9 = (x9 + x13) & m
        x5 ^= x9
        x5 = ((x5 << 12) | (x5 >> 20)) & m
        x1 = (x1 + x5) & m
        x13 ^= x1
        x13 = ((x13 << 8) | (x13 >> 24)) & m
        x9 = (x9 + x13) & m
        x5 ^= x9
        x5 = ((x5 << 7) | (x5 >> 25)) & m
        x2 = (x2 + x6) & m
        x14 ^= x2
        x14 = ((x14 << 16) | (x14 >> 16)) & m
        x10 = (x10 + x14) & m
        x6 ^= x10
        x6 = ((x6 << 12) | (x6 >> 20)) & m
        x2 = (x2 + x6) & m
        x14 ^= x2
        x14 = ((x14 << 8) | (x14 >> 24)) & m
        x10 = (x10 + x14) & m
        x6 ^= x10
        x6 = ((x6 << 7) | (x6 >> 25)) & m
        x3 = (x3 + x7) & m
        x15 ^= x3
        x15 = ((x15 << 16) | (x15 >> 16)) & m
        x11 = (x11 + x15) & m
        x7 ^= x11
        x7 = ((x7 << 12) | (x7 >> 20)) & m
        x3 = (x3 + x7) & m
        x15 ^= x3
        x15 = ((x15 << 8) | (x15 >> 24)) & m
        x11 = (x11 + x15) & m
        x7 ^= x11
        x7 = ((x7 << 7) | (x7 >> 25)) & m
        # diagonal rounds
        x0 = (x0 + x5) & m
        x15 ^= x0
        x15 = ((x15 << 16) | (x15 >> 16)) & m
        x10 = (x10 + x15) & m
        x5 ^= x10
        x5 = ((x5 << 12) | (x5 >> 20)) & m
        x0 = (x0 + x5) & m
        x15 ^= x0
        x15 = ((x15 << 8) | (x15 >> 24)) & m
        x10 = (x10 + x15) & m
        x5 ^= x10
        x5 = ((x5 << 7) | (x5 >> 25)) & m
        x1 = (x1 + x6) & m
        x12 ^= x1
        x12 = ((x12 << 16) | (x12 >> 16)) & m
        x11 = (x11 + x12) & m
        x6 ^= x11
        x6 = ((x6 << 12) | (x6 >> 20)) & m
        x1 = (x1 + x6) & m
        x12 ^= x1
        x12 = ((x12 << 8) | (x12 >> 24)) & m
        x11 = (x11 + x12) & m
        x6 ^= x11
        x6 = ((x6 << 7) | (x6 >> 25)) & m
        x2 = (x2 + x7) & m
        x13 ^= x2
        x13 = ((x13 << 16) | (x13 >> 16)) & m
        x8 = (x8 + x13) & m
        x7 ^= x8
        x7 = ((x7 << 12) | (x7 >> 20)) & m
        x2 = (x2 + x7) & m
        x13 ^= x2
        x13 = ((x13 << 8) | (x13 >> 24)) & m
        x8 = (x8 + x13) & m
        x7 ^= x8
        x7 = ((x7 << 7) | (x7 >> 25)) & m
        x3 = (x3 + x4) & m
        x14 ^= x3
        x14 = ((x14 << 16) | (x14 >> 16)) & m
        x9 = (x9 + x14) & m
        x4 ^= x9
        x4 = ((x4 << 12) | (x4 >> 20)) & m
        x3 = (x3 + x4) & m
        x14 ^= x3
        x14 = ((x14 << 8) | (x14 >> 24)) & m
        x9 = (x9 + x14) & m
        x4 ^= x9
        x4 = ((x4 << 7) | (x4 >> 25)) & m

    out = bytearray(64 * n)
    for i, (x, s) in enumerate(zip(
            (x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15),
            state)):
        # word i of every block: the low 4 bytes of each 8-byte lane
        lanes = ((x + s) & m).to_bytes(8 * n, 'little')
        for b in range(4):
            out[4 * i + b::64] = lanes[b::8]
    return bytes(out)


class ChaCha20:
    """ChaCha20 stream cipher.

    A 12-byte nonce gives RFC 7539 ChaCha20 (32-bit block counter, Firebird's
    ``ChaCha`` plugin); an 8-byte nonce gives the original variant with a
    64-bit counter (Firebird's ``ChaCha64``).
    """

    def __init__(self, key, nonce, counter=0):
        assert len(key) == 32
        assert len(nonce) in (8, 12)
        self.nonce = nonce
        self.counter = counter      # number of the next key stream block
        self._head = _SIGMA + struct.unpack('<8L', key)
        self._nonce_words = struct.unpack('<%dL' % (len(nonce) // 4), nonce)
        self._rest = b''            # unused key stream of the last block

    def _keystream(self, nblocks):
        counter = self.counter
        self.counter = counter + nblocks
        mask = 0xffffffff if len(self._nonce_words) == 3 else 0xffffffffffffffff
        counters = [c & mask for c in range(counter, counter + nblocks)]
        return chacha20_blocks(self._head, counters, self._nonce_words)

    def translate(self, plain):
        n = len(plain)
        if n == 0:
            return b''
        stream = self._rest
        if len(stream) < n:
            stream += self._keystream(max(_AHEAD_BLOCKS, (n - len(stream) + 63) // 64))
        self._rest = stream[n:]
        # XOR the whole message at once as one big integer
        return (int.from_bytes(plain, 'little') ^
                int.from_bytes(stream[:n], 'little')).to_bytes(n, 'little')

    # PyCrypto compatible method
    @staticmethod
    def new(key, nonce):
        return ChaCha20(key, nonce)
    encrypt = translate
    decrypt = translate
