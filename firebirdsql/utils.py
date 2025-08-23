##############################################################################
# Copyright (c) 2014-2025, Hajime Nakagami<nakagami@gmail.com>
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
import binascii
import struct
from urllib.parse import urlparse
from collections.abc import Mapping
from firebirdsql.err import InternalError

DEBUG_LEVEL = 0


def enable_debug_print(verbose=False):
    global DEBUG_LEVEL
    DEBUG_LEVEL = 2 if verbose else 1


def disable_debug_print():
    global DEBUG
    DEBUG_LEVEL = 0


def debug_level():
    return DEBUG_LEVEL


def hex_to_bytes(s):
    """
    convert hex string to bytes
    """
    if len(s) % 2:
        s = b'0' + s
    ia = [int(s[i:i+2], 16) for i in range(0, len(s), 2)]   # int array
    return bytes(ia)


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
        if len(b) == 16:
            if u:
                a, b = struct.unpack('>QQ', b)
            else:
                a, b = struct.unpack('>qq', b)
            return (a << 64) | b

        raise InternalError("Invalid bytes length:%d" % (len(b), ))
    return struct.unpack(fmt, b)[0]


def bytes_to_int(b):        # Read as little endian.
    fmtmap = {1: 'b', 2: '<h', 4: '<l', 8: '<q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError("Invalid bytes length:%d" % (len(b), ))
    return struct.unpack(fmt, b)[0]


def bytes_to_uint(b):        # Read as little endian unsigned int.
    fmtmap = {1: 'B', 2: '<H', 4: '<L', 8: '<Q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError("Invalid bytes length:%d" % (len(b), ))
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


def parse_dsn(dsn, host=None, port=None, database=None, user=None, password=None):
    if dsn:
        parsed = urlparse("//" + dsn)
        if host is None and parsed.hostname is not None:
            host = parsed.hostname
        if port is None and parsed.port is not None:
            port = parsed.port
        if database is None and parsed.path is not None:
            database = parsed.path
            assert(database[0] == '/')
            if '/' not in database[1:]:
                database = database[1:]
            elif database[2] == ':':     # Windows drive letter
                database = database[1:]
        if user is None and parsed.username is not None:
            user = parsed.username
        if password is None and parsed.password is not None:
            password = parsed.password
    if host is None:
        host = 'localhost'
    if port is None:
        port = 3050

    return host, port, database, user, password


def guess_wire_crypt(b):
    available_plugins = []
    plugin_nonce = []
    i = 0
    while i < len(b):
        t = b[i]
        i += 1
        ln = b[i]
        i += 1
        v = b[i:i+ln]
        i += ln
        if t == 0:
            assert v == b"Symmetric"
        elif t == 1:
            available_plugins = v.split()
        elif t == 3:
            plugin_nonce.append(v)

    if b'ChaCha64' in available_plugins:
        for s in plugin_nonce:
            if s[:9] == b"ChaCha64\x00":
                return (b'ChaCha64', s[9:])
    if b'ChaCha' in available_plugins:
        for s in plugin_nonce:
            if s[:7] == b"ChaCha\x00":
                return (b'ChaCha', s[7:7 + 12])
    elif b'Arc4' in available_plugins:
        return (b'Arc4', None)
    return None, None


class RowMapping(Mapping):
    """dict like interface to result rows
    """
    __slots__ = ("_description", "_fields")

    def __init__(self, row, description):
        self._fields = fields = {}
        # result may contain multiple fields with the same name. The
        # RowMapping API ignores these additional fields.
        for i, descr in enumerate(description):
            fields.setdefault(descr[0], row[i])
        self._description = description

    def __getitem__(self, key):
        fields = self._fields
        # try unnormalized key first
        try:
            return fields[key]
        except KeyError:
            pass

        # normalize field name
        if key[0] == '"' and key[-1] == '"':
            # field names in quotes are case sensitive
            normkey = key[1:-1]
        else:
            # default is all upper case fields
            normkey = key.upper()

        try:
            return fields[normkey]
        except KeyError:
            raise KeyError("RowMapping has no field names '%s'. Available "
                           "field names are: %s" %
                           (key, ", ".join(self.keys())))

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __repr__(self):
        fields = self._fields
        values = ["%s=%r" % (desc[0], fields[desc[0]])
                  for desc in self._description]
        return ("<RowMapping at 0x%08x with fields: %s>" %
                (id(self), ", ".join(values)))
