##############################################################################
# Copyright (c) 2009-2023, Hajime Nakagami<nakagami@gmail.com>
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
import socket
import zlib

try:
    import fcntl
except ImportError:
    def setcloexec(sock):
        pass
else:
    def setcloexec(sock):
        """Set FD_CLOEXEC property on a file descriptors
        """
        fd = sock.fileno()
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


class SocketStream(object):
    def __init__(self, host, port, timeout=None, cloexec=False):
        self._sock = socket.create_connection((host, port), timeout)
        if cloexec:
            setcloexec(self._sock)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.read_translator = None
        self.write_translator = None
        self._compressor = None
        self._decompressor = None
        self._recv_buf = b''

    def enable_compression(self):
        """Enable zlib wire compression for the stream.
        Called after the server accepts compression during protocol negotiation.
        """
        self._compressor = zlib.compressobj()
        self._decompressor = zlib.decompressobj()
        self._recv_buf = b''

    def recv(self, nbytes):
        if self._decompressor:
            while len(self._recv_buf) < nbytes:
                b = self._sock.recv(max(nbytes, 8192))
                if not b:
                    break
                if self.read_translator:
                    b = self.read_translator.decrypt(b)
                self._recv_buf += self._decompressor.decompress(b)
            result = self._recv_buf[:nbytes]
            self._recv_buf = self._recv_buf[nbytes:]
            return result
        else:
            b = self._sock.recv(nbytes)
            if self.read_translator:
                b = self.read_translator.decrypt(b)
            return b

    def send(self, b):
        if self._compressor:
            b = self._compressor.compress(b) + self._compressor.flush(zlib.Z_SYNC_FLUSH)
        if self.write_translator:
            b = self.write_translator.encrypt(b)
        n = 0
        while (n < len(b)):
            n += self._sock.send(b[n:])

    def close(self):
        self._sock.close()

    def set_translator(self, read_translator, write_translator):
        self.read_translator = read_translator
        self.write_translator = write_translator
