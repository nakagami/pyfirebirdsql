##############################################################################
# Copyright (c) 2009-2025, Hajime Nakagami<nakagami@gmail.com>
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
import asyncio
import zlib

from firebirdsql.stream import SocketStream
from firebirdsql.utils import bytes_to_bint


class AsyncSocketStream(SocketStream):
    def __init__(self, host, port, loop, timeout, cloexec):
        super().__init__(host, port, timeout, cloexec)
        self.loop = loop
        self._send_lock = asyncio.Lock()
        self._last_send_task = None
        self._sock.setblocking(False)
        self._buf = b''

    async def _await_pending_send(self):
        task = self._last_send_task
        if task is not None:
            await task

    async def async_recv(self, nbytes):
        await self._await_pending_send()

        if self._decompressor:
            while len(self._buf) < nbytes:
                read_size = max(8192, nbytes - len(self._buf))
                chunk = await self.loop.sock_recv(self._sock, read_size)
                if not chunk:
                    break
                if self.read_translator:
                    chunk = self.read_translator.decrypt(chunk)
                self._buf += self._decompressor.decompress(chunk)
        else:
            if len(self._buf) < nbytes:
                read_size = max(8192, nbytes - len(self._buf))
                chunk = await self.loop.sock_recv(self._sock, read_size)
                if self.read_translator:
                    chunk = self.read_translator.decrypt(chunk)
                self._buf += chunk

        ret = self._buf[:nbytes]
        self._buf = self._buf[nbytes:]
        return ret

    def send(self, b):
        if not self.loop.is_running():
            return super().send(b)
        if self._compressor:
            b = self._compressor.compress(b) + self._compressor.flush(zlib.Z_SYNC_FLUSH)
        if self.write_translator:
            b = self.write_translator.encrypt(b)

        previous_task = self._last_send_task

        async def _send_all(payload):
            if previous_task is not None:
                await previous_task
            async with self._send_lock:
                await self.loop.sock_sendall(self._sock, payload)

        self._last_send_task = self.loop.create_task(_send_all(b))
        return None
