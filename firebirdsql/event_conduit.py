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
import select
from firebirdsql.err import InternalError, OperationalError
from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql.wireprotocol import WireProtocol
from firebirdsql.stream import SocketStream


class EventConduit(WireProtocol):
    def _recv_channel(self, nbytes, timeout):
        n = nbytes
        if n % 4:
            n += 4 - nbytes % 4  # 4 bytes word alignment
        r = bytes([])
        while n:
            if (timeout is not None and select.select([self.sock._sock], [], [], timeout)[0] == []):
                break
            b = self.sock.recv(n)
            if not b:
                break
            r += b
            n -= len(b)
        if len(r) < nbytes:
            raise OperationalError('Can not recv() packets')
        return r[:nbytes]

    def _wait_for_event(self, timeout):
        event_count = {}
        event_id = 0
        while True:
            op_code = bytes_to_bint(self._recv_channel(4, timeout))
            if op_code == self.op_dummy:
                pass
            elif op_code == self.op_exit or op_code == self.op_disconnect:
                break
            elif op_code == self.op_event:
                bytes_to_int(self._recv_channel(4, timeout))    # db_handle
                ln = bytes_to_bint(self._recv_channel(4, timeout))
                b = self._recv_channel(ln, timeout)
                assert b[0] == 1
                i = 1
                while i < len(b):
                    ln = b[i]
                    s = self.connection.bytes_to_str(b[i+1:i+1+ln])
                    n = bytes_to_int(b[i+1+ln:i+1+ln+4])
                    event_count[s] = n
                    i += ln + 5
                self._recv_channel(8, timeout)  # ignore AST info

                event_id = bytes_to_bint(self._recv_channel(4, timeout))
                break
            else:
                raise InternalError("_wait_for_event:op_code = %d" % (op_code,))

        assert event_id == self.event_id   # treat only one event_id
        r = {}
        for k, v in event_count.items():
            r[k] = v - self.event_count[k]
            self.event_count[k] = v
        return r

    def __init__(self, conn, names, event_id, timeout):
        self.connection = conn
        self.event_count = {}
        for name in names:
            self.event_count[name] = 0

        self.connection._op_connect_request()
        (_, _, buf) = self.connection._op_response()

        family = buf[:2]
        port = bytes_to_bint(buf[2:4], u=True)
        if family == b'\x02\x00':     # IPv4
            ip_address = '.'.join([str(c) for c in buf[4:8]])
        elif family in (b'\x0a\x00', b'\x17\00'):  # IPv6
            if bytes_to_hex(buf[4:20]) == b"0000000000000000000000000000ffff":
                # ipv4 mapped ipv6 address
                ip_address = '.'.join([str(c) for c in buf[20:24]])
            else:
                address = bytes_to_hex(buf[4:20])
                if not isinstance(address, str):    # Py3
                    address = address.decode('ascii')
                ip_address = ':'.join(
                    [address[i: i+4] for i in range(0, len(address), 4)]
                )
        self.sock = SocketStream(ip_address, port, timeout)
        if event_id:
            self.event_id = event_id
        else:
            self.connection.last_event_id += 1
            self.event_id = self.connection.last_event_id

        self.connection._op_que_events(self.event_count, self.event_id)
        self.connection._op_response()

        self._wait_for_event(timeout)

    def wait(self, timeout=None):
        self.connection._op_que_events(self.event_count, self.event_id)
        (h, oid, buf) = self.connection._op_response()

        return self._wait_for_event(timeout)

    def close(self):
        self.connection._op_cancel_events(self.event_id)
        (h, oid, buf) = self.connection._op_response()
        self.sock.close()
        self.sock = None
