##############################################################################
# Copyright (c) 2009-2018, Hajime Nakagami<nakagami@gmail.com>
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
from __future__ import print_function
import sys
import os
import socket
import xdrlib
import datetime
import decimal
import select
import warnings
import hashlib

try:
    import crypt
except ImportError:     # Not posix
    crypt = None
from firebirdsql.fberrmsgs import messages
from firebirdsql import DisconnectByPeer, InternalError, OperationalError, IntegrityError
from firebirdsql.consts import *
from firebirdsql.utils import *
from firebirdsql import srp
try:
    from Crypto.Cipher import ARC4
except ImportError:
    from firebirdsql.arc4 import ARC4

DEBUG = False


def DEBUG_OUTPUT(*argv):
    if not DEBUG:
        return
    for s in argv:
        print(s, end=' ', file=sys.stderr)
    print(file=sys.stderr)

INFO_SQL_SELECT_DESCRIBE_VARS = bs([
    isc_info_sql_select,
    isc_info_sql_describe_vars,
    isc_info_sql_sqlda_seq,
    isc_info_sql_type,
    isc_info_sql_sub_type,
    isc_info_sql_scale,
    isc_info_sql_length,
    isc_info_sql_null_ind,
    isc_info_sql_field,
    isc_info_sql_relation,
    isc_info_sql_owner,
    isc_info_sql_alias,
    isc_info_sql_describe_end])


def get_crypt(plain):
    if crypt is None:
        return ''
    return crypt.crypt(plain, '9z')[2:]


def convert_date(v):  # Convert datetime.date to BLR format data
    i = v.month + 9
    jy = v.year + (i // 12) - 1
    jm = i % 12
    c = jy // 100
    jy -= 100 * c
    j = (146097*c) // 4 + (1461*jy) // 4 + (153*jm+2) // 5 + v.day - 678882
    return bint_to_bytes(j, 4)


def convert_time(v):  # Convert datetime.time to BLR format time
    t = (v.hour*3600 + v.minute*60 + v.second) * 10000 + v.microsecond // 100
    return bint_to_bytes(t, 4)


def convert_timestamp(v):   # Convert datetime.datetime to BLR format timestamp
    return convert_date(v.date()) + convert_time(v.time())


def wire_operation(fn):
    if not DEBUG:
        return fn

    def f(*args, **kwargs):
        DEBUG_OUTPUT('<--', fn, '-->')
        r = fn(*args, **kwargs)
        return r
    return f


class WireProtocol(object):
    buffer_length = 1024

    op_connect = 1
    op_exit = 2
    op_accept = 3
    op_reject = 4
    op_protocol = 5
    op_disconnect = 6
    op_response = 9
    op_attach = 19
    op_create = 20
    op_detach = 21
    op_transaction = 29
    op_commit = 30
    op_rollback = 31
    op_open_blob = 35
    op_get_segment = 36
    op_put_segment = 37
    op_close_blob = 39
    op_info_database = 40
    op_info_transaction = 42
    op_batch_segments = 44
    op_que_events = 48
    op_cancel_events = 49
    op_commit_retaining = 50
    op_event = 52
    op_connect_request = 53
    op_aux_connect = 53
    op_create_blob2 = 57
    op_allocate_statement = 62
    op_execute = 63
    op_exec_immediate = 64
    op_fetch = 65
    op_fetch_response = 66
    op_free_statement = 67
    op_prepare_statement = 68
    op_info_sql = 70
    op_dummy = 71
    op_execute2 = 76
    op_sql_response = 78
    op_drop_database = 81
    op_service_attach = 82
    op_service_detach = 83
    op_service_info = 84
    op_service_start = 85
    op_rollback_retaining = 86
    # FB3
    op_update_account_info = 87
    op_authenticate_user = 88
    op_partial = 89
    op_trusted_auth = 90
    op_cancel = 91
    op_cont_auth = 92
    op_ping = 93
    op_accept_data = 94
    op_abort_aux_connection = 95
    op_crypt = 96
    op_crypt_key_callback = 97
    op_cond_accept = 98

    def __init__(self):
        self.accept_plugin_name = ''
        self.auth_data = b''

    def recv_channel(self, nbytes, word_alignment=False):
        n = nbytes
        if word_alignment and (n % 4):
            n += 4 - nbytes % 4  # 4 bytes word alignment
        r = bs([])
        while n:
            if (self.timeout is not None and select.select([self.sock._sock], [], [], self.timeout)[0] == []):
                break
            b = self.sock.recv(n)
            if not b:
                break
            r += b
            n -= len(b)
        if len(r) < nbytes:
            raise OperationalError('Can not recv() packets')
        return r[:nbytes]

    def str_to_bytes(self, s):
        "convert str to bytes"
        if (PYTHON_MAJOR_VER == 3 or
                (PYTHON_MAJOR_VER == 2 and type(s) == unicode)):
            return s.encode(charset_map.get(self.charset, self.charset))
        return s

    def bytes_to_str(self, b):
        "convert bytes array to raw string"
        if PYTHON_MAJOR_VER == 3:
            return b.decode(charset_map.get(self.charset, self.charset))
        return b

    def bytes_to_ustr(self, b):
        "convert bytes array to unicode string"
        return b.decode(charset_map.get(self.charset, self.charset))

    def _parse_status_vector(self):
        sql_code = 0
        gds_codes = set()
        message = ''
        n = bytes_to_bint(self.recv_channel(4))
        while n != isc_arg_end:
            if n == isc_arg_gds:
                gds_code = bytes_to_bint(self.recv_channel(4))
                if gds_code:
                    gds_codes.add(gds_code)
                    message += messages.get(gds_code, '@1')
                    num_arg = 0
            elif n == isc_arg_number:
                num = bytes_to_bint(self.recv_channel(4))
                if gds_code == 335544436:
                    sql_code = num
                num_arg += 1
                message = message.replace('@' + str(num_arg), str(num))
            elif n == isc_arg_string:
                nbytes = bytes_to_bint(self.recv_channel(4))
                s = str(self.recv_channel(nbytes, word_alignment=True))
                num_arg += 1
                message = message.replace('@' + str(num_arg), s)
            elif n == isc_arg_interpreted:
                nbytes = bytes_to_bint(self.recv_channel(4))
                s = str(self.recv_channel(nbytes, word_alignment=True))
                message += s
            elif n == isc_arg_sql_state:
                nbytes = bytes_to_bint(self.recv_channel(4))
                s = str(self.recv_channel(nbytes, word_alignment=True))
            n = bytes_to_bint(self.recv_channel(4))

        return (gds_codes, sql_code, message)

    def _parse_op_response(self):
        b = self.recv_channel(16)
        h = bytes_to_bint(b[0:4])         # Object handle
        oid = b[4:12]                       # Object ID
        buf_len = bytes_to_bint(b[12:])   # buffer length
        buf = self.recv_channel(buf_len, word_alignment=True)

        (gds_codes, sql_code, message) = self._parse_status_vector()
        if gds_codes.intersection([
            335544838, 335544879, 335544880, 335544466, 335544665, 335544347, 335544558
        ]):
            raise IntegrityError(message, gds_codes, sql_code)
        elif gds_codes.intersection([335544321]):
            warnings.warn(message)
        elif (sql_code or message) and not gds_codes.intersection([335544434]):
            raise OperationalError(message, gds_codes, sql_code)
        return (h, oid, buf)

    def _parse_op_event(self):
        b = self.recv_channel(4096)     # too large TODO: read step by step
        # TODO: parse event name
        db_handle = bytes_to_bint(b[0:4])
        event_id = bytes_to_bint(b[-4:])

        return (db_handle, event_id, {})

    def _create_blob(self, trans_handle, b):
        self._op_create_blob2(trans_handle)
        (blob_handle, blob_id, buf) = self._op_response()

        i = 0
        while i < len(b):
            self._op_put_segment(blob_handle, b[i:i+BLOB_SEGMENT_SIZE])
            (h, oid, buf) = self._op_response()
            i += BLOB_SEGMENT_SIZE

        self._op_close_blob(blob_handle)
        (h, oid, buf) = self._op_response()
        return blob_id

    def params_to_blr(self, trans_handle, params):
        "Convert parameter array to BLR and values format."
        ln = len(params) * 2
        blr = bs([5, 2, 4, 0, ln & 255, ln >> 8])
        if self.accept_version < PROTOCOL_VERSION13:
            values = bs([])
        else:
            # start with null indicator bitmap
            null_indicator = 0
            for i, p in enumerate(params):
                if p is None:
                    null_indicator |= (1 << i)
            n = len(params) // 8
            if len(params) % 8 != 0:
                n += 1
            if n % 4:   # padding
                n += 4 - n % 4
            null_indicator_bytes = []
            for i in range(n):
                null_indicator_bytes.append(null_indicator & 255)
                null_indicator >>= 8
            values = bs(null_indicator_bytes)
        for p in params:
            if (
                (PYTHON_MAJOR_VER == 2 and type(p) == unicode) or
                (PYTHON_MAJOR_VER == 3 and type(p) == str)
            ):
                p = self.str_to_bytes(p)
            t = type(p)
            if p is None:
                v = bs([])
                blr += bs([14, 0, 0])
            elif (
                (PYTHON_MAJOR_VER == 2 and t == str) or
                (PYTHON_MAJOR_VER == 3 and t == bytes)
            ):
                if len(p) > MAX_CHAR_LENGTH:
                    v = self._create_blob(trans_handle, p)
                    blr += bs([9, 0])
                else:
                    v = p
                    nbytes = len(v)
                    pad_length = ((4-nbytes) & 3)
                    v += bs([0]) * pad_length
                    blr += bs([14, nbytes & 255, nbytes >> 8])
            elif t == int:
                v = bint_to_bytes(p, 4)
                blr += bs([8, 0])    # blr_long
            elif t == float and p == float("inf"):
                v = b'\x7f\x80\x00\x00'
                blr += bs([10])
            elif t == decimal.Decimal or t == float:
                if t == float:
                    p = decimal.Decimal(str(p))
                (sign, digits, exponent) = p.as_tuple()
                v = 0
                ln = len(digits)
                for i in range(ln):
                    v += digits[i] * (10 ** (ln - i - 1))
                if sign:
                    v *= -1
                v = bint_to_bytes(v, 8)
                if exponent < 0:
                    exponent += 256
                blr += bs([16, exponent])
            elif t == datetime.date:
                v = convert_date(p)
                blr += bs([12])
            elif t == datetime.time:
                v = convert_time(p)
                blr += bs([13])
            elif t == datetime.datetime:
                v = convert_timestamp(p)
                blr += bs([35])
            elif t == bool:
                v = bs([1, 0, 0, 0]) if p else bs([0, 0, 0, 0])
                blr += bs([23])
            else:   # fallback, convert to string
                p = p.__repr__()
                if PYTHON_MAJOR_VER == 3 or (PYTHON_MAJOR_VER == 2 and type(p) == unicode):
                    p = self.str_to_bytes(p)
                v = p
                nbytes = len(v)
                pad_length = ((4-nbytes) & 3)
                v += bs([0]) * pad_length
                blr += bs([14, nbytes & 255, nbytes >> 8])
            blr += bs([7, 0])
            values += v
            if self.accept_version < PROTOCOL_VERSION13:
                values += bs([0]) * 4 if not p is None else bs([0xff, 0xff, 0xff, 0xff])
        blr += bs([255, 76])    # [blr_end, blr_eoc]
        return blr, values

    def uid(self, auth_plugin_name, wire_crypt):
        def pack_cnct_param(k, v):
            if k != CNCT_specific_data:
                return bs([k] + [len(v)]) + v
            # specific_data split per 254 bytes
            b = b''
            i = 0
            while len(v) > 254:
                b += bs([k, 255, i]) + v[:254]
                v = v[254:]
                i += 1
            b += bs([k, len(v)+1, i]) + v
            return b

        auth_plugin_list = ('Srp256', 'Srp', 'Legacy_Auth')
        # get and calculate CNCT_xxxx values
        if sys.platform == 'win32':
            user = os.environ['USERNAME']
            hostname = os.environ['COMPUTERNAME']
        else:
            user = os.environ.get('USER', '')
            hostname = socket.gethostname()

        if auth_plugin_name in ('Srp256', 'Srp'):
            self.client_public_key, self.client_private_key = srp.client_seed()
            specific_data = bytes_to_hex(srp.long2bytes(self.client_public_key))
        elif auth_plugin_name == 'Legacy_Auth':
            assert crypt, "Legacy_Auth needs crypt module"
            specific_data = self.str_to_bytes(get_crypt(self.password))
        else:
            raise OperationalError("Unknown auth plugin name '%s'" % (auth_plugin_name,))
        self.plugin_name = auth_plugin_name
        self.plugin_list = b','.join([s.encode('utf-8') for s in auth_plugin_list])
        client_crypt = b'\x01\x00\x00\x00' if wire_crypt else b'\x00\x00\x00\x00'

        # set CNCT_xxxx values
        r = b''
        r += pack_cnct_param(CNCT_login, self.str_to_bytes(self.user))
        r += pack_cnct_param(CNCT_plugin_name, self.str_to_bytes(self.plugin_name))
        r += pack_cnct_param(CNCT_plugin_list, self.plugin_list)
        r += pack_cnct_param(CNCT_specific_data, specific_data)
        r += pack_cnct_param(CNCT_client_crypt, client_crypt)

        r += pack_cnct_param(CNCT_user, self.str_to_bytes(user))
        r += pack_cnct_param(CNCT_host, self.str_to_bytes(hostname))
        r += pack_cnct_param(CNCT_user_verification, b'')
        return r

    @wire_operation
    def _op_connect(self, auth_plugin_name, wire_crypt):
        protocols = [
            # PROTOCOL_VERSION, Arch type (Generic=1), min, max, weight
            '0000000a00000001000000000000000500000002',     # 10, 1, 0, 5, 2
            'ffff800b00000001000000000000000500000004',     # 11, 1, 0, 5, 4
            'ffff800c00000001000000000000000500000006',     # 12, 1, 0, 5, 6
            'ffff800d00000001000000000000000500000008',     # 13, 1, 0, 5, 8
        ]
        p = xdrlib.Packer()
        p.pack_int(self.op_connect)
        p.pack_int(self.op_attach)
        p.pack_int(3)   # CONNECT_VERSION
        p.pack_int(1)   # arch_generic
        p.pack_string(self.str_to_bytes(self.filename if self.filename else ''))

        p.pack_int(len(protocols))
        p.pack_bytes(self.uid(auth_plugin_name, wire_crypt))
        self.sock.send(p.get_buffer() + hex_to_bytes(''.join(protocols)))

    @wire_operation
    def _op_create(self, page_size=4096):
        dpb = bs([1])
        s = self.str_to_bytes(self.charset)
        dpb += bs([isc_dpb_set_db_charset, len(s)]) + s
        dpb += bs([isc_dpb_lc_ctype, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bs([isc_dpb_user_name, len(s)]) + s
        if self.accept_version < PROTOCOL_VERSION13:
            enc_pass = get_crypt(self.password)
            if self.accept_version == PROTOCOL_VERSION10 or not enc_pass:
                s = self.str_to_bytes(self.password)
                dpb += bs([isc_dpb_password, len(s)]) + s
            else:
                enc_pass = self.str_to_bytes(enc_pass)
                dpb += bs([isc_dpb_password_enc, len(enc_pass)]) + enc_pass
        if self.role:
            s = self.str_to_bytes(self.role)
            dpb += bs([isc_dpb_sql_role_name, len(s)]) + s
        if self.auth_data:
            s = bytes_to_hex(self.auth_data)
            dpb += bs([isc_dpb_specific_auth_data, len(s)]) + s
        dpb += bs([isc_dpb_sql_dialect, 4]) + int_to_bytes(3, 4)
        dpb += bs([isc_dpb_force_write, 4]) + int_to_bytes(1, 4)
        dpb += bs([isc_dpb_overwrite, 4]) + int_to_bytes(1, 4)
        dpb += bs([isc_dpb_page_size, 4]) + int_to_bytes(page_size, 4)
        p = xdrlib.Packer()
        p.pack_int(self.op_create)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_accept(self):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_reject:
            raise OperationalError('Connection is rejected')

        op_code = bytes_to_bint(b)
        if op_code == self.op_response:
            return self._parse_op_response()    # error occured

        b = self.recv_channel(12)
        self.accept_version = byte_to_int(b[3])
        self.accept_architecture = bytes_to_bint(b[4:8])
        self.accept_type = bytes_to_bint(b[8:])
        self.lazy_response_count = 0

        if op_code == self.op_cond_accept or op_code == self.op_accept_data:
            read_length = 0

            ln = bytes_to_bint(self.recv_channel(4))
            data = self.recv_channel(ln, word_alignment=True)

            ln = bytes_to_bint(self.recv_channel(4))
            self.accept_plugin_name = self.recv_channel(ln, word_alignment=True)

            is_authenticated = bytes_to_bint(self.recv_channel(4))
            read_length += 4
            ln = bytes_to_bint(self.recv_channel(4))
            self.recv_channel(ln, word_alignment=True)   # keys

            if is_authenticated == 0:
                if self.accept_plugin_name in (b'Srp256',  b'Srp'):
                    if self.accept_plugin_name == b'Srp256':
                        hash_algo = hashlib.sha256
                    elif self.accept_plugin_name == b'Srp':
                        hash_algo = hashlib.sha1
                    else:
                        raise OperationalError(
                            'Unknown auth plugin %s' % (self.accept_plugin_name)
                        )
                    user = self.user
                    if len(user) > 2 and user[0] == user[-1] == '"':
                        user = user[1:-1]
                        user = user.replace('""','"')
                    else:
                        user = user.upper()

                    if len(data) == 0:
                        raise OperationalError('Unauthorized')
                    ln = bytes_to_int(data[:2])
                    server_salt = data[2:ln+2]
                    server_public_key = srp.bytes2long(
                        hex_to_bytes(data[4+ln:]))

                    auth_data, session_key = srp.client_proof(
                        self.str_to_bytes(user),
                        self.str_to_bytes(self.password),
                        server_salt,
                        self.client_public_key,
                        server_public_key,
                        self.client_private_key,
                        hash_algo)
                elif self.accept_plugin_name == b'Legacy_Auth':
                    auth_data = self.str_to_bytes(get_crypt(self.password))
                else:
                    raise OperationalError('Unauthorized')
                if self.wire_crypt:
                    # send op_cont_auth
                    p = xdrlib.Packer()
                    p.pack_int(self.op_cont_auth)
                    p.pack_string(bytes_to_hex(auth_data))
                    p.pack_bytes(self.accept_plugin_name)
                    p.pack_bytes(self.plugin_list)
                    p.pack_bytes(b'')
                    self.sock.send(p.get_buffer())
                    (h, oid, buf) = self._op_response()

                    # op_crypt: plugin[Arc4] key[Symmetric]
                    p = xdrlib.Packer()
                    p.pack_int(self.op_crypt)
                    p.pack_string(b'Arc4')
                    p.pack_string(b'Symmetric')
                    self.sock.send(p.get_buffer())
                    self.sock.set_translator(
                        ARC4.new(session_key), ARC4.new(session_key))
                    (h, oid, buf) = self._op_response()
                else:   # use later _op_attach() and _op_create()
                    self.auth_data = auth_data
        else:
            assert op_code == self.op_accept

    @wire_operation
    def _op_attach(self):
        dpb = bs([isc_dpb_version1])
        s = self.str_to_bytes(self.charset)
        dpb += bs([isc_dpb_lc_ctype, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bs([isc_dpb_user_name, len(s)]) + s
        if self.accept_version < PROTOCOL_VERSION13:
            enc_pass = get_crypt(self.password)
            if self.accept_version == PROTOCOL_VERSION10 or not enc_pass:
                s = self.str_to_bytes(self.password)
                dpb += bs([isc_dpb_password, len(s)]) + s
            else:
                enc_pass = self.str_to_bytes(enc_pass)
                dpb += bs([isc_dpb_password_enc, len(enc_pass)]) + enc_pass
        if self.role:
            s = self.str_to_bytes(self.role)
            dpb += bs([isc_dpb_sql_role_name, len(s)]) + s
        dpb += bs([isc_dpb_process_id, 4]) + int_to_bytes(os.getpid(), 4)
        s = self.str_to_bytes(sys.argv[0])
        dpb += bs([isc_dpb_process_name, len(s)]) + s
        if self.auth_data:
            s = bytes_to_hex(self.auth_data)
            dpb += bs([isc_dpb_specific_auth_data, len(s)]) + s
        p = xdrlib.Packer()
        p.pack_int(self.op_attach)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_drop_database(self):
        if self.db_handle is None:
            raise OperationalError('_op_drop_database() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_drop_database)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_attach(self):
        spb = bs([2, 2])
        s = self.str_to_bytes(self.user)
        spb += bs([isc_spb_user_name, len(s)]) + s
        if self.accept_version < PROTOCOL_VERSION13:
            enc_pass = get_crypt(self.password)
            if self.accept_version == PROTOCOL_VERSION10 or not enc_pass:
                s = self.str_to_bytes(self.password)
                spb += bs([isc_dpb_password, len(s)]) + s
            else:
                enc_pass = self.str_to_bytes(enc_pass)
                spb += bs([isc_dpb_password_enc, len(enc_pass)]) + enc_pass
        if self.auth_data:
            s = self.str_to_bytes(bytes_to_hex(self.auth_data))
            spb += bs([isc_dpb_specific_auth_data, len(s)]) + s
        spb += bs([isc_spb_dummy_packet_interval, 0x04, 0x78, 0x0a, 0x00, 0x00])
        p = xdrlib.Packer()
        p.pack_int(self.op_service_attach)
        p.pack_int(0)
        p.pack_string(self.str_to_bytes('service_mgr'))
        p.pack_bytes(spb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_info(self, param, item, buffer_length=512):
        if self.db_handle is None:
            raise OperationalError('_op_service_info() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_service_info)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        p.pack_bytes(item)
        p.pack_int(buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_start(self, param):
        if self.db_handle is None:
            raise OperationalError('_op_service_start() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_service_start)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_detach(self):
        if self.db_handle is None:
            raise OperationalError('_op_service_detach() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_service_detach)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_info_database(self, b):
        if self.db_handle is None:
            raise OperationalError('_op_info_database() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_info_database)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(b)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_transaction(self, tpb):
        if self.db_handle is None:
            raise OperationalError('_op_transaction() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_transaction)
        p.pack_int(self.db_handle)
        p.pack_bytes(tpb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_commit(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_commit_retaining(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit_retaining)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_rollback(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_rollback_retaining(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback_retaining)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_allocate_statement(self):
        if self.db_handle is None:
            raise OperationalError('_op_allocate_statement() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_allocate_statement)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_info_transaction(self, trans_handle, b):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_transaction)
        p.pack_int(trans_handle)
        p.pack_int(0)
        p.pack_bytes(b)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_free_statement(self, stmt_handle, mode):
        p = xdrlib.Packer()
        p.pack_int(self.op_free_statement)
        p.pack_int(stmt_handle)
        p.pack_int(mode)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_prepare_statement(self, stmt_handle, trans_handle, query, option_items=None):
        if option_items is None:
            option_items=bs([])
        desc_items = option_items + bs([isc_info_sql_stmt_type])+INFO_SQL_SELECT_DESCRIBE_VARS
        p = xdrlib.Packer()
        p.pack_int(self.op_prepare_statement)
        p.pack_int(trans_handle)
        p.pack_int(stmt_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_string(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_info_sql(self, stmt_handle, vars):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_sql)
        p.pack_int(stmt_handle)
        p.pack_int(0)
        p.pack_bytes(vars)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_execute(self, stmt_handle, trans_handle, params):
        p = xdrlib.Packer()
        p.pack_int(self.op_execute)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            p.pack_bytes(bs([]))
            p.pack_int(0)
            p.pack_int(0)
            self.sock.send(p.get_buffer())
        else:
            (blr, values) = self.params_to_blr(trans_handle, params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)
            self.sock.send(p.get_buffer() + values)

    @wire_operation
    def _op_execute2(self, stmt_handle, trans_handle, params, output_blr):
        p = xdrlib.Packer()
        p.pack_int(self.op_execute2)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            values = b''
            p.pack_bytes(bs([]))
            p.pack_int(0)
            p.pack_int(0)
        else:
            (blr, values) = self.params_to_blr(trans_handle, params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)

        q = xdrlib.Packer()
        q.pack_bytes(output_blr)
        q.pack_int(0)
        self.sock.send(p.get_buffer() + values + q.get_buffer())

    @wire_operation
    def _op_exec_immediate(self, trans_handle, query):
        if self.db_handle is None:
            raise OperationalError('_op_exec_immediate() Invalid db handle')
        desc_items = bs([])
        p = xdrlib.Packer()
        p.pack_int(self.op_exec_immediate)
        p.pack_int(trans_handle)
        p.pack_int(self.db_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_string(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_fetch(self, stmt_handle, blr):
        p = xdrlib.Packer()
        p.pack_int(self.op_fetch)
        p.pack_int(stmt_handle)
        p.pack_bytes(blr)
        p.pack_int(0)
        p.pack_int(400)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_fetch_response(self, stmt_handle, xsqlda):
        op_code = bytes_to_bint(self.recv_channel(4))
        while op_code == self.op_dummy:
            op_code = bytes_to_bint(self.recv_channel(4))

        while op_code == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            h, oid, buf = self._parse_op_response()
            op_code = bytes_to_bint(self.recv_channel(4))

        if op_code != self.op_fetch_response:
            if op_code == self.op_response:
                self._parse_op_response()
            raise InternalError
        b = self.recv_channel(8)
        status = bytes_to_bint(b[:4])
        count = bytes_to_bint(b[4:8])
        rows = []
        while count:
            r = [None] * len(xsqlda)
            if self.accept_version < PROTOCOL_VERSION13:
                for i in range(len(xsqlda)):
                    x = xsqlda[i]
                    if x.io_length() < 0:
                        b = self.recv_channel(4)
                        ln = bytes_to_bint(b)
                    else:
                        ln = x.io_length()
                    raw_value = self.recv_channel(ln, word_alignment=True)
                    if self.recv_channel(4) == bs([0]) * 4:     # Not NULL
                        r[i] = x.value(raw_value)
            else:   # PROTOCOL_VERSION13
                n = len(xsqlda) // 8
                if len(xsqlda) % 8 != 0:
                    n += 1
                null_indicator = 0
                for c in reversed(self.recv_channel(n, word_alignment=True)):
                    null_indicator <<= 8
                    null_indicator += c if PYTHON_MAJOR_VER == 3 else ord(c)
                for i in range(len(xsqlda)):
                    x = xsqlda[i]
                    if null_indicator & (1 << i):
                        continue
                    if x.io_length() < 0:
                        b = self.recv_channel(4)
                        ln = bytes_to_bint(b)
                    else:
                        ln = x.io_length()
                    raw_value = self.recv_channel(ln, word_alignment=True)
                    r[i] = x.value(raw_value)
            rows.append(r)
            b = self.recv_channel(12)
            op_code = bytes_to_bint(b[:4])
            status = bytes_to_bint(b[4:8])
            count = bytes_to_bint(b[8:])
        return rows, status != 100

    @wire_operation
    def _op_detach(self):
        if self.db_handle is None:
            raise OperationalError('_op_detach() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_detach)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_open_blob(self, blob_id, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_open_blob)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer() + blob_id)

    @wire_operation
    def _op_create_blob2(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_create_blob2)
        p.pack_int(0)
        p.pack_int(trans_handle)
        p.pack_int(0)
        p.pack_int(0)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_get_segment(self, blob_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_get_segment)
        p.pack_int(blob_handle)
        p.pack_int(self.buffer_length)
        p.pack_int(0)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_put_segment(self, blob_handle, seg_data):
        ln = len(seg_data)
        p = xdrlib.Packer()
        p.pack_int(self.op_put_segment)
        p.pack_int(blob_handle)
        p.pack_int(ln)
        p.pack_int(ln)
        pad_length = (4-ln) & 3
        self.sock.send(p.get_buffer() + seg_data + bs([0])*pad_length)

    @wire_operation
    def _op_batch_segments(self, blob_handle, seg_data):
        ln = len(seg_data)
        p = xdrlib.Packer()
        p.pack_int(self.op_batch_segments)
        p.pack_int(blob_handle)
        p.pack_int(ln + 2)
        p.pack_int(ln + 2)
        pad_length = ((4-(ln+2)) & 3)
        self.sock.send(p.get_buffer() + int_to_bytes(ln, 2) + seg_data + bs([0])*pad_length)

    @wire_operation
    def _op_close_blob(self, blob_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_close_blob)
        p.pack_int(blob_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_que_events(self, event_names, event_id):
        if self.db_handle is None:
            raise OperationalError('_op_que_events() Invalid db handle')
        params = bs([1])
        for name, n in event_names.items():
            params += bs([len(name)])
            params += self.str_to_bytes(name)
            params += int_to_bytes(n, 4)
        p = xdrlib.Packer()
        p.pack_int(self.op_que_events)
        p.pack_int(self.db_handle)
        p.pack_bytes(params)
        p.pack_int(0)    # ast
        p.pack_int(0)    # args
        p.pack_int(event_id)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_cancel_events(self, event_id):
        if self.db_handle is None:
            raise OperationalError('_op_cancel_events() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_cancel_events)
        p.pack_int(self.db_handle)
        p.pack_int(event_id)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_connect_request(self):
        if self.db_handle is None:
            raise OperationalError('_op_connect_request() Invalid db handle')
        p = xdrlib.Packer()
        p.pack_int(self.op_connect_request)
        p.pack_int(1)    # async
        p.pack_int(self.db_handle)
        p.pack_int(0)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_response(self):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            h, oid, buf = self._parse_op_response()
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_cont_auth:
            raise OperationalError('Unauthorized')
        elif bytes_to_bint(b) != self.op_response:
            raise InternalError
        return self._parse_op_response()

    @wire_operation
    def _op_event(self):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_response and self.lazy_response_count:
            self.lazy_response_count -= 1
            self._parse_op_response()
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_exit or bytes_to_bint(b) == self.op_exit:
            raise DisconnectByPeer
        if bytes_to_bint(b) != self.op_event:
            if bytes_to_bint(b) == self.op_response:
                self._parse_op_response()
            raise InternalError
        return self._parse_op_event()

    @wire_operation
    def _op_sql_response(self, xsqlda):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) != self.op_sql_response:
            if bytes_to_bint(b) == self.op_response:
                self._parse_op_response()
            raise InternalError

        b = self.recv_channel(4)
        count = bytes_to_bint(b[:4])
        r = []
        if count == 0:
            return []
        if self.accept_version < PROTOCOL_VERSION13:
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if x.io_length() < 0:
                    b = self.recv_channel(4)
                    ln = bytes_to_bint(b)
                else:
                    ln = x.io_length()
                raw_value = self.recv_channel(ln, word_alignment=True)
                if self.recv_channel(4) == bs([0]) * 4:     # Not NULL
                    r.append(x.value(raw_value))
                else:
                    r.append(None)
        else:
            n = len(xsqlda) // 8
            if len(xsqlda) % 8 != 0:
                n += 1
            null_indicator = 0
            for c in reversed(self.recv_channel(n, word_alignment=True)):
                null_indicator <<= 8
                null_indicator += c if PYTHON_MAJOR_VER == 3 else ord(c)
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if null_indicator & (1 << i):
                    r.append(None)
                else:
                    if x.io_length() < 0:
                        b = self.recv_channel(4)
                        ln = bytes_to_bint(b)
                    else:
                        ln = x.io_length()
                    raw_value = self.recv_channel(ln, word_alignment=True)
                    r.append(x.value(raw_value))
        return r

    def _wait_for_event(self, timeout):
        event_names = {}
        event_id = 0
        while True:
            b4 = self.recv_channel(4)
            if b4 is None:
                return None
            op = bytes_to_bint(b4)
            if op == self.op_dummy:
                pass
            elif op == self.op_exit or op == self.op_disconnect:
                break
            elif op == self.op_event:
                bytes_to_int(self.recv_channel(4))  # db_handle
                ln = bytes_to_bint(self.recv_channel(4))
                b = self.recv_channel(ln, word_alignment=True)
                assert byte_to_int(b[0]) == 1
                i = 1
                while i < len(b):
                    ln = byte_to_int(b[i])
                    s = self.connection.bytes_to_str(b[i+1:i+1+ln])
                    n = bytes_to_int(b[i+1+ln:i+1+ln+4])
                    event_names[s] = n
                    i += ln + 5
                self.recv_channel(8)  # ignore AST info

                event_id = bytes_to_bint(self.recv_channel(4))
                break
            else:
                raise InternalError

        return (event_id, event_names)
