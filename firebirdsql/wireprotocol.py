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
from __future__ import print_function
import sys
import os
import socket
import datetime
import decimal

from firebirdsql.err import OperationalError
from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql import srp
from firebirdsql import tz_utils


def DEBUG_OUTPUT(*argv):
    if debug_level() < 2:
        return
    for s in argv:
        print(s, end=' ', file=sys.stderr)
    print(file=sys.stderr)


INFO_SQL_SELECT_DESCRIBE_VARS = bytes([
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
    from passlib.hash import des_crypt
    return des_crypt.using(salt='9z').hash(plain)[2:]


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


def convert_time_tz(v):  # Convert datetime.time to BLR format time_tz
    v2 = datetime.datetime.combine(datetime.date.today(), v).astimezone(datetime.timezone.utc)

    t = (v2.hour*3600 + v2.minute*60 + v2.second) * 10000 + v2.microsecond // 100
    r = bint_to_bytes(t, 4)
    r += bint_to_bytes(tz_utils.get_timezone_id_by_name(str(v.tzinfo)), 4)
    return r


def convert_timestamp_tz(v):   # Convert datetime.datetime to BLR format timestamp_tz
    v2 = v.astimezone(datetime.timezone.utc)

    r = convert_date(v2.date()) + convert_time(v2.time())
    r += bint_to_bytes(tz_utils.get_timezone_id_by_name(str(v.tzinfo)), 4)
    return r


def wire_operation(fn):
    def f(*args, **kwargs):
        if kwargs:
            DEBUG_OUTPUT(fn.__name__, id(args[0]), args[0].db_handle, args[1:], kwargs)
        else:
            DEBUG_OUTPUT(fn.__name__, id(args[0]), args[0].db_handle, args[1:])
        r = fn(*args, **kwargs)
        return r
    return f


class Packer(object):
    def __init__(self):
        self.buf = b''

    def pack_int(self, v):
        self.buf += bint_to_bytes(v, 4)

    def pack_bytes(self, v):
        n = len(v)
        self.buf += bint_to_bytes(n, 4)
        n = ((n+3)//4)*4
        self.buf += v + (n - len(v)) * b'\0'

    def get_buffer(self):
        return self.buf


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
    op_open_blob2 = 56
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

    def str_to_bytes(self, s):
        "convert str to bytes"
        if isinstance(s, str):
            return s.encode(charset_map.get(self.charset, self.charset))
        return s

    def bytes_to_str(self, b):
        return b.decode(charset_map.get(self.charset, self.charset))

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
        blr = bytes([5, 2, 4, 0, ln & 255, ln >> 8])
        if self.accept_version < PROTOCOL_VERSION13:
            values = bytes([])
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
            values = bytes(null_indicator_bytes)
        for p in params:
            if isinstance(p, str):
                p = self.str_to_bytes(p)
            t = type(p)
            if p is None:
                v = bytes([])
                blr += bytes([14, 0, 0])
            elif t == bytes:
                if len(p) > MAX_CHAR_LENGTH:
                    v = self._create_blob(trans_handle, p)
                    blr += bytes([9, 0])
                else:
                    v = p
                    nbytes = len(v)
                    pad_length = ((4-nbytes) & 3)
                    v += bytes([0]) * pad_length
                    blr += bytes([14, nbytes & 255, nbytes >> 8])
            elif t == int:
                if p <= 0x7FFFFFFF and p >= -0x80000000:
                    v = bint_to_bytes(p, 4)
                    blr += bytes([8, 0])    # blr_long
                else:
                    v = bint_to_bytes(p, 8)
                    blr += bytes([16, 0])    # blr_int64
            elif t == float and p == float("inf"):
                v = b'\x7f\x80\x00\x00'
                blr += bytes([10])
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
                blr += bytes([16, exponent])
            elif t == datetime.date:
                v = convert_date(p)
                blr += bytes([12])
            elif t == datetime.time:
                if p.tzinfo:
                    v = convert_time_tz(p)
                    blr += bytes([28])
                else:
                    v = convert_time(p)
                    blr += bytes([13])
            elif t == datetime.datetime:
                if p.tzinfo:
                    v = convert_timestamp_tz(p)
                    blr += bytes([29])
                else:
                    v = convert_timestamp(p)
                    blr += bytes([35])
            elif t == bool:
                v = bytes([1, 0, 0, 0]) if p else bytes([0, 0, 0, 0])
                blr += bytes([23])
            else:   # fallback, convert to string
                p = p.__repr__()
                if isinstance(p, str):
                    p = self.str_to_bytes(p)
                v = p
                nbytes = len(v)
                pad_length = ((4-nbytes) & 3)
                v += bytes([0]) * pad_length
                blr += bytes([14, nbytes & 255, nbytes >> 8])
            blr += bytes([7, 0])
            values += v
            if self.accept_version < PROTOCOL_VERSION13:
                values += bytes([0]) * 4 if p is not None else bytes([0xff, 0xff, 0xff, 0xff])
        blr += bytes([255, 76])    # [blr_end, blr_eoc]
        return blr, values

    def uid(self, auth_plugin_name, wire_crypt):
        def pack_cnct_param(k, v):
            if k != CNCT_specific_data:
                return bytes([k] + [len(v)]) + v
            # specific_data split per 254 bytes
            b = b''
            i = 0
            while len(v) > 254:
                b += bytes([k, 255, i]) + v[:254]
                v = v[254:]
                i += 1
            b += bytes([k, len(v)+1, i]) + v
            return b

        auth_plugin_list = ('Srp256', 'Srp', 'Legacy_Auth')
        # get and calculate CNCT_xxxx values
        if sys.platform == 'win32':
            user = os.environ['USERNAME']
            hostname = os.environ['COMPUTERNAME']
        else:
            user = os.environ.get('USER', '')
            hostname = socket.gethostname()

        self.client_public_key, self.client_private_key = srp.client_seed()
        if auth_plugin_name in ('Srp256', 'Srp'):
            specific_data = bytes_to_hex(srp.long2bytes(self.client_public_key))
        elif auth_plugin_name == 'Legacy_Auth':
            enc_password = get_crypt(self.password)
            assert enc_password, "Legacy_Auth needs passlib."
            specific_data = self.str_to_bytes(enc_password)
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
            'ffff800e0000000100000000000000050000000a',     # 14, 1, 0, 5, 10
            'ffff800f0000000100000000000000050000000c',     # 15, 1, 0, 5, 12
            'ffff80100000000100000000000000050000000e',     # 16, 1, 0, 5, 14
            'ffff801100000001000000000000000500000010',     # 17, 1, 0, 5, 16
        ]
        p = Packer()
        p.pack_int(self.op_connect)
        p.pack_int(self.op_attach)
        p.pack_int(3)   # CONNECT_VERSION
        p.pack_int(1)   # arch_generic
        p.pack_bytes(self.str_to_bytes(self.filename if self.filename else ''))

        p.pack_int(len(protocols))
        p.pack_bytes(self.uid(auth_plugin_name, wire_crypt))
        self.sock.send(p.get_buffer() + hex_to_bytes(''.join(protocols)))

    @wire_operation
    def _op_create(self, timezone, page_size=4096):
        dpb = bytes([1])
        s = self.str_to_bytes(self.charset)
        dpb += bytes([isc_dpb_set_db_charset, len(s)]) + s
        dpb += bytes([isc_dpb_lc_ctype, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bytes([isc_dpb_user_name, len(s)]) + s
        if self.accept_version < PROTOCOL_VERSION13:
            enc_pass = get_crypt(self.password)
            if self.accept_version == PROTOCOL_VERSION10 or not enc_pass:
                s = self.str_to_bytes(self.password)
                dpb += bytes([isc_dpb_password, len(s)]) + s
            else:
                enc_pass = self.str_to_bytes(enc_pass)
                dpb += bytes([isc_dpb_password_enc, len(enc_pass)]) + enc_pass
        if self.role:
            s = self.str_to_bytes(self.role)
            dpb += bytes([isc_dpb_sql_role_name, len(s)]) + s
        if self.auth_data:
            s = bytes_to_hex(self.auth_data)
            dpb += bytes([isc_dpb_specific_auth_data, len(s)]) + s
        if timezone:
            s = self.str_to_bytes(timezone)
            dpb += bytes([isc_dpb_session_time_zone, len(s)]) + s
        dpb += bytes([isc_dpb_sql_dialect, 4]) + int_to_bytes(3, 4)
        dpb += bytes([isc_dpb_force_write, 4]) + int_to_bytes(1, 4)
        dpb += bytes([isc_dpb_overwrite, 4]) + int_to_bytes(1, 4)
        dpb += bytes([isc_dpb_page_size, 4]) + int_to_bytes(page_size, 4)
        p = Packer()
        p.pack_int(self.op_create)
        p.pack_int(0)                       # Database Object ID
        p.pack_bytes(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_cont_auth(self, auth_data, auth_plugin_name, auth_plugin_list, keys):
        p = Packer()
        p.pack_int(self.op_cont_auth)
        p.pack_bytes(bytes_to_hex(auth_data))
        p.pack_bytes(auth_plugin_name)
        p.pack_bytes(auth_plugin_list)
        p.pack_bytes(keys)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_crypt(self, algo):
        p = Packer()
        p.pack_int(self.op_crypt)
        p.pack_bytes(algo)
        p.pack_bytes(b'Symmetric')
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_attach(self, timezone):
        dpb = bytes([isc_dpb_version1])
        s = self.str_to_bytes(self.charset)
        dpb += bytes([isc_dpb_lc_ctype, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bytes([isc_dpb_user_name, len(s)]) + s
        if self.accept_version < PROTOCOL_VERSION13:
            enc_pass = get_crypt(self.password)
            if self.accept_version == PROTOCOL_VERSION10 or not enc_pass:
                s = self.str_to_bytes(self.password)
                dpb += bytes([isc_dpb_password, len(s)]) + s
            else:
                enc_pass = self.str_to_bytes(enc_pass)
                dpb += bytes([isc_dpb_password_enc, len(enc_pass)]) + enc_pass
        if self.role:
            s = self.str_to_bytes(self.role)
            dpb += bytes([isc_dpb_sql_role_name, len(s)]) + s
        dpb += bytes([isc_dpb_process_id, 4]) + int_to_bytes(os.getpid(), 4)
        s = self.str_to_bytes(sys.argv[0])
        dpb += bytes([isc_dpb_process_name, len(s)]) + s
        if self.auth_data:
            s = bytes_to_hex(self.auth_data)
            dpb += bytes([isc_dpb_specific_auth_data, len(s)]) + s
        if timezone:
            s = self.str_to_bytes(self.timezone)
            dpb += bytes([isc_dpb_session_time_zone, len(s)]) + s
        p = Packer()
        p.pack_int(self.op_attach)
        p.pack_int(0)                       # Database Object ID
        p.pack_bytes(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_drop_database(self):
        if self.db_handle is None:
            raise OperationalError('_op_drop_database() Invalid db handle')
        p = Packer()
        p.pack_int(self.op_drop_database)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_attach(self):
        spb = bytes([2, 2])
        s = self.str_to_bytes(self.user)
        spb += bytes([isc_spb_user_name, len(s)]) + s
        if self.accept_version < PROTOCOL_VERSION13:
            enc_pass = get_crypt(self.password)
            if self.accept_version == PROTOCOL_VERSION10 or not enc_pass:
                s = self.str_to_bytes(self.password)
                spb += bytes([isc_dpb_password, len(s)]) + s
            else:
                enc_pass = self.str_to_bytes(enc_pass)
                spb += bytes([isc_dpb_password_enc, len(enc_pass)]) + enc_pass
        if self.auth_data:
            s = self.str_to_bytes(bytes_to_hex(self.auth_data))
            spb += bytes([isc_dpb_specific_auth_data, len(s)]) + s
        spb += bytes([isc_spb_dummy_packet_interval, 0x04, 0x78, 0x0a, 0x00, 0x00])
        p = Packer()
        p.pack_int(self.op_service_attach)
        p.pack_int(0)
        p.pack_bytes(b'service_mgr')
        p.pack_bytes(spb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_info(self, param, item, buffer_length=512):
        if self.db_handle is None:
            raise OperationalError('_op_service_info() Invalid db handle')
        p = Packer()
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
        p = Packer()
        p.pack_int(self.op_service_start)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_service_detach(self):
        if self.db_handle is None:
            raise OperationalError('_op_service_detach() Invalid db handle')
        p = Packer()
        p.pack_int(self.op_service_detach)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_info_database(self, b):
        if self.db_handle is None:
            raise OperationalError('_op_info_database() Invalid db handle')
        p = Packer()
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
        p = Packer()
        p.pack_int(self.op_transaction)
        p.pack_int(self.db_handle)
        p.pack_bytes(tpb)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_commit(self, trans_handle):
        p = Packer()
        p.pack_int(self.op_commit)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_commit_retaining(self, trans_handle):
        p = Packer()
        p.pack_int(self.op_commit_retaining)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_rollback(self, trans_handle):
        p = Packer()
        p.pack_int(self.op_rollback)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_rollback_retaining(self, trans_handle):
        p = Packer()
        p.pack_int(self.op_rollback_retaining)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_allocate_statement(self):
        if self.db_handle is None:
            raise OperationalError('_op_allocate_statement() Invalid db handle')
        p = Packer()
        p.pack_int(self.op_allocate_statement)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_info_transaction(self, trans_handle, b):
        p = Packer()
        p.pack_int(self.op_info_transaction)
        p.pack_int(trans_handle)
        p.pack_int(0)
        p.pack_bytes(b)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_free_statement(self, stmt_handle, mode):
        p = Packer()
        p.pack_int(self.op_free_statement)
        p.pack_int(stmt_handle)
        p.pack_int(mode)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_prepare_statement(self, stmt_handle, trans_handle, query, option_items=None):
        if option_items is None:
            option_items = bytes([])
        desc_items = option_items + bytes([isc_info_sql_stmt_type])+INFO_SQL_SELECT_DESCRIBE_VARS
        p = Packer()
        p.pack_int(self.op_prepare_statement)
        p.pack_int(trans_handle)
        p.pack_int(stmt_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_bytes(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_info_sql(self, stmt_handle, vars):
        p = Packer()
        p.pack_int(self.op_info_sql)
        p.pack_int(stmt_handle)
        p.pack_int(0)
        p.pack_bytes(vars)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_execute(self, stmt_handle, trans_handle, params):
        p = Packer()
        p.pack_int(self.op_execute)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            p.pack_bytes(bytes([]))
            p.pack_int(0)
            p.pack_int(0)
            buf = p.get_buffer()
        else:
            (blr, values) = self.params_to_blr(trans_handle, params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)
            buf = p.get_buffer() + values
        if self.accept_version >= PROTOCOL_VERSION16:
            buf += int_to_bytes(0, 4)
        self.sock.send(buf)

    @wire_operation
    def _op_execute2(self, stmt_handle, trans_handle, params, output_blr):
        p = Packer()
        p.pack_int(self.op_execute2)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            values = b''
            p.pack_bytes(bytes([]))
            p.pack_int(0)
            p.pack_int(0)
        else:
            (blr, values) = self.params_to_blr(trans_handle, params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)

        q = Packer()
        q.pack_bytes(output_blr)
        q.pack_int(0)
        buf = p.get_buffer() + values + q.get_buffer()
        if self.accept_version >= PROTOCOL_VERSION16:
            buf += int_to_bytes(0, 4)
        self.sock.send(buf)

    @wire_operation
    def _op_exec_immediate(self, trans_handle, query):
        if self.db_handle is None:
            raise OperationalError('_op_exec_immediate() Invalid db handle')
        desc_items = bytes([])
        p = Packer()
        p.pack_int(self.op_exec_immediate)
        p.pack_int(trans_handle)
        p.pack_int(self.db_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_bytes(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_fetch(self, stmt_handle, blr):
        p = Packer()
        p.pack_int(self.op_fetch)
        p.pack_int(stmt_handle)
        p.pack_bytes(blr)
        p.pack_int(0)
        p.pack_int(400)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_detach(self):
        if self.db_handle is None:
            raise OperationalError('_op_detach() Invalid db handle')
        p = Packer()
        p.pack_int(self.op_detach)
        p.pack_int(self.db_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_open_blob(self, blob_id, trans_handle):
        p = Packer()
        p.pack_int(self.op_open_blob)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer() + blob_id)

    @wire_operation
    def _op_open_blob2(self, blob_id, trans_handle):
        p = Packer()
        p.pack_int(self.op_open_blob2)
        p.pack_int(0)
        p.pack_int(trans_handle)
        self.sock.send(p.get_buffer() + blob_id)

    @wire_operation
    def _op_create_blob2(self, trans_handle):
        p = Packer()
        p.pack_int(self.op_create_blob2)
        p.pack_int(0)
        p.pack_int(trans_handle)
        p.pack_int(0)
        p.pack_int(0)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_get_segment(self, blob_handle):
        p = Packer()
        p.pack_int(self.op_get_segment)
        p.pack_int(blob_handle)
        p.pack_int(self.buffer_length)
        p.pack_int(0)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_put_segment(self, blob_handle, seg_data):
        ln = len(seg_data)
        p = Packer()
        p.pack_int(self.op_put_segment)
        p.pack_int(blob_handle)
        p.pack_int(ln)
        p.pack_int(ln)
        pad_length = (4-ln) & 3
        self.sock.send(p.get_buffer() + seg_data + bytes([0])*pad_length)

    @wire_operation
    def _op_batch_segments(self, blob_handle, seg_data):
        ln = len(seg_data)
        p = Packer()
        p.pack_int(self.op_batch_segments)
        p.pack_int(blob_handle)
        p.pack_int(ln + 2)
        p.pack_int(ln + 2)
        pad_length = ((4-(ln+2)) & 3)
        self.sock.send(p.get_buffer() + int_to_bytes(ln, 2) + seg_data + bytes([0])*pad_length)

    @wire_operation
    def _op_close_blob(self, blob_handle):
        p = Packer()
        p.pack_int(self.op_close_blob)
        p.pack_int(blob_handle)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_ping(self):
        p = Packer()
        p.pack_int(self.op_ping)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_que_events(self, event_count, event_id):
        if self.db_handle is None:
            raise OperationalError('_op_que_events() Invalid db handle')
        params = bytes([1])
        for name, n in event_count.items():
            params += bytes([len(name)])
            params += self.str_to_bytes(name)
            params += int_to_bytes(n, 4)
        p = Packer()
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
        p = Packer()
        p.pack_int(self.op_cancel_events)
        p.pack_int(self.db_handle)
        p.pack_int(event_id)
        self.sock.send(p.get_buffer())

    @wire_operation
    def _op_connect_request(self):
        if self.db_handle is None:
            raise OperationalError('_op_connect_request() Invalid db handle')
        p = Packer()
        p.pack_int(self.op_connect_request)
        p.pack_int(1)    # async
        p.pack_int(self.db_handle)
        p.pack_int(0)
        self.sock.send(p.get_buffer())

