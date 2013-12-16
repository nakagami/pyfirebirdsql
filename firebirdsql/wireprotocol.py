##############################################################################
# Copyright (c) 2009-2013 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import os
import socket
import xdrlib, time, datetime, decimal, struct, select
from firebirdsql.fberrmsgs import messages
from firebirdsql import (DisconnectByPeer,
    DatabaseError, InternalError, OperationalError,
    ProgrammingError, IntegrityError, DataError, NotSupportedError,
)
from firebirdsql.consts import *

DEBUG = False

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

def convert_date(v):  # Convert datetime.date to BLR format data
    i = v.month + 9
    jy = v.year + (i // 12) -1
    jm = i % 12
    c = jy // 100
    jy -= 100 * c
    j = (146097*c) // 4 + (1461*jy) // 4 + (153*jm+2) // 5 + v.day - 678882
    return bint_to_bytes(j, 4)

def convert_time(v):  # Convert datetime.time to BLR format time
    t = (v.hour*3600 + v.minute*60 + v.second) *10000 + v.microsecond // 100
    return bint_to_bytes(t, 4)

def convert_timestamp(v):   # Convert datetime.datetime to BLR format timestamp
    return convert_date(v.date()) + convert_time(v.time())

def wire_operation(fn):
    if not DEBUG:
        return fn
    def f(*args, **kwargs):
        print('<----', fn, args)
        r = fn(*args, **kwargs)
        print(fn, '---->')
        return r
    return f

def bytes_to_bint(b, u=False):           # Read as big endian
    if u:
        fmtmap = {1: 'B', 2: '>H', 4: '>L', 8: '>Q'}
    else:
        fmtmap = {1: 'b', 2: '>h', 4: '>l', 8: '>q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bytes_to_int(b, u=False):            # Read as little endian.
    if u:
        fmtmap = {1: 'B', 2: '<H', 4: '<L', 8: '<Q'}
    else:
        fmtmap = {1: 'b', 2: '<h', 4: '<l', 8: '<q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bint_to_bytes(val, nbytes): # Convert int value to big endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8*(nbytes - n - 1)) & 0xff))
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[-1] += 1
        for i in range(nbytes):
            if b[nbytes -i -1] == 256:
                b[nbytes -i -1] = 0
                b[nbytes -i -2] += 1
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

def byte_to_int(b):
    "byte to int"
    if PYTHON_MAJOR_VER == 3:
        return b
    else:
        return ord(b)

def send_channel(sock, b):
    n = 0
    while (n < len(b)):
        n += sock.send(b[n:])

class WireProtocol:
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

    def recv_channel(self, nbytes, word_alignment=False):
        n = nbytes
        if word_alignment and (n % 4):
            n += 4 - nbytes % 4  # 4 bytes word alignment
        r = bytes([])
        while n:
            if (os.name != 'java'
                and self.timeout is not None
                and select.select([self.sock], [], [], self.timeout)[0] == []):
                break
            b = self.sock.recv(n)
            if not b:
                break
            r += b
            n -= len(b)
        if len(r) < nbytes:
            raise OperationalError('Can not recv() packets', None, None)
        return r[:nbytes]

    def str_to_bytes(self, s):
        "convert str to bytes"
        if (PYTHON_MAJOR_VER == 3 or
                (PYTHON_MAJOR_VER == 2 and type(s)==unicode)):
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
            elif (n == isc_arg_string or
                    n == isc_arg_interpreted
                    or n == isc_arg_sql_state):
                nbytes = bytes_to_bint(self.recv_channel(4))
                s = str(self.recv_channel(nbytes, word_alignment=True))
                num_arg += 1
                message = message.replace('@' + str(num_arg), s)
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
        if sql_code or message:
            raise OperationalError(message, gds_codes, sql_code)

        return (h, oid, buf)

    def _parse_op_event(self):
        b = self.recv_channel(4096) # too large TODO: read step by step
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
        blr = bytes([5, 2, 4, 0, ln & 255, ln >> 8])
        values = bytes([])
        for p in params:
            t = type(p)
            if ((PYTHON_MAJOR_VER == 2 and type(p) == unicode) or
                (PYTHON_MAJOR_VER == 3 and type(p) == str)):
                p = self.str_to_bytes(p)
                t = type(p)
            if ((PYTHON_MAJOR_VER == 2 and t == str) or
                (PYTHON_MAJOR_VER == 3 and t == bytes)):
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
                v = bint_to_bytes(p, 4)
                blr += bytes([8, 0])    # blr_long
            elif t == decimal.Decimal or t == float:
                if t == float:
                    p = decimal.Decimal(str(p))
                (sign, digits, exponent) = p.as_tuple()
                v = 0
                ln = len(digits)
                for i in range(ln):
                    v += digits[i] * (10 ** (ln -i-1))
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
                v = convert_time(p)
                blr += bytes([13])
            elif t == datetime.datetime:
                v = convert_timestamp(p)
                blr += bytes([35])
            elif t == bool:
                v = bytes([1, 0, 0, 0]) if p else bytes([0, 0, 0, 0])
                blr += bytes([23])
            else:   # fallback, convert to string
                if p is None:
                    v = bytes([])
                else:
                    p = p.__repr__()
                    if (PYTHON_MAJOR_VER==3 or
                        (PYTHON_MAJOR_VER == 2 and type(p)==unicode)):
                        p = self.str_to_bytes(p)
                    v = p
                nbytes = len(v)
                pad_length = ((4-nbytes) & 3)
                v += bytes([0]) * pad_length
                blr += bytes([14, nbytes & 255, nbytes >> 8])
            values += v
            blr += bytes([7, 0])
            values += bytes([0]) * 4 if p != None else bytes([0xff,0xff,0xff,0xff])
        blr += bytes([255, 76])    # [blr_end, blr_eoc]
        return blr, values

    def uid(self):
        if sys.platform == 'win32':
            user = os.environ['USERNAME']
            hostname = os.environ['COMPUTERNAME']
        else:
            user = os.environ.get('USER', '')
            hostname = socket.gethostname()
        return bytes([1] + [len(user)] + [ord(c) for c in user]
                + [4] + [len(hostname)] + [ord(c) for c in hostname] + [6, 0])

    @wire_operation
    def _op_connect(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_connect)
        p.pack_int(self.op_attach)
        p.pack_int(2)   # CONNECT_VERSION2
        p.pack_int(1)   # Arch type (Generic = 1)
        p.pack_string(self.str_to_bytes(self.filename if self.filename else ''))
        p.pack_int(1)   # Protocol version understood count.
        p.pack_bytes(self.uid())
        p.pack_int(10)  # PROTOCOL_VERSION10
        p.pack_int(1)   # Arch type (Generic = 1)
        p.pack_int(2)   # Min type
        p.pack_int(3)   # Max type
        p.pack_int(2)   # Preference weight
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_create(self, page_size=4096):
        dpb = bytes([1])
        s = self.str_to_bytes(self.charset)
        dpb += bytes([isc_dpb_set_db_charset, len(s)]) + s
        dpb += bytes([isc_dpb_lc_ctype, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bytes([isc_dpb_user_name, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bytes([isc_dpb_password, len(s)]) + s
        if self.role:
            s = self.str_to_bytes(self.role)
            dpb += bytes([isc_dpb_sql_role_name, len(s)]) + s
        dpb += bytes([isc_dpb_sql_dialect, 4]) + int_to_bytes(3, 4)
        dpb += bytes([isc_dpb_force_write, 4]) + bint_to_bytes(1, 4)
        dpb += bytes([isc_dpb_overwrite, 4]) + bint_to_bytes(1, 4)
        dpb += bytes([isc_dpb_page_size, 4]) + int_to_bytes(page_size, 4)
        p = xdrlib.Packer()
        p.pack_int(self.op_create)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_accept(self):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_reject:
            raise OperationalError('Connection is rejected', None, None)
        assert bytes_to_bint(b) == self.op_accept
        b = self.recv_channel(12)
        up = xdrlib.Unpacker(b)
        assert up.unpack_int() == 10
        assert  up.unpack_int() == 1
        assert up.unpack_int() == 3
        up.done()

    @wire_operation
    def _op_attach(self):
        dpb = bytes([1])
        s = self.str_to_bytes(self.charset)
        dpb += bytes([isc_dpb_lc_ctype, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bytes([isc_dpb_user_name, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bytes([isc_dpb_password, len(s)]) + s
        if self.role:
            s = self.str_to_bytes(self.role)
            dpb += bytes([isc_dpb_sql_role_name, len(s)]) + s
        p = xdrlib.Packer()
        p.pack_int(self.op_attach)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_drop_database(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_drop_database)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_attach(self):
        dpb = bytes([2,2])
        s = self.str_to_bytes(self.user)
        dpb += bytes([isc_spb_user_name, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bytes([isc_spb_password, len(s)]) + s
        dpb += bytes([isc_spb_dummy_packet_interval,0x04,0x78,0x0a,0x00,0x00])
        p = xdrlib.Packer()
        p.pack_int(self.op_service_attach)
        p.pack_int(0)
        p.pack_string(self.str_to_bytes('service_mgr'))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_info(self, param, item, buffer_length=512):
        p = xdrlib.Packer()
        p.pack_int(self.op_service_info)
#        p.pack_int(self.svc_handle)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        p.pack_bytes(item)
        p.pack_int(buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_start(self, param):
        p = xdrlib.Packer()
        p.pack_int(self.op_service_start)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(param)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_service_detach(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_service_detach)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_info_database(self, b):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_database)
        p.pack_int(self.db_handle)
        p.pack_int(0)
        p.pack_bytes(b)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_transaction(self, tpb):
        p = xdrlib.Packer()
        p.pack_int(self.op_transaction)
        p.pack_int(self.db_handle)
        p.pack_bytes(tpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_commit(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_commit_retaining(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_commit_retaining)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_rollback(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_rollback_retaining(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_rollback_retaining)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_allocate_statement(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_allocate_statement)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_info_transaction(self, trans_handle, b):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_transaction)
        p.pack_int(trans_handle)
        p.pack_int(0)
        p.pack_bytes(b)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_free_statement(self, stmt_handle, mode):
        p = xdrlib.Packer()
        p.pack_int(self.op_free_statement)
        p.pack_int(stmt_handle)
        p.pack_int(mode)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_prepare_statement(self, stmt_handle, trans_handle, query, option_items=bytes([])):
        desc_items = option_items + bytes([isc_info_sql_stmt_type])+INFO_SQL_SELECT_DESCRIBE_VARS
        p = xdrlib.Packer()
        p.pack_int(self.op_prepare_statement)
        p.pack_int(trans_handle)
        p.pack_int(stmt_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_string(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_info_sql(self, stmt_handle, vars):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_sql)
        p.pack_int(stmt_handle)
        p.pack_int(0)
        p.pack_bytes(vars)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_execute(self, stmt_handle, trans_handle, params):
        p = xdrlib.Packer()
        p.pack_int(self.op_execute)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            p.pack_bytes(bytes([]))
            p.pack_int(0)
            p.pack_int(0)
            send_channel(self.sock, p.get_buffer())
        else:
            (blr, values) = self.params_to_blr(trans_handle, params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)
            send_channel(self.sock, p.get_buffer() + values)

    @wire_operation
    def _op_execute2(self, stmt_handle, trans_handle, params, output_blr):
        p = xdrlib.Packer()
        p.pack_int(self.op_execute2)
        p.pack_int(stmt_handle)
        p.pack_int(trans_handle)

        if len(params) == 0:
            p.pack_bytes(bytes([]))
            p.pack_int(0)
            p.pack_int(0)
            send_channel(self.sock, p.get_buffer())
        else:
            (blr, values) = self.params_to_blr(trans_handle, params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)
            send_channel(self.sock, p.get_buffer() + values)

        p = xdrlib.Packer()
        p.pack_bytes(output_blr)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_exec_immediate(self, trans_handle, query):
        desc_items = bytes([])
        p = xdrlib.Packer()
        p.pack_int(self.op_exec_immediate)
        p.pack_int(trans_handle)
        p.pack_int(self.db_handle)
        p.pack_int(3)   # dialect = 3
        p.pack_string(self.str_to_bytes(query))
        p.pack_bytes(desc_items)
        p.pack_int(self.buffer_length)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_fetch(self, stmt_handle, blr):
        p = xdrlib.Packer()
        p.pack_int(self.op_fetch)
        p.pack_int(stmt_handle)
        p.pack_bytes(blr)
        p.pack_int(0)
        p.pack_int(400)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_fetch_response(self, stmt_handle, xsqlda):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_response:
            return self._parse_op_response()    # error occured
        if bytes_to_bint(b) != self.op_fetch_response:
            raise InternalError
        b = self.recv_channel(8)
        status = bytes_to_bint(b[:4])
        count = bytes_to_bint(b[4:8])
        rows = []
        while count:
            r = [None] * len(xsqlda)
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if x.io_length() < 0:
                    b = self.recv_channel(4)
                    ln = bytes_to_bint(b)
                else:
                    ln = x.io_length()
                raw_value = self.recv_channel(ln, word_alignment=True)
                if self.recv_channel(4) == bytes([0]) * 4: # Not NULL
                    r[i] = x.value(raw_value)
            rows.append(r)
            b = self.recv_channel(12)
            op = bytes_to_bint(b[:4])
            status = bytes_to_bint(b[4:8])
            count = bytes_to_bint(b[8:])
        return rows, status != 100

    @wire_operation
    def _op_detach(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_detach)
        p.pack_int(self.db_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_open_blob(self, blob_id, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_open_blob)
        p.pack_int(trans_handle)
        send_channel(self.sock, p.get_buffer() + blob_id)

    @wire_operation
    def _op_create_blob2(self, trans_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_create_blob2)
        p.pack_int(0)
        p.pack_int(trans_handle)
        p.pack_int(0)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_get_segment(self, blob_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_get_segment)
        p.pack_int(blob_handle)
        p.pack_int(self.buffer_length)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_put_segment(self, blob_handle, b):
        p = xdrlib.Packer()
        p.pack_int(self.op_put_segment)
        p.pack_int(blob_handle)
        p.pack_int(len(b))
        p.pack_int(len(b))
        send_channel(self.sock, p.get_buffer() + b)

    @wire_operation
    def _op_batch_segments(self, blob_handle, seg_data):
        ln = len(seg_data)
        p = xdrlib.Packer()
        p.pack_int(self.op_batch_segments)
        p.pack_int(blob_handle)
        p.pack_int(ln + 2)
        p.pack_int(ln + 2)
        pad_length = ((4-(ln+2)) & 3)
        send_channel(self.sock, p.get_buffer() 
                + int_to_bytes(ln, 2) + seg_data + bytes([0])*pad_length)

    @wire_operation
    def _op_close_blob(self, blob_handle):
        p = xdrlib.Packer()
        p.pack_int(self.op_close_blob)
        p.pack_int(blob_handle)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_que_events(self, event_names, ast, args, event_id):
        params = bytes([1])
        for name, n in event_names.items():
            params += bytes([len(name)])
            params += self.str_to_bytes(name)
            params += int_to_bytes(n, 4)
        p = xdrlib.Packer()
        p.pack_int(self.op_que_events)
        p.pack_int(self.db_handle)
        p.pack_bytes(params)
        p.pack_int(ast)
        p.pack_int(args)
        p.pack_int(event_id)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_cancel_events(self, event_id):
        p = xdrlib.Packer()
        p.pack_int(self.op_cancel_events)
        p.pack_int(self.db_handle)
        p.pack_int(event_id)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_connect_request(self):
        p = xdrlib.Packer()
        p.pack_int(self.op_connect_request)
        p.pack_int(1)    # async
        p.pack_int(self.db_handle)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) != self.op_response:
            raise InternalError

        h = bytes_to_bint(self.recv_channel(4))
        self.recv_channel(8)  # garbase
        ln = bytes_to_bint(self.recv_channel(4))
        ln += ln % 4    # padding
        family = bytes_to_bint(self.recv_channel(2))
        port = bytes_to_bint(self.recv_channel(2), u=True)
        b = self.recv_channel(4)
        ip_address = '.'.join([str(byte_to_int(c)) for c in b])
        ln -= 8
        self.recv_channel(ln)

        (gds_codes, sql_code, message) = self._parse_status_vector()
        if sql_code or message:
            raise OperationalError(message, gds_codes, sql_code)

        return (h, port, family, ip_address)

    @wire_operation
    def _op_response(self):
        b = self.recv_channel(4)
        if b is None:
            return
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) != self.op_response:
            raise InternalError
        return self._parse_op_response()

    @wire_operation
    def _op_event(self):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) == self.op_response:
            return self._parse_op_response()
        if bytes_to_bint(b) == self.op_exit or bytes_to_bint(b) == self.op_exit:
            raise DisconnectByPeer
        if bytes_to_bint(b) != self.op_event:
            raise InternalError
        return self._parse_op_event()

    @wire_operation
    def _op_sql_response(self, xsqlda):
        b = self.recv_channel(4)
        while bytes_to_bint(b) == self.op_dummy:
            b = self.recv_channel(4)
        if bytes_to_bint(b) != self.op_sql_response:
            raise InternalError

        b = self.recv_channel(4)
        count = bytes_to_bint(b[:4])
        r = []
        if count == 0:
            return []
        for i in range(len(xsqlda)):
            x = xsqlda[i]
            if x.io_length() < 0:
                b = self.recv_channel(4)
                ln = bytes_to_bint(b)
            else:
                ln = x.io_length()
            raw_value = self.recv_channel(ln, word_alignment=True)
            if self.recv_channel(4) == bytes([0]) * 4: # Not NULL
                r.append(x.value(raw_value))
            else:
                r.append(None)
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
                db_handle = bytes_to_int(self.recv_channel(4))
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
