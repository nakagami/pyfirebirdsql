##############################################################################
# Copyright (c) 2009-2012 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import xdrlib, time, datetime, decimal, struct
from firebirdsql.fberrmsgs import messages
from firebirdsql import (DatabaseError, InternalError, OperationalError, 
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

def wire_operation(fn):
    if not DEBUG:
        return fn
    def f(*args):
        print('<----', fn, args)
        r = fn(*args)
        print(fn, '---->')
        return r
    return f

def bytes_to_bint(b):           # Read as big endian
    fmtmap = {1: 'b', 2: '>h', 4: '>l', 8: '>q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bytes_to_int(b):            # Read as little endian.
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

def recv_channel(sock, nbytes, word_alignment=False):
    n = nbytes
    if word_alignment and (n % 4):
        n += 4 - nbytes % 4  # 4 bytes word alignment
    r = bytes([])
    while n:
        b = sock.recv(n)
        r += b
        n -= len(b)
    return r[:nbytes]

def send_channel(sock, b):
    sock.send(b)

def params_to_blr(params):
    "Convert parameter array to BLR and values format."
    ln = len(params) * 2
    blr = bytes([5, 2, 4, 0, ln & 255, ln >> 8])
    values = bytes([])
    for p in params:
        t = type(p)
        if ((PYTHON_MAJOR_VER == 2 and t == str) or
            (PYTHON_MAJOR_VER == 3 and t == bytes)):
            v = p
            nbytes = len(v)
            pad_length = ((4-nbytes) & 3)
            v += bytes([0]) * pad_length
            blr += bytes([14, nbytes & 255, nbytes >> 8])
        elif t == int:
            v = bint_to_bytes(p, 4)
            blr += bytes([7, 0])
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
        elif p == None:
            v = bytes([0]) * 8
            blr += bytes([9, 0])
        values += v
        blr += bytes([7, 0])
        values += bytes([0]) * 4 if p != None else bytes([0xff,0xff,0x34,0x8c])
    blr += bytes([255, 76])    # [blr_end, blr_eoc]
    return blr, values


class WireProtocol:
    buffer_length = 1024

    op_connect = 1
    op_accept = 3
    op_response = 9
    op_dummy = 17
    op_attach = 19
    op_create = 20
    op_detach = 21
    op_transaction = 29
    op_commit = 30
    op_commit_retaining = 50
    op_rollback_retaining = 86
    op_rollback = 31
    op_open_blob = 35
    op_get_segment = 36
    op_close_blob = 39
    op_info_database = 40
    op_info_transaction = 42
    op_batch_segments = 44
    op_allocate_statement = 62
    op_create_blob2 = 57
    op_execute = 63
    op_execute_immediate = 64
    op_fetch = 65
    op_fetch_response = 66
    op_free_statement = 67
    op_prepare_statement = 68
    op_info_sql = 70
    op_execute2 = 76
    op_sql_response = 78
    op_drop_database = 81
    op_service_attach = 82
    op_service_detach = 83
    op_service_info = 84
    op_service_start = 85

    charset_map = {
    # DB CHAR SET NAME    :   PYTHON CODEC NAME (CANONICAL)
    # --------------------------------------------------------------------------
    'OCTETS'              :   None, # Allow to pass through unchanged.
    'UNICODE_FSS'         :   'utf_8',
    'UTF8'                :   'utf_8', # (Firebird 2.0+)
    'SJIS_0208'           :   'shift_jis',
    'EUCJ_0208'           :   'euc_jp',
    'DOS737'              :   'cp737',
    'DOS437'              :   'cp437',
    'DOS850'              :   'cp850',
    'DOS865'              :   'cp865',
    'DOS860'              :   'cp860',
    'DOS863'              :   'cp863',
    'DOS775'              :   'cp775',
    'DOS862'              :   'cp862',
    'DOS864'              :   'cp864',
    'ISO8859_1'           :   'iso8859_1',
    'ISO8859_2'           :   'iso8859_2',
    'ISO8859_3'           :   'iso8859_3',
    'ISO8859_4'           :   'iso8859_4',
    'ISO8859_5'           :   'iso8859_5',
    'ISO8859_6'           :   'iso8859_6',
    'ISO8859_7'           :   'iso8859_7',
    'ISO8859_8'           :   'iso8859_8',
    'ISO8859_9'           :   'iso8859_9',
    'ISO8859_13'          :   'iso8859_13',
    'KSC_5601'            :   'euc_kr',
    'DOS852'              :   'cp852',
    'DOS857'              :   'cp857',
    'DOS861'              :   'cp861',
    'DOS866'              :   'cp866',
    'DOS869'              :   'cp869',
    'WIN1250'             :   'cp1250',
    'WIN1251'             :   'cp1251',
    'WIN1252'             :   'cp1252',
    'WIN1253'             :   'cp1253',
    'WIN1254'             :   'cp1254',
    'BIG_5'               :   'big5',
    'GB_2312'             :   'gb2312',
    'WIN1255'             :   'cp1255',
    'WIN1256'             :   'cp1256',
    'WIN1257'             :   'cp1257',
    'KOI8-R'              :   'koi8_r', # (Firebird 2.0+)
    'KOI8-U'              :   'koi8_u', # (Firebird 2.0+)
    'WIN1258'             :   'cp1258', # (Firebird 2.0+)
    }

    def str_to_bytes(self, s):
        return s.encode(self.charset_map.get(self.charset, self.charset))

    def bytes_to_str(self, b):
        "convert bytes array to raw string"
        if PYTHON_MAJOR_VER == 3:
            return b.decode(self.charset_map.get(self.charset, self.charset))
        return b

    def bytes_to_ustr(self, b):
        "convert bytes array to unicode string"
        return b.decode(self.charset_map.get(self.charset, self.charset))

    def _parse_op_response(self):
        b = recv_channel(self.sock, 16)
        h = bytes_to_bint(b[0:4])         # Object handle
        oid = b[4:12]                       # Object ID
        buf_len = bytes_to_bint(b[12:])   # buffer length
        buf = recv_channel(self.sock, buf_len, True)

        # Parse status vector
        sql_code = 0
        gds_codes = set()
        message = ''
        n = bytes_to_bint(recv_channel(self.sock, 4))
        while n != isc_arg_end:
            if n == isc_arg_gds:
                gds_code = bytes_to_bint(recv_channel(self.sock, 4))
                if gds_code:
                    gds_codes.add(gds_code)
                    message += messages.get(gds_code, '@1')
                    num_arg = 0
            elif n == isc_arg_number:
                num = bytes_to_bint(recv_channel(self.sock, 4))
                if gds_code == 335544436:
                    sql_code = num
                num_arg += 1
                message = message.replace('@' + str(num_arg), str(num))
            elif n == isc_arg_string or n == isc_arg_interpreted:
                nbytes = bytes_to_bint(recv_channel(self.sock, 4))
                n = str(recv_channel(self.sock, nbytes, True))
                num_arg += 1
                message = message.replace('@' + str(num_arg), n)
            n = bytes_to_bint(recv_channel(self.sock, 4))

        if sql_code or message:
            raise OperationalError(message, gds_codes, sql_code)

        return (h, oid, buf)

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
        dpb += bytes([68, len(s)]) + s
        dpb += bytes([48, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bytes([28, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bytes([29, len(s)]) + s
        dpb += bytes([63, 4]) + int_to_bytes(3, 4) # isc_dpb_sql_dialect = 3
        dpb += bytes([24, 4]) + bint_to_bytes(1, 4) # isc_dpb_force_write = 1
        dpb += bytes([54, 4]) + bint_to_bytes(1, 4) # isc_dpb_overwirte = 1
        dpb += bytes([4, 4]) + int_to_bytes(page_size, 4)
        p = xdrlib.Packer()
        p.pack_int(self.op_create)
        p.pack_int(0)                       # Database Object ID
        p.pack_string(self.str_to_bytes(self.filename))
        p.pack_bytes(dpb)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_accept(self):
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        assert bytes_to_bint(b) == self.op_accept
        b = recv_channel(self.sock, 12)
        up = xdrlib.Unpacker(b)
        assert up.unpack_int() == 10
        assert  up.unpack_int() == 1
        assert up.unpack_int() == 3
        up.done()

    @wire_operation
    def _op_attach(self):
        dpb = bytes([1])
        s = self.str_to_bytes(self.charset)
        dpb += bytes([48, len(s)]) + s
        s = self.str_to_bytes(self.user)
        dpb += bytes([28, len(s)]) + s
        s = self.str_to_bytes(self.password)
        dpb += bytes([29, len(s)]) + s
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
    def _op_allocate_statement(self, trans_handle):
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
    def _op_info_database(self, b):
        p = xdrlib.Packer()
        p.pack_int(self.op_info_database)
        p.pack_int(self.db_handle)
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
            (blr, values) = params_to_blr(params)
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
            (blr, values) = params_to_blr(params)
            p.pack_bytes(blr)
            p.pack_int(0)
            p.pack_int(1)
            send_channel(self.sock, p.get_buffer() + values)

        p = xdrlib.Packer()
        p.pack_bytes(output_blr)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())

    @wire_operation
    def _op_execute_immediate(self, trans_handle, db_handle, sql='', params=[],
                                    in_msg='', out_msg='', possible_requests=0):
        sql = self.str_to_bytes(sql)
        in_msg = self.str_to_bytes(in_msg)
        out_msg = self.str_to_bytes(out_msg)
        r = bint_to_bytes(self.op_execute_immediate, 4)
        r += bint_to_bytes(trans_handle, 4) + bint_to_bytes(db_handle, 4)
        r += bint_to_bytes(len(sql), 2) + sql
        r += bint_to_bytes(3, 2)    # dialect
        if len(params) == 0:
            r += bint_to_bytes(0, 2)    # in_blr len
            values = bytes([])
        else:
            (blr, values) = params_to_blr(params)
            r += bint_to_bytes(len(blr), 2) + blr
        r += bint_to_bytes(len(in_msg), 2) + in_msg
        r += bint_to_bytes(0, 2)    # unknown short int 0
        r += bint_to_bytes(len(out_msg), 2) + out_msg
        r += bint_to_bytes(possible_requests, 4)
        r += bytes([0]) * ((4-len(r+values)) & 3)    # padding
        send_channel(self.sock, r + values)

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
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        if bytes_to_bint(b) == self.op_response:
            return self._parse_op_response()    # error occured
        if bytes_to_bint(b) != self.op_fetch_response:
            raise InternalError
        b = recv_channel(self.sock, 8)
        status = bytes_to_bint(b[:4])
        count = bytes_to_bint(b[4:8])
        rows = []
        while count:
            r = [None] * len(xsqlda)
            for i in range(len(xsqlda)):
                x = xsqlda[i]
                if x.io_length() < 0:
                    b = recv_channel(self.sock, 4)
                    ln = bytes_to_bint(b)
                else:
                    ln = x.io_length()
                raw_value = recv_channel(self.sock, ln, True)
                if recv_channel(self.sock, 4) == bytes([0]) * 4: # Not NULL
                    r[i] = x.value(raw_value)
            rows.append(r)
            b = recv_channel(self.sock, 12)
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
    def _op_response(self):
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        if bytes_to_bint(b) != self.op_response:
            raise InternalError
        return self._parse_op_response()

    @wire_operation
    def _op_sql_response(self, xsqlda):
        b = recv_channel(self.sock, 4)
        while bytes_to_bint(b) == self.op_dummy:
            b = recv_channel(self.sock, 4)
        if bytes_to_bint(b) != self.op_sql_response:
            raise InternalError

        b = recv_channel(self.sock, 4)
        count = bytes_to_bint(b[:4])

        r = []
        for i in range(len(xsqlda)):
            x = xsqlda[i]
            if x.io_length() < 0:
                b = recv_channel(self.sock, 4)
                ln = bytes_to_bint(b)
            else:
                ln = x.io_length()
            raw_value = recv_channel(self.sock, ln, True)
            if recv_channel(self.sock, 4) == bytes([0]) * 4: # Not NULL
                r.append(x.value(raw_value))
            else:
                r.append(None)

        b = recv_channel(self.sock, 32)     # ??? why 32 bytes skip

        return r
