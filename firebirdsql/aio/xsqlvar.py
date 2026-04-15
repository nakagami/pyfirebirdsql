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
from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql.wireprotocol import INFO_SQL_SELECT_DESCRIBE_VARS
from firebirdsql.xsqlvar import parse_select_items


async def async_parse_xsqlda(buf, connection, stmt_handle):
    xsqlda = []
    stmt_type = None
    i = 0
    while i < len(buf):
        if buf[i:i+3] == bytes([isc_info_sql_stmt_type, 0x04, 0x00]):
            stmt_type = bytes_to_int(buf[i+3:i+7])
            i += 7
        elif buf[i:i+2] == bytes([isc_info_sql_select, isc_info_sql_describe_vars]):
            i += 2
            ln = bytes_to_int(buf[i:i+2])
            i += 2
            col_len = bytes_to_int(buf[i:i+ln])
            xsqlda = [None] * col_len
            next_index = parse_select_items(buf[i+ln:], xsqlda, connection)
            while next_index > 0:   # more describe vars
                connection._op_info_sql(
                    stmt_handle,
                    bytes([isc_info_sql_sqlda_start, 2]) + int_to_bytes(next_index, 2) + INFO_SQL_SELECT_DESCRIBE_VARS
                )
                (h, oid, buf) = await connection._async_op_response()
                assert buf[:2] == bytes([0x04, 0x07])
                ln = bytes_to_int(buf[2:4])
                assert bytes_to_int(buf[4:4+ln]) == col_len
                next_index = parse_select_items(buf[4+ln:], xsqlda, connection)
        else:
            break
    return stmt_type, xsqlda
