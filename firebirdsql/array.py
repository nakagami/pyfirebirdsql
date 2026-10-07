##############################################################################
# Copyright (c) 2026, Hajime Nakagami<nakagami@gmail.com>
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
import decimal
import datetime
from typing import Any, Sequence

from firebirdsql.consts import *    # noqa
from firebirdsql.err import OperationalError
from firebirdsql.utils import bint_to_bytes, bytes_to_bint
from firebirdsql.xsqlvar import XSQLVAR, calc_blr


class ArrayDimension:
    def __init__(self, lower_bound: int, length: int) -> None:
        self.lower_bound = lower_bound
        self.length = length

    def __repr__(self) -> str:
        return f"ArrayDimension(lower_bound={self.lower_bound}, length={self.length})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ArrayDimension):
            return self.lower_bound == other.lower_bound and self.length == other.length
        return False


class ArrayMeta:
    def __init__(
        self,
        table_name: str,
        field_name: str,
        type_id: int,
        blr_type_id: int,
        scale: int,
        length: int,
        field_bytes: int,
        sub_type: int,
        dimensions: list[ArrayDimension] | None = None,
        field_source: str = '',
    ) -> None:
        self.table_name = table_name
        self.field_name = field_name
        self.type_id = type_id
        self.blr_type_id = blr_type_id
        self.scale = scale
        self.length = length
        self.field_bytes = field_bytes
        self.sub_type = sub_type
        self.dimensions = dimensions or []
        self.field_source = field_source

    def __repr__(self) -> str:
        return (
            f"ArrayMeta(table_name={self.table_name!r}, field_name={self.field_name!r}, "
            f"type_id={self.type_id}, blr_type_id={self.blr_type_id}, scale={self.scale}, "
            f"length={self.length}, field_bytes={self.field_bytes}, sub_type={self.sub_type}, "
            f"dimensions={self.dimensions!r})"
        )


class Array:
    """Wrapper for array column values."""
    def __init__(self, elements: Any = None, dimensions: list[ArrayDimension] | None = None) -> None:
        self.elements = elements
        self.dimensions = dimensions

    def __repr__(self) -> str:
        return f"Array(elements={self.elements!r}, dimensions={self.dimensions!r})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Array):
            return self.elements == other.elements and self.dimensions == other.dimensions
        elif isinstance(other, (list, tuple)):
            return self.elements == other
        return False


def array_type_from_blr(blr: int) -> int:
    if blr in (blr_type_varying, blr_type_varying2):
        return SQL_TYPE_VARYING
    elif blr in (blr_type_text, blr_type_text2, blr_type_cstring, blr_type_cstring2):
        return SQL_TYPE_TEXT
    elif blr == 7:  # blr_short
        return SQL_TYPE_SHORT
    elif blr == 8:  # blr_long
        return SQL_TYPE_LONG
    elif blr == 9:  # blr_quad
        return SQL_TYPE_QUAD
    elif blr == blr_type_int64:
        return SQL_TYPE_INT64
    elif blr == blr_type_double:
        return SQL_TYPE_DOUBLE
    elif blr == 10:  # blr_float
        return SQL_TYPE_FLOAT
    elif blr == 11:  # blr_d_float
        return SQL_TYPE_D_FLOAT
    elif blr == 12:  # blr_sql_date
        return SQL_TYPE_DATE
    elif blr == 13:  # blr_sql_time
        return SQL_TYPE_TIME
    elif blr == blr_type_time_tz:
        return SQL_TYPE_TIME_TZ
    elif blr == blr_type_timestamp:
        return SQL_TYPE_TIMESTAMP
    elif blr == blr_type_ts_tz:
        return SQL_TYPE_TIMESTAMP_TZ
    elif blr == blr_type_bool:
        return SQL_TYPE_BOOLEAN
    elif blr == blr_type_dec64:
        return SQL_TYPE_DEC64
    elif blr == blr_type_dec128:
        return SQL_TYPE_DEC128
    elif blr == blr_type_int128:
        return SQL_TYPE_INT128
    else:
        raise OperationalError(f"unknown array element blr type {blr}")


def append_dimension_bound(sdl: bytearray, v: int) -> None:
    if -128 <= v <= 127:
        sdl.append(isc_sdl_tiny_integer)
        sdl.append(v & 0xFF)
    elif -32768 <= v <= 32767:
        sdl.append(isc_sdl_short_integer)
        sdl.extend(struct.pack('<h', v))
    else:
        sdl.append(isc_sdl_long_integer)
        sdl.extend(struct.pack('<i', v))


def generate_sdl(meta: ArrayMeta, dims: list[ArrayDimension], for_get: bool = False) -> bytes:
    sdl = bytearray([isc_sdl_version1, isc_sdl_struct, 1, meta.blr_type_id & 0xFF])

    if meta.blr_type_id in (7, 8, 9, blr_type_int64, blr_type_int128):
        sdl.append(meta.scale & 0xFF)
    elif meta.blr_type_id in (blr_type_text, blr_type_text2, blr_type_cstring, blr_type_cstring2, blr_type_varying):
        sdl.extend(struct.pack('<H', meta.field_bytes))

    table_bytes = meta.table_name.encode('utf-8')
    sdl.extend([isc_sdl_relation, len(table_bytes)])
    sdl.extend(table_bytes)

    field_bytes = meta.field_name.encode('utf-8')
    sdl.extend([isc_sdl_field, len(field_bytes)])
    sdl.extend(field_bytes)

    for i, d in enumerate(dims):
        if d.lower_bound == 1:
            sdl.extend([isc_sdl_do1, i])
        else:
            sdl.extend([isc_sdl_do2, i])
            append_dimension_bound(sdl, d.lower_bound)
        append_dimension_bound(sdl, d.lower_bound + d.length - 1)

    sdl.extend([isc_sdl_element, 1, isc_sdl_scalar, 0, len(dims)])
    for i in range(len(dims)):
        sdl.extend([isc_sdl_variable, i])
    sdl.append(isc_sdl_eoc)
    return bytes(sdl)


def array_element_stride(meta: ArrayMeta) -> int:
    if meta.type_id == SQL_TYPE_SHORT:
        return 2
    elif meta.type_id in (SQL_TYPE_LONG, SQL_TYPE_FLOAT, SQL_TYPE_DATE, SQL_TYPE_TIME):
        return 4
    elif meta.type_id in (SQL_TYPE_INT64, SQL_TYPE_DOUBLE, SQL_TYPE_TIMESTAMP, SQL_TYPE_QUAD, SQL_TYPE_DEC64):
        return 8
    elif meta.type_id in (SQL_TYPE_INT128, SQL_TYPE_DEC128):
        return 16
    elif meta.type_id == SQL_TYPE_BOOLEAN:
        return 1
    elif meta.type_id == SQL_TYPE_TEXT:
        return meta.field_bytes
    elif meta.type_id == SQL_TYPE_VARYING:
        return 2 + meta.field_bytes
    else:
        return meta.field_bytes


def array_get_element_stride(meta: ArrayMeta) -> int:
    return array_element_stride(meta)


def all_elements_count(dimensions: list[ArrayDimension]) -> int:
    if not dimensions:
        return 0
    count = 1
    for d in dimensions:
        if d.length <= 0:
            return 0
        count *= d.length
    return count


def array_slice_app_length(meta: ArrayMeta, dims: list[ArrayDimension]) -> int:
    count = all_elements_count(dims)
    if count == 0:
        return 0
    return count * array_element_stride(meta)


def array_slice_get_length(meta: ArrayMeta, dims: list[ArrayDimension]) -> int:
    count = all_elements_count(dims)
    if count == 0:
        return 0
    return count * array_get_element_stride(meta)


def round_up_4(n: int) -> int:
    r = n % 4
    if r != 0:
        return n + 4 - r
    return n


def slice_wire_element_size(meta: ArrayMeta) -> tuple[int, bool]:
    if meta.type_id in (SQL_TYPE_INT64, SQL_TYPE_DOUBLE, SQL_TYPE_TIMESTAMP, SQL_TYPE_QUAD, SQL_TYPE_DEC64):
        return (8, False)
    elif meta.type_id in (SQL_TYPE_INT128, SQL_TYPE_DEC128):
        return (16, False)
    elif meta.type_id == SQL_TYPE_VARYING:
        return (meta.field_bytes, True)
    elif meta.type_id == SQL_TYPE_TEXT:
        return (round_up_4(meta.field_bytes), False)
    else:
        return (4, False)


def put_slice_wire_element_size(meta: ArrayMeta) -> int:
    if meta.type_id == SQL_TYPE_TEXT:
        return round_up_4(meta.field_bytes)
    size, varying = slice_wire_element_size(meta)
    if varying:
        return 4
    return size


def pad_slice_wire(meta: ArrayMeta, data: bytes, actual_count: int) -> bytes:
    declared = all_elements_count(meta.dimensions)
    missing = declared - actual_count
    if missing <= 0:
        return data
    per = put_slice_wire_element_size(meta)
    return data + (b'\x00' * (missing * per))


def to_scaled_int(e: Any, scale: int) -> int:
    if scale == 0:
        if isinstance(e, (int, str)):
            return int(e)
        elif isinstance(e, decimal.Decimal):
            return int(e)
        elif isinstance(e, float):
            return int(round(e))
        else:
            return int(e)
    if isinstance(e, str):
        d = decimal.Decimal(e)
    elif isinstance(e, float):
        d = decimal.Decimal(str(e))
    elif isinstance(e, decimal.Decimal):
        d = e
    elif isinstance(e, int):
        d = decimal.Decimal(e)
    else:
        d = decimal.Decimal(str(e))
    multiplier = decimal.Decimal(10) ** (-scale)
    return int((d * multiplier).to_integral_value(rounding=decimal.ROUND_HALF_UP))


def encode_array_element(meta: ArrayMeta, e: Any, charset: str) -> bytes:
    if e is None:
        raise OperationalError("cannot encode NULL array element")
    type_id = meta.type_id
    scale = meta.scale

    if type_id == SQL_TYPE_SHORT:
        v = to_scaled_int(e, scale)
        if v < -32768 or v > 32767:
            raise OperationalError(f"value {v} out of SMALLINT range")
        return struct.pack('!i', v)
    elif type_id == SQL_TYPE_LONG:
        v = to_scaled_int(e, scale)
        if v < -2147483648 or v > 2147483647:
            raise OperationalError(f"value {v} out of INTEGER range")
        return struct.pack('!i', v)
    elif type_id in (SQL_TYPE_INT64, SQL_TYPE_QUAD):
        v = to_scaled_int(e, scale)
        return struct.pack('!q', v)
    elif type_id == SQL_TYPE_FLOAT:
        return struct.pack('!f', float(e))
    elif type_id == SQL_TYPE_DOUBLE:
        return struct.pack('!d', float(e))
    elif type_id == SQL_TYPE_BOOLEAN:
        if not isinstance(e, bool):
            raise OperationalError(f"cannot encode {type(e).__name__} as BOOLEAN element")
        return b'\x01\x00\x00\x00' if e else b'\x00\x00\x00\x00'
    elif type_id == SQL_TYPE_TEXT:
        if not isinstance(e, str):
            e = str(e)
        codec = charset_map.get(charset, charset) or 'utf-8'
        enc = e.encode(codec)
        if (meta.length and len(e) > meta.length) or len(enc) > meta.field_bytes:
            raise OperationalError(f"value {e!r} too long for CHAR({meta.length}) array element")
        padded = enc + b' ' * (round_up_4(meta.field_bytes) - len(enc))
        return padded
    elif type_id == SQL_TYPE_VARYING:
        if not isinstance(e, str):
            e = str(e)
        codec = charset_map.get(charset, charset) or 'utf-8'
        enc = e.encode(codec)
        if (meta.length and len(e) > meta.length) or len(enc) > meta.field_bytes:
            raise OperationalError(f"value {e!r} too long for VARCHAR({meta.length}) array element")
        pad_len = (4 - (len(enc) % 4)) % 4
        return struct.pack('!i', len(enc)) + enc + (b'\x00' * pad_len)
    elif type_id == SQL_TYPE_INT128:
        v = to_scaled_int(e, scale)
        return bint_to_bytes(v, 16)
    elif type_id == SQL_TYPE_DATE:
        from firebirdsql.wireprotocol import convert_date
        if not isinstance(e, datetime.date):
            raise OperationalError(f"cannot encode {type(e).__name__} as DATE element")
        return convert_date(e)
    elif type_id == SQL_TYPE_TIME:
        from firebirdsql.wireprotocol import convert_time
        if not isinstance(e, datetime.time):
            raise OperationalError(f"cannot encode {type(e).__name__} as TIME element")
        return convert_time(e)
    elif type_id == SQL_TYPE_TIMESTAMP:
        from firebirdsql.wireprotocol import convert_timestamp
        if not isinstance(e, datetime.datetime):
            raise OperationalError(f"cannot encode {type(e).__name__} as TIMESTAMP element")
        return convert_timestamp(e)
    elif type_id in (SQL_TYPE_TIME_TZ, SQL_TYPE_TIMESTAMP_TZ):
        raise OperationalError("array elements of type TIME/TIMESTAMP WITH TIME ZONE are not supported yet")
    else:
        raise OperationalError(f"unsupported array element type {type_id}")


def encode_slice_elements(meta: ArrayMeta, elements: list[Any], charset: str) -> bytes:
    declared = all_elements_count(meta.dimensions)
    if declared > 0 and len(elements) > declared:
        raise OperationalError(f"array argument has {len(elements)} elements but the column holds {declared}")
    buf = bytearray()
    for e in elements:
        buf.extend(encode_array_element(meta, e, charset))
    return bytes(buf)


def slice_element_count(meta: ArrayMeta, data: bytes) -> int:
    elem_len, varying = slice_wire_element_size(meta)
    if varying:
        count = 0
        pos = 0
        while pos < len(data):
            if len(data) - pos < 4:
                raise OperationalError("array varying element length prefix truncated")
            l = struct.unpack('!i', data[pos:pos+4])[0]
            pos += 4 + l
            count += 1
        return count
    if elem_len < 1:
        raise OperationalError("array element length must not be less than 1")
    return len(data) // elem_len


def decode_slice_elements(meta: ArrayMeta, data: bytes, bytes_to_str: Any) -> list[Any]:
    if not data:
        return []
    if meta.type_id in (SQL_TYPE_TIME_TZ, SQL_TYPE_TIMESTAMP_TZ):
        raise OperationalError("array elements of type TIME/TIMESTAMP WITH TIME ZONE are not supported yet")

    count = slice_element_count(meta, data)
    elem_len, varying = slice_wire_element_size(meta)

    dummy_xvar = XSQLVAR(bytes_to_str)
    dummy_xvar.sqltype = meta.type_id
    dummy_xvar.sqlscale = meta.scale
    dummy_xvar.sqlsubtype = meta.sub_type
    dummy_xvar.sqllen = meta.length

    out = []
    pos = 0
    for i in range(count):
        if varying:
            if len(data) - pos < 4:
                raise OperationalError(f"array body truncated at element {i}")
            l = struct.unpack('!i', data[pos:pos+4])[0]
            pos += 4
            if l < 0 or pos + l > len(data):
                raise OperationalError(f"array element {i} length {l} exceeds remaining bytes")
            raw = data[pos:pos+l]
            pos += l
        else:
            if pos + elem_len > len(data):
                raise OperationalError(f"array body truncated at element {i}")
            raw = data[pos:pos+elem_len]
            pos += elem_len
            if meta.type_id == SQL_TYPE_BOOLEAN:
                raw = raw[:1]
        v = dummy_xvar.value(raw)
        if meta.type_id == SQL_TYPE_TEXT:
            if isinstance(v, str):
                v = v.rstrip('\x00').rstrip(' ')
        out.append(v)
    return format_array_output(meta, out)


def shrink_dimensions(declared: list[ArrayDimension], actual: int) -> list[ArrayDimension]:
    decl_count = all_elements_count(declared)
    missing = decl_count - actual
    if missing < 0:
        raise OperationalError(f"array holds {actual} elements but the column declares {decl_count}")
    out = []
    for i in range(len(declared)):
        tail = elements_in_trailing_axes(declared[i:])
        if tail <= 0:
            raise OperationalError(f"array dimension {i} is not positive")
        out.append(ArrayDimension(
            lower_bound=declared[i].lower_bound,
            length=declared[i].length - (missing // tail),
        ))
        missing %= tail
    return out


def elements_in_trailing_axes(dims: list[ArrayDimension]) -> int:
    total = 1
    for i in range(1, len(dims)):
        total *= dims[i].length
    return total


class IndexRange:
    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, IndexRange):
            return self.start == other.start and self.end == other.end
        return False

    def __repr__(self) -> str:
        return f"IndexRange({self.start}, {self.end})"


def present_index_ranges(declared: list[ArrayDimension], actual: list[ArrayDimension]) -> list[IndexRange]:
    out: list[IndexRange] = []
    _walk_present_ranges(out, declared, actual, 0)
    return out


def _walk_present_ranges(out: list[IndexRange], declared: list[ArrayDimension], actual: list[ArrayDimension], pos: int) -> int:
    if len(declared) > 1:
        for i in range(actual[0].length):
            pos = _walk_present_ranges(out, declared[1:], actual[1:], pos)
        pos += elements_in_trailing_axes(declared) * (declared[0].length - actual[0].length)
    elif len(declared) == 1:
        out.append(IndexRange(pos, pos + actual[0].length - 1))
        pos += declared[0].length
    return pos


def format_array_output(meta: ArrayMeta, elements: list[Any]) -> list[Any]:
    if not meta or not meta.dimensions:
        return elements
    dims = meta.dimensions
    if len(dims) <= 1:
        return elements
    # Multi-dimensional: reconstruct nested lists
    res = list(elements)
    for d in reversed(dims[1:]):
        res = [res[i:i + d.length] for i in range(0, len(res), d.length)]
    return res


def is_ragged(seq: Any) -> bool:
    if not isinstance(seq, (list, tuple)) or not seq:
        return False
    first_is_seq = isinstance(seq[0], (list, tuple))
    for elem in seq:
        if isinstance(elem, (list, tuple)) != first_is_seq:
            return True
        if first_is_seq:
            if len(elem) != len(seq[0]) or is_ragged(elem):
                return True
    return False


def extract_array_elements(val: Any) -> list[Any] | None:
    if isinstance(val, Array):
        val = val.elements
    if val is None:
        return None
    if is_ragged(val):
        raise OperationalError("cannot encode a ragged multi-dimensional array")
    if isinstance(val, (list, tuple)):
        flat: list[Any] = []

        def _flatten(item: Any) -> None:
            if isinstance(item, (list, tuple)):
                for sub in item:
                    _flatten(sub)
            else:
                if item is None:
                    raise OperationalError("cannot encode NULL array element")
                flat.append(item)

        _flatten(val)
        return flat
    raise OperationalError(f"unsupported array argument type {type(val)}")


def quote_meta_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _internal_query(connection: Any, query: str) -> list[tuple[Any, ...]]:
    from firebirdsql.fbcore import Statement, Transaction
    if connection._transaction is None:
        connection._transaction = Transaction(connection, connection._autocommit)
    connection._transaction.check_trans_handle()
    stmt = Statement(connection._transaction)
    try:
        stmt.prepare(query)
        connection._op_execute(stmt.handle, connection._transaction.trans_handle, [])
        (h, oid, buf) = connection._op_response()
        fetch_count = 100
        connection._op_fetch(stmt.handle, calc_blr(stmt.xsqlda), fetch_count)
        (rows, more_data) = connection._op_fetch_response(stmt.handle, stmt.xsqlda)
        all_rows = []
        for r in stmt.fetch_generator(rows, more_data, fetch_count):
            all_rows.append(r)
        return all_rows
    finally:
        stmt.close()
        stmt.drop()


def query_array_meta(connection: Any, pairs: list[tuple[str, str]]) -> tuple[dict[tuple[str, str], ArrayMeta], dict[str, list[ArrayDimension]]]:
    tables = list({quote_meta_literal(p[0]) for p in pairs})
    fields = list({quote_meta_literal(p[1]) for p in pairs})

    query = (
        "SELECT Y.RDB$FIELD_TYPE, Y.RDB$FIELD_SCALE, Y.RDB$FIELD_LENGTH, "
        "CASE WHEN Y.RDB$FIELD_TYPE IN (14, 37) "
        "THEN Y.RDB$CHARACTER_SET_ID ELSE Y.RDB$FIELD_SUB_TYPE END, "
        "CASE WHEN Y.RDB$FIELD_TYPE IN (14, 37) "
        "THEN Y.RDB$CHARACTER_LENGTH ELSE Y.RDB$FIELD_LENGTH END, "
        "X.RDB$FIELD_SOURCE, X.RDB$RELATION_NAME, X.RDB$FIELD_NAME, Y.RDB$FIELD_LENGTH "
        "FROM RDB$RELATION_FIELDS X, RDB$FIELDS Y "
        "WHERE X.RDB$FIELD_SOURCE = Y.RDB$FIELD_NAME "
        f"AND X.RDB$RELATION_NAME IN ({','.join(tables)}) "
        f"AND X.RDB$FIELD_NAME IN ({','.join(fields)})"
    )

    rows = _internal_query(connection, query)
    metas: dict[tuple[str, str], ArrayMeta] = {}
    dimensions: dict[str, list[ArrayDimension]] = {}
    sources: list[str] = []

    for row in rows:
        blr = row[0]
        scale = row[1]
        field_bytes = row[2]
        sub = row[3] if row[3] is not None else 0
        length = field_bytes
        if row[4] is not None and row[4] > 0:
            length = row[4]
        source = row[5].strip() if row[5] else ''
        rel = row[6].strip() if row[6] else ''
        field = row[7].strip() if row[7] else ''

        type_id = array_type_from_blr(blr)
        am = ArrayMeta(
            table_name=rel,
            field_name=field,
            type_id=type_id,
            blr_type_id=blr,
            scale=scale,
            length=length,
            field_bytes=field_bytes,
            sub_type=sub,
            field_source=source,
        )
        metas[(rel, field)] = am
        metas[(rel.upper(), field.upper())] = am
        if source and source not in dimensions:
            sources.append(source)
            dimensions[source] = []

    if sources:
        quoted = [quote_meta_literal(s) for s in sources]
        dim_query = (
            "SELECT X.RDB$LOWER_BOUND, X.RDB$UPPER_BOUND, X.RDB$FIELD_NAME "
            "FROM RDB$FIELD_DIMENSIONS X "
            f"WHERE X.RDB$FIELD_NAME IN ({','.join(quoted)}) "
            "ORDER BY X.RDB$FIELD_NAME, X.RDB$DIMENSION"
        )
        dim_rows = _internal_query(connection, dim_query)
        for row in dim_rows:
            lower = row[0]
            upper = row[1]
            source = row[2].strip() if row[2] else ''
            if upper < lower:
                raise OperationalError(f"array field {source} has inverted bounds [{lower}, {upper}]")
            dimensions[source].append(ArrayDimension(lower, upper - lower + 1))

    return metas, dimensions


def resolve_array_meta(connection: Any, xsqlda: list[Any] | None) -> None:
    if not xsqlda or getattr(connection, '_resolving_array_meta', False):
        return
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for x in xsqlda:
        if x and x.sqltype == SQL_TYPE_ARRAY and x.relname and x.fieldname:
            k = (x.relname.strip(), x.fieldname.strip())
            if k not in seen:
                seen.add(k)
                pairs.append(k)
    if not pairs:
        return

    connection._resolving_array_meta = True
    try:
        metas, dimensions = query_array_meta(connection, pairs)
        for am in metas.values():
            am.dimensions = dimensions.get(am.field_source, [])
        for x in xsqlda:
            if x and x.sqltype == SQL_TYPE_ARRAY and x.relname and x.fieldname:
                k = (x.relname.strip(), x.fieldname.strip())
                x.array_meta = metas.get(k) or metas.get((k[0].upper(), k[1].upper()))
    finally:
        connection._resolving_array_meta = False
