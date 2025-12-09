# column_serializers.py
"""
Column serializers and parsers for CSTM format.

Provides:
- serialize_int32_column(values) -> bytes
- serialize_float64_column(values) -> bytes
- serialize_string_column(values) -> bytes

And the inverse:
- parse_int32_block(data, num_values) -> list[int]
- parse_float64_block(data, num_values) -> list[float]
- parse_string_block(data, num_values) -> list[str]

Edge cases:
- Empty column -> valid representation
- Strings use 32-bit offsets (max 4 GiB). If exceeded, raise ValueError.
"""

import io
import struct
from typing import List

from byte_utils import (
    pack_i32, unpack_i32, SIZE_I32,
    pack_f64, unpack_f64, SIZE_F64,
    pack_u32, unpack_u32, SIZE_U32
)

# Integer limits for signed 32-bit
I32_MIN = -2**31
I32_MAX = 2**31 - 1
UINT32_MAX = 0xFFFFFFFF

# ---- Serializers ----
def serialize_int32_column(values: List[int]) -> bytes:
    """
    Pack each value as little-endian signed 32-bit (<i).
    Raises ValueError if any value cannot be converted to int or overflows int32.
    """
    buf = io.BytesIO()
    for v in values:
        if v is None:
            # We don't implement nulls in this simple version; decide policy:
            # treat None as 0 or raise. We'll raise for safety.
            raise ValueError("Null values not supported for INT32 in this implementation.")
        iv = int(v)
        if iv < I32_MIN or iv > I32_MAX:
            raise ValueError(f"Value {iv} out of int32 range")
        buf.write(pack_i32(iv))
    return buf.getvalue()

def serialize_float64_column(values: List[float]) -> bytes:
    """
    Pack each value as little-endian IEEE 754 double (<d).
    """
    buf = io.BytesIO()
    for v in values:
        if v is None:
            # similar policy to ints: no null support here
            raise ValueError("Null values not supported for FLOAT64 in this implementation.")
        fv = float(v)
        buf.write(pack_f64(fv))
    return buf.getvalue()

def serialize_string_column(values: List[str]) -> bytes:
    """
    Build offsets array (num_values+1 uint32 little-endian) followed by concatenated UTF-8 bytes.
    Returns offsets_bytes + data_bytes.

    If values is empty list -> offsets array with single 0 and empty data.
    """
    num_values = len(values)
    offsets = []
    data_parts = []
    cur = 0
    offsets.append(cur)  # O[0] = 0
    for v in values:
        if v is None:
            # For now treat None as empty string. If you want to implement NULLs,
            # use a sentinel in offsets (e.g., 0xFFFFFFFF) and handle in parser.
            b = b''
        else:
            if isinstance(v, str):
                b = v.encode('utf-8')
            else:
                b = str(v).encode('utf-8')
        data_parts.append(b)
        cur += len(b)
        offsets.append(cur)

    # Validate total data length fits in 32-bit
    total_data_len = cur
    if total_data_len > UINT32_MAX:
        raise ValueError("Total string data exceeds 4GiB; 32-bit offsets cannot represent this.")

    # Build offsets bytes
    buf = io.BytesIO()
    # offsets: num_values+1 uint32 entries
    for off in offsets:
        buf.write(pack_u32(off))

    # Then write data bytes concatenated
    for part in data_parts:
        buf.write(part)

    return buf.getvalue()

# ---- Parsers ----
def parse_int32_block(data: bytes, num_values: int) -> List[int]:
    """
    Parse a block of bytes into a list of int32 values.
    """
    expected = num_values * SIZE_I32
    if len(data) != expected:
        # allow case where data length matches or we may get trailing bytes in some contexts;
        # caller must ensure uncompressed_size matches expected. We'll still validate strictly.
        raise ValueError(f"INT32 block size mismatch: expected {expected} bytes, got {len(data)}")
    out = []
    # Use struct.unpack for speed over looping pack_i32/unpack
    # Build format string safely: '<' + 'i'*num_values may be big; but acceptable for moderate sizes.
    # We'll iterate to avoid huge format strings.
    off = 0
    for _ in range(num_values):
        chunk = data[off:off+SIZE_I32]
        val = unpack_i32(chunk)
        out.append(val)
        off += SIZE_I32
    return out

def parse_float64_block(data: bytes, num_values: int) -> List[float]:
    expected = num_values * SIZE_F64
    if len(data) != expected:
        raise ValueError(f"FLOAT64 block size mismatch: expected {expected} bytes, got {len(data)}")
    out = []
    off = 0
    for _ in range(num_values):
        chunk = data[off:off+SIZE_F64]
        val = unpack_f64(chunk)
        out.append(val)
        off += SIZE_F64
    return out

def parse_string_block(data: bytes, num_values: int) -> List[str]:
    """
    Parse offsets array then data bytes.
    Offsets array is (num_values+1) uint32 entries (little-endian).
    Data section immediately follows offsets.
    """
    offsets_byte_len = (num_values + 1) * SIZE_U32
    if len(data) < offsets_byte_len:
        raise ValueError(f"STRING block too small for offsets: need {offsets_byte_len} bytes, got {len(data)}")
    # Read offsets
    offsets = []
    off = 0
    for _ in range(num_values + 1):
        chunk = data[off:off+SIZE_U32]
        offsets.append(unpack_u32(chunk))
        off += SIZE_U32

    data_section = data[offsets_byte_len:]
    # Validate offsets within data_section length
    if offsets[-1] > len(data_section):
        raise ValueError(f"Last offset {offsets[-1]} exceeds data section length {len(data_section)}")

    out = []
    for i in range(num_values):
        s_off = offsets[i]
        s_end = offsets[i+1]
        if s_off > s_end:
            raise ValueError(f"Invalid offsets: offsets[{i}] > offsets[{i+1}]")
        raw = data_section[s_off:s_end]
        out.append(raw.decode('utf-8'))
    return out

# ---- Self-tests ----
if __name__ == '__main__':
    # Ints
    ints = [1, -2, 0, 2147483647, -2147483648]
    bytes_int = serialize_int32_column(ints)
    parsed_ints = parse_int32_block(bytes_int, len(ints))
    assert parsed_ints == ints

    # Floats
    floats = [0.0, -3.5, 1.23456789e10]
    bytes_f = serialize_float64_column(floats)
    parsed_floats = parse_float64_block(bytes_f, len(floats))
    # floats equality check
    for a, b in zip(floats, parsed_floats):
        assert abs(a - b) < 1e-12

    # Strings: simple and unicode
    strs = ["", "hello", "こんにちは", "a,comma", "line\nbreak"]
    bytes_s = serialize_string_column(strs)
    parsed_strs = parse_string_block(bytes_s, len(strs))
    assert parsed_strs == strs

    # Empty column tests
    empty_ints = []
    bytes_empty_ints = serialize_int32_column(empty_ints)
    assert bytes_empty_ints == b''  # zero-length block
    parsed_empty_ints = parse_int32_block(bytes_empty_ints, 0)
    assert parsed_empty_ints == []

    empty_strs = []
    bytes_empty_strs = serialize_string_column(empty_strs)
    # offsets array should be single u32 zero (4 bytes)
    assert bytes_empty_strs == pack_u32(0)
    parsed_empty_strs = parse_string_block(bytes_empty_strs, 0)
    assert parsed_empty_strs == []

    print("column_serializers.py self-tests passed")
