# byte_utils.py
"""
Binary packing/unpacking helpers for the CSTM columnar format.

Endianness: all multi-byte values are **little-endian** (struct format prefix '<').

This module centralizes:
- format constants (magic, version, type IDs)
- pack/unpack helpers for primitive types
- safe file read helpers (read_exact)
"""

from typing import BinaryIO
import struct

# ---- Format constants ----
MAGIC = b'CSTM'         # 4 bytes
VERSION = 1             # uint8
# Column type IDs used in the header
TYPE_INT32   = 0
TYPE_FLOAT64 = 1
TYPE_STRING  = 2

# Optional: useful offsets / header base
HEADER_PREFIX_LEN = 0x14  # 20 bytes where header begins in our spec

# ---- Struct helpers (little-endian) ----
def pack_u8(x: int) -> bytes:
    return struct.pack('<B', x)

def unpack_u8(b: bytes) -> int:
    return struct.unpack('<B', b)[0]

def pack_u16(x: int) -> bytes:
    return struct.pack('<H', x)

def unpack_u16(b: bytes) -> int:
    return struct.unpack('<H', b)[0]

def pack_u32(x: int) -> bytes:
    return struct.pack('<I', x)

def unpack_u32(b: bytes) -> int:
    return struct.unpack('<I', b)[0]

def pack_u64(x: int) -> bytes:
    return struct.pack('<Q', x)

def unpack_u64(b: bytes) -> int:
    return struct.unpack('<Q', b)[0]

def pack_i32(x: int) -> bytes:
    return struct.pack('<i', x)

def unpack_i32(b: bytes) -> int:
    return struct.unpack('<i', b)[0]

def pack_f64(x: float) -> bytes:
    return struct.pack('<d', x)

def unpack_f64(b: bytes) -> float:
    return struct.unpack('<d', b)[0]

# ---- Convenience / IO helpers ----
def read_exact(f: BinaryIO, n: int) -> bytes:
    """
    Read exactly n bytes from file-like object f.
    Raises EOFError if fewer than n bytes available.
    """
    data = f.read(n)
    if len(data) != n:
        raise EOFError(f"Expected {n} bytes, got {len(data)} bytes")
    return data

def pack_length_prefixed_bytes(b: bytes) -> bytes:
    """
    Prefix a bytes object with a u32 length (little-endian).
    Useful for writing length-prefixed strings or blobs.
    """
    return pack_u32(len(b)) + b

def unpack_length_prefixed_bytes(b: bytes) -> (int, bytes):
    """
    Given a bytes buffer starting with u32 length, return (length, payload_bytes).
    Does not check that payload length matches; caller must validate.
    """
    if len(b) < 4:
        raise ValueError("Buffer too small for length prefix")
    L = unpack_u32(b[:4])
    payload = b[4:4+L]
    return L, payload

# ---- Utility: size constants (for reading) ----
SIZE_U8 = struct.calcsize('<B')
SIZE_U16 = struct.calcsize('<H')
SIZE_U32 = struct.calcsize('<I')
SIZE_U64 = struct.calcsize('<Q')
SIZE_I32 = struct.calcsize('<i')
SIZE_F64 = struct.calcsize('<d')

# ---- Small self-tests when run as a script ----
if __name__ == '__main__':
    # Quick round-trip tests
    assert pack_u32(0x12345678) == b'\x78\x56\x34\x12'
    assert unpack_u32(pack_u32(123456789)) == 123456789
    assert unpack_i32(pack_i32(-42)) == -42
    assert abs(unpack_f64(pack_f64(3.14159)) - 3.14159) < 1e-12

    # read_exact using BytesIO
    import io
    bio = io.BytesIO(b'hello')
    assert read_exact(bio, 5) == b'hello'
    try:
        bio2 = io.BytesIO(b'abc')
        read_exact(bio2, 4)
        raise SystemExit("read_exact did not raise EOFError")
    except EOFError:
        pass

    # length-prefixed
    p = pack_length_prefixed_bytes(b'abc')
    L, payload = unpack_length_prefixed_bytes(p)
    assert L == 3 and payload == b'abc'

    print("byte_utils.py self-tests passed")
