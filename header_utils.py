# header_utils.py

# Column type IDs — must match serializers
TYPE_INT32 = 0
TYPE_FLOAT64 = 1
TYPE_STRING = 2

"""
Header builder & parser for the CSTM format.

Functions:
- build_header(schema_signature, total_rows, column_entries) -> bytes
- parse_header(header_bytes) -> (schema_signature, total_rows, columns_list)

`column_entries` is a list of dicts with keys:
  - name (str)
  - type (int)
  - flags (int)
  - num_values (int)
  - offset (int)           # placeholder 0 when building initial header before writing blocks
  - comp_size (int)        # placeholder 0
  - uncomp_size (int)      # placeholder 0
"""

import io
import zlib     # used only if you want to compute CRC32 of schema string
from typing import List, Tuple, Dict

from byte_utils import (
    pack_u32, pack_u64, pack_u16, pack_u8,
    unpack_u32, unpack_u64, unpack_u16, unpack_u8,
    SIZE_U32, SIZE_U64, SIZE_U16, SIZE_U8
)


def build_header(schema_signature: int, total_rows: int, column_entries: List[Dict]) -> bytes:
    """
    Build header bytes deterministically.

    - schema_signature: uint32 (often a CRC32 of schema JSON) or 0
    - total_rows: uint64
    - column_entries: list of dicts with keys:
        name (str), type (int), flags (int),
        num_values (int), offset (int), comp_size (int), uncomp_size (int)
    """
    buf = io.BytesIO()

    # schema_signature (u32)
    buf.write(pack_u32(int(schema_signature) & 0xFFFFFFFF))
    # total_rows (u64)
    buf.write(pack_u64(int(total_rows)))
    # num_columns (u32)
    num_columns = len(column_entries)
    buf.write(pack_u32(num_columns))

    # per-column entries
    for entry in column_entries:
        name = entry['name']
        if not isinstance(name, bytes):
            name_b = name.encode('utf-8')
        else:
            name_b = name
        name_len = len(name_b)
        if name_len >= (1 << 16):
            raise ValueError("Column name too long (>65535 bytes)")

        # name_length u16 + name bytes
        buf.write(pack_u16(name_len))
        buf.write(name_b)

        # type_id u8, flags u8
        buf.write(pack_u8(int(entry.get('type', 0))))
        buf.write(pack_u8(int(entry.get('flags', 0))))

        # num_values (u64)
        buf.write(pack_u64(int(entry.get('num_values', 0))))

        # block_offset (u64)
        buf.write(pack_u64(int(entry.get('offset', 0))))
        # compressed_size (u64)
        buf.write(pack_u64(int(entry.get('comp_size', 0))))
        # uncompressed_size (u64)
        buf.write(pack_u64(int(entry.get('uncomp_size', 0))))

    return buf.getvalue()


def parse_header(header_bytes: bytes) -> Tuple[int, int, List[Dict]]:
    """
    Parse header bytes produced by build_header.

    Returns:
      (schema_signature (int), total_rows (int), columns_list (list of dicts))
    Each column dict contains keys:
      name (str), type (int), flags (int), num_values (int),
      offset (int), comp_size (int), uncomp_size (int)
    """
    buf = io.BytesIO(header_bytes)

    # schema_signature (u32)
    raw = buf.read(SIZE_U32)
    if len(raw) < SIZE_U32:
        raise ValueError("Header too short when reading schema_signature")
    schema_sig = unpack_u32(raw)

    # total_rows (u64)
    raw = buf.read(SIZE_U64)
    if len(raw) < SIZE_U64:
        raise ValueError("Header too short when reading total_rows")
    total_rows = unpack_u64(raw)

    # num_columns (u32)
    raw = buf.read(SIZE_U32)
    if len(raw) < SIZE_U32:
        raise ValueError("Header too short when reading num_columns")
    num_columns = unpack_u32(raw)

    columns = []
    for i in range(num_columns):
        # name length (u16)
        raw = buf.read(SIZE_U16)
        if len(raw) < SIZE_U16:
            raise ValueError(f"Header truncated reading name_length for column {i}")
        name_len = unpack_u16(raw)

        # name bytes
        name_b = buf.read(name_len)
        if len(name_b) < name_len:
            raise ValueError(f"Header truncated reading name bytes for column {i}")
        name = name_b.decode('utf-8')

        # type u8
        raw = buf.read(SIZE_U8)
        if len(raw) < SIZE_U8:
            raise ValueError(f"Header truncated reading type for column {name}")
        type_id = unpack_u8(raw)

        # flags u8
        raw = buf.read(SIZE_U8)
        if len(raw) < SIZE_U8:
            raise ValueError(f"Header truncated reading flags for column {name}")
        flags = unpack_u8(raw)

        # num_values u64
        raw = buf.read(SIZE_U64)
        if len(raw) < SIZE_U64:
            raise ValueError(f"Header truncated reading num_values for column {name}")
        num_values = unpack_u64(raw)

        # offset u64
        raw = buf.read(SIZE_U64)
        if len(raw) < SIZE_U64:
            raise ValueError(f"Header truncated reading offset for column {name}")
        offset = unpack_u64(raw)

        # comp_size u64
        raw = buf.read(SIZE_U64)
        if len(raw) < SIZE_U64:
            raise ValueError(f"Header truncated reading comp_size for column {name}")
        comp_size = unpack_u64(raw)

        # uncomp_size u64
        raw = buf.read(SIZE_U64)
        if len(raw) < SIZE_U64:
            raise ValueError(f"Header truncated reading uncomp_size for column {name}")
        uncomp_size = unpack_u64(raw)

        columns.append({
            'name': name,
            'type': type_id,
            'flags': flags,
            'num_values': num_values,
            'offset': offset,
            'comp_size': comp_size,
            'uncomp_size': uncomp_size
        })

    return schema_sig, total_rows, columns


# -------------------
# Self-tests and examples
# -------------------
if __name__ == '__main__':
    # Basic test: build -> parse roundtrip
    sample_columns = [
        {'name': 'id',   'type': 0, 'flags': 0, 'num_values': 3, 'offset': 0, 'comp_size': 0, 'uncomp_size': 0},
        {'name': 'score','type': 1, 'flags': 0, 'num_values': 3, 'offset': 0, 'comp_size': 0, 'uncomp_size': 0},
        {'name': 'name', 'type': 2, 'flags': 0, 'num_values': 3, 'offset': 0, 'comp_size': 0, 'uncomp_size': 0},
    ]
    header1 = build_header(schema_signature=0xDEADBEEF, total_rows=3, column_entries=sample_columns)
    print("Header length:", len(header1))

    sig, total_rows, cols = parse_header(header1)
    assert sig == (0xDEADBEEF & 0xFFFFFFFF)
    assert total_rows == 3
    assert len(cols) == 3
    assert cols[0]['name'] == 'id' and cols[0]['type'] == 0 and cols[0]['num_values'] == 3
    assert cols[2]['name'] == 'name' and cols[2]['type'] == 2

    # Simulate writing blocks: set offsets/sizes
    # We'll ensure header length remains identical after updating numeric fields.
    # Note: header length should be identical because only fixed-size numeric fields changed.
    sample_columns_updated = []
    base_offset = 1000
    for i, c in enumerate(sample_columns):
        c2 = c.copy()
        c2['offset'] = base_offset + i * 200
        c2['comp_size'] = 150
        c2['uncomp_size'] = 400
        sample_columns_updated.append(c2)

    header2 = build_header(schema_signature=0xDEADBEEF, total_rows=3, column_entries=sample_columns_updated)
    print("Header length after update:", len(header2))
    if len(header1) != len(header2):
        raise SystemExit("Header length changed between initial and final serialization! This breaks placeholder-rewrite flow.")
    else:
        print("Header size stable — ok to use placeholder header then overwrite with final header.")

    # parse again to confirm values present
    sig2, total_rows2, cols2 = parse_header(header2)
    assert cols2[0]['offset'] == base_offset
    assert cols2[1]['comp_size'] == 150
    assert cols2[2]['uncomp_size'] == 400

    print("header_utils.py self-tests passed")
