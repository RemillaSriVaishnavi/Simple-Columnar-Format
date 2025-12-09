# writer.py
import os
import io
import zlib
import struct
from typing import List

from byte_utils import (
    MAGIC, VERSION, pack_u64, HEADER_PREFIX_LEN
)
from header_utils import build_header
from column_serializers import (
    serialize_int32_column,
    serialize_float64_column,
    serialize_string_column
)

# map type id to serializer function
_SERIALIZERS = {
    0: serialize_int32_column,
    1: serialize_float64_column,
    2: serialize_string_column
}

def write_cstm(out_path: str,
               column_names: List[str],
               column_types: List[int],
               columns: List[List],
               schema_signature: int = 0) -> None:
    """
    Write table data to a .cstm file.

    - column_names: list of N column names
    - column_types: list of N type ids (0=int32,1=float64,2=string)
    - columns: list of N lists, each list has total_rows elements (column-major)
    - schema_signature: optional uint32 (CRC32 of schema JSON)

    Raises ValueError on validation failures.
    """
    # --- Validation ---
    if not (len(column_names) == len(column_types) == len(columns)):
        raise ValueError("column_names, column_types and columns must have same length")
    num_columns = len(column_names)
    total_rows = 0
    if num_columns > 0:
        total_rows = len(columns[0])
        for i, col in enumerate(columns):
            if len(col) != total_rows:
                raise ValueError(f"Column {i} length {len(col)} != expected {total_rows}")

    # --- Build column_entries placeholders ---
    column_entries = []
    for name, t, col in zip(column_names, column_types, columns):
        entry = {
            'name': name,
            'type': int(t),
            'flags': 0,
            'num_values': len(col),
            'offset': 0,
            'comp_size': 0,
            'uncomp_size': 0
        }
        column_entries.append(entry)

    # --- Build initial header bytes (with placeholder offsets/sizes) ---
    initial_header = build_header(schema_signature, total_rows, column_entries)
    header_len = len(initial_header)

    # File layout: MAGIC(4) + VERSION(1) + reserved(7) + header_len(u64) + header_bytes
    # header begins at offset HEADER_PREFIX_LEN (0x14 = 20) in our spec
    # Sanity: HEADER_PREFIX_LEN should be 20
    # Write file and column blocks
    with open(out_path, 'wb') as f:
        # write prefix
        f.write(MAGIC)
        f.write(struct.pack('<B', VERSION))
        f.write(b'\x00' * 7)          # reserved
        f.write(pack_u64(header_len)) # header length (u64 little-endian)
        f.write(initial_header)       # placeholder header bytes

        # Write each column block compressed and update entries
        for idx, entry in enumerate(column_entries):
            col_type = entry['type']
            serializer = _SERIALIZERS.get(col_type)
            if serializer is None:
                raise ValueError(f"Unknown column type id {col_type} for column {entry['name']}")

            # Serialize uncompressed bytes for this column
            uncompressed = serializer(columns[idx])
            uncomp_size = len(uncompressed)

            # Compress using zlib
            compressed = zlib.compress(uncompressed)
            comp_size = len(compressed)

            # record block offset (absolute)
            block_offset = f.tell()

            # write compressed bytes
            f.write(compressed)

            # update entry in-place
            entry['offset'] = block_offset
            entry['comp_size'] = comp_size
            entry['uncomp_size'] = uncomp_size

        # After writing all column blocks, build final header and overwrite header area
        final_header = build_header(schema_signature, total_rows, column_entries)
        if len(final_header) != header_len:
            # Simple approach: fail early — header length must be deterministic for placeholder rewrite flow.
            raise RuntimeError(
                "Header length changed between initial and final serialization. "
                "Ensure schema (column names, types, and num_values) is fixed before writing blocks."
            )

        # Seek to header start and overwrite
        f.seek(HEADER_PREFIX_LEN)  # 0x14
        f.write(final_header)
        f.flush()
    # file closed
    return None
