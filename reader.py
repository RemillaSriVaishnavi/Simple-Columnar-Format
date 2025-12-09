# reader.py
import struct
import zlib
from typing import List, Optional, Tuple

from byte_utils import MAGIC, HEADER_PREFIX_LEN, read_exact, unpack_u64
from header_utils import parse_header
from column_serializers import (
    parse_int32_block,
    parse_float64_block,
    parse_string_block
)

# Map type id -> parser
_PARSERS = {
    0: parse_int32_block,
    1: parse_float64_block,
    2: parse_string_block
}


def read_cstm(path: str, select_columns: Optional[List[str]] = None) -> Tuple[List[str], List[List[str]]]:
    """
    Read a .cstm file and return (names, rows).

    - select_columns: list of column names to read. If None, read all columns.
    - Returned rows are lists of string-ish values (int/float converted to str).
      If you want raw typed values, modify parse_* functions to return numeric types.

    Raises RuntimeError / ValueError on malformed files.
    """
    with open(path, 'rb') as f:
        # Validate magic
        magic = read_exact(f, 4)
        if magic != MAGIC:
            raise RuntimeError("Bad magic: not a CSTM file")

        # Read version and reserved (we don't currently use version)
        ver_b = read_exact(f, 1)
        version = ver_b[0]
        # reserved 7 bytes
        _ = read_exact(f, 7)

        # header_len (u64 little-endian)
        header_len_bytes = read_exact(f, 8)
        header_len = unpack_u64(header_len_bytes)

        # Read header bytes
        header_bytes = read_exact(f, header_len)

        # Parse header
        schema_sig, total_rows, columns_meta = parse_header(header_bytes)

        # Build map name -> column_meta
        name_to_meta = {col['name']: col for col in columns_meta}

        # Decide which columns to read and in what order
        if select_columns is None:
            selected_meta = columns_meta  # preserves header order
        else:
            selected_meta = []
            for name in select_columns:
                if name not in name_to_meta:
                    raise ValueError(f"Requested column '{name}' not found in file")
                selected_meta.append(name_to_meta[name])

        # For each selected column, seek, read compressed data, decompress, parse
        results_columns = []  # list of (name, parsed_values_list)
        for col in selected_meta:
            name = col['name']
            typ = col['type']
            num_values = col['num_values']
            offset = col['offset']
            comp_size = col['comp_size']
            uncomp_size = col['uncomp_size']

            # validate sizes are sensible
            if comp_size == 0 and uncomp_size == 0:
                # empty column block — handle as empty arrays
                parsed = []
                results_columns.append((name, parsed))
                continue

            # Seek to compressed block and read it
            f.seek(offset)
            comp_bytes = read_exact(f, comp_size)

            # Decompress
            try:
                uncompressed = zlib.decompress(comp_bytes)
            except Exception as e:
                raise RuntimeError(f"zlib decompression failed for column '{name}': {e}")

            # Validate uncompressed size if provided
            if len(uncompressed) != uncomp_size:
                # Warning: not fatal, but likely indicates corruption
                raise RuntimeError(
                    f"Uncompressed size mismatch for column '{name}': header says {uncomp_size}, "
                    f"got {len(uncompressed)}"
                )

            # Parse according to type
            parser = _PARSERS.get(typ)
            if parser is None:
                raise ValueError(f"Unknown column type {typ} for column '{name}'")
            parsed_values = parser(uncompressed, num_values)

            # Convert parsed numeric values to strings for CSV-friendly output,
            # but preserve strings as-is. If you want typed outputs, skip conversion.
            # Here we keep the parsed values directly (numbers for ints/floats, str for strings).
            results_columns.append((name, parsed_values))

        # Reconstruct rows by zipping columns
        if len(results_columns) == 0:
            return [], []

        col_names = [c[0] for c in results_columns]
        cols_data = [c[1] for c in results_columns]

        # Validate all selected columns have same length (num_values)
        expected_len = len(cols_data[0])
        for arr in cols_data:
            if len(arr) != expected_len:
                raise RuntimeError("Selected columns have differing lengths; file may be corrupt")

        # Build rows (list of lists)
        rows = []
        for i in range(expected_len):
            row = []
            for arr in cols_data:
                # Convert non-strings to str for uniform CSV output
                v = arr[i]
                # keep numeric types as-is if you want; for CSV convert to str below as needed
                row.append(v)
            rows.append(row)

        return col_names, rows


def read_cstm_to_csv(cstm_path: str, csv_out_path: str, select_columns: Optional[List[str]] = None) -> None:
    """
    Convenience helper: reads selected columns and writes a CSV file.
    """
    import csv
    col_names, rows = read_cstm(cstm_path, select_columns=select_columns)
    with open(csv_out_path, 'w', newline='', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow(col_names)
        # Convert all values to strings for CSV
        for r in rows:
            writer.writerow([str(x) for x in r])
