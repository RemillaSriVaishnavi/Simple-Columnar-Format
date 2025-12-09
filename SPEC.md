# CSTM — Simple Columnar Binary Format (SPEC)

## Overview
CSTM is a small, columnar binary format optimized for analytic workloads. Each column is stored as a separate compressed block. A compact header at the file start contains schema and absolute block offsets so a reader can seek to and read only requested columns.

All multi-byte integers and floats are **little-endian**.

## Top-level file layout (byte offsets)

0x00  4 bytes   Magic: ASCII "CSTM"
0x04  1 byte    Version (uint8) = 1
0x05  7 bytes   Reserved (zeros)
0x0C  8 bytes   Header length `H` (uint64 LE) — number of bytes in header section
0x14  H bytes   Header (binary, described below)
0x14+H ...     Column blocks (compressed with zlib) — stored consecutively
EOF

## Header binary structure (size = H)
- uint32 LE: schema_signature (CRC32 of schema JSON; optional, may be 0)
- uint64 LE: total_rows
- uint32 LE: num_columns (N)

Then for each column (repeat N times):

For each column entry:
  - uint16 LE: name_length (L)
  - L bytes: UTF-8 column name
  - uint8: type_id
     * 0 = INT32 (signed 32-bit)
     * 1 = FLOAT64 (IEEE 754)
     * 2 = STRING (variable-length)
  - uint8: flags (bitmask; 0 for now)
  - uint64 LE: num_values (rows in column)
  - uint64 LE: block_offset (absolute file byte offset to start of compressed block)
  - uint64 LE: compressed_size (bytes)
  - uint64 LE: uncompressed_size (bytes)

Notes:
- `block_offset` points to the first byte of the zlib compressed block for the column.
- Readers use `block_offset` + `compressed_size` to read only the bytes needed.

## Column uncompressed block formats (before compression)

### INT32 (type 0)
- N 32-bit signed integers, little-endian.
- Uncompressed size = 4 * num_values

### FLOAT64 (type 1)
- N IEEE 754 double values, little-endian.
- Uncompressed size = 8 * num_values

### STRING (type 2) — two-block layout inside *one* logical column block
To keep random access light, the column's uncompressed block contains two parts concatenated:

1) Offsets section:
   - num_values + 1 uint32 LE offsets: O[0], O[1], ..., O[num_values]
   - Each offset is the byte index (relative to start of the data section) where string i begins; O[0] = 0, O[i+1] = O[i] + len(string_i)
   - Using num_values+1 simplifies slicing and empty-string handling.
   - Offsets are 32-bit unsigned; if your strings might exceed 4GiB, use 64-bit in a future version.

2) Data section:
   - Concatenated UTF-8 bytes of all strings: string_0 + string_1 + ... + string_n-1

Uncompressed size = 4*(num_values+1) + sum(len(string_i) for i)

NULL handling:
- Reserve a special value in the offsets array (e.g., O[i] == 0xFFFFFFFF) to denote NULL. (Optional initially.)

## Compression
- Each column's full uncompressed block (as above) is compressed with **zlib** (RFC1950).
- Header stores both `compressed_size` and `uncompressed_size`.

## Endianness
- All multi-byte numbers are little-endian.

## Writing procedure (summary)
- Build header (with placeholders for offsets), write header length and header.
- Write compressed column blocks consecutively, record each block's offset & sizes.
- Re-write the header area with actual block offsets & sizes.

## Reading procedure (summary)
- Validate magic + version, read header length, parse header.
- For full reads: for each column, seek to block_offset, read compressed_size, decompress, parse block into column array, then reconstruct rows.
- For selective reads: only seek/read/decompress the requested columns.

## Versioning & compatibility
- Version byte allows evolution.
- Readers must ignore unknown flags and should validate header length.

