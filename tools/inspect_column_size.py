# tools/inspect_column_size.py
from header_utils import parse_header
import struct, sys
p = sys.argv[1]
with open(p,'rb') as f:
    f.seek(12)
    header_len_bytes = f.read(8)
    header_len = struct.unpack('<Q', header_len_bytes)[0]
    header = f.read(header_len)
sig, total_rows, cols = parse_header(header)
for c in cols:
    if c['name']=='col10':
        print("comp_size:", c['comp_size'])
