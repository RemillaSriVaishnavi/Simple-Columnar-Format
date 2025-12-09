# tools/hexdump_header.py
import struct, sys

path = sys.argv[1]
with open(path, 'rb') as f:
    magic = f.read(4); print("magic:", magic)
    ver = f.read(1); print("version:", ver[0])
    reserved = f.read(7)
    header_len_bytes = f.read(8)
    header_len = struct.unpack('<Q', header_len_bytes)[0]
    print("header_len:", header_len)
    # print first 16 bytes of header
    header_preview = f.read(min(16, header_len))
    print("header_preview (hex):", header_preview.hex())
    # seek to header end and print 16 bytes after header (likely compressed data start)
    f.seek(20 + header_len)
    post = f.read(16)
    print("bytes after header (hex):", post.hex())
