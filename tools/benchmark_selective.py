# tools/benchmark_selective.py
import time, os, csv, sys
from reader import read_cstm

csv_path = sys.argv[1]
cstm_path = sys.argv[2]
col_index = int(sys.argv[3])  # index of column to extract for CSV
col_name = sys.argv[4]        # name of column for CSTM read

# CSV scan
t0 = time.time()
count = 0
with open(csv_path, newline='', encoding='utf-8') as f:
    r = csv.reader(f)
    header = next(r)
    for row in r:
        _ = row[col_index]
        count += 1
t_csv = time.time() - t0

# CSTM selective read
t0 = time.time()
names, rows = read_cstm(cstm_path, select_columns=[col_name])
t_cstm = time.time() - t0

print("rows scanned:", count)
print("CSV scan time:", t_csv)
print("CSTM selective time:", t_cstm)
print("CSV size (bytes):", os.path.getsize(csv_path))
print("CSTM size (bytes):", os.path.getsize(cstm_path))
