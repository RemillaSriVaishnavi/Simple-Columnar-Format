# tools/generate_wide_csv.py
import csv, sys, random

out = sys.argv[1]
ncols = int(sys.argv[2])   # e.g., 50
nrows = int(sys.argv[3])   # e.g., 100000
header = [f"col{i}" for i in range(ncols)]
with open(out, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(header)
    for r in range(nrows):
        row = [str(random.randint(0,1000000)) for _ in range(ncols)]
        w.writerow(row)
print("wrote", out)
