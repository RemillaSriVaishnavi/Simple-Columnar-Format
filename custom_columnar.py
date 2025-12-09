#!/usr/bin/env python3
"""
custom_columnar.py

CLI wrapper for converting CSV <-> custom CSTM columnar format.

Usage:
  python custom_columnar.py csv_to_custom  in.csv  out.cstm
  python custom_columnar.py custom_to_csv  in.cstm out.csv [--cols col1,col2,...]

Exit codes:
  0 = success
  1 = runtime error (IO, format error, etc.)
  2 = incorrect usage (arg parsing)
"""
import sys
import csv
import argparse
from typing import List, Optional

# adjust imports if your module names differ
from writer import write_cstm
from reader import read_cstm, read_cstm_to_csv

def csv_to_custom_cli(in_csv: str, out_cstm: str) -> None:
    """
    Read CSV (small/medium size) fully into memory, convert to column-major and write .cstm.
    Note: for very large CSVs, implement a streaming writer instead.
    """
    # Read CSV fully
    with open(in_csv, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            # Empty CSV: no header -> write empty file with zero columns
            header = []
            rows = []
        else:
            rows = list(reader)

    # If there are rows, transpose into columns; if no rows, produce empty columns lists
    if header and rows:
        # Verify all rows have same length as header
        ncols = len(header)
        for r in rows:
            if len(r) != ncols:
                raise ValueError(f"CSV row has {len(r)} columns but header has {ncols}")
        # Build columns: list of lists (strings)
        columns = [ [row[i] for row in rows] for i in range(len(header)) ]
    else:
        # no rows or header
        columns = [ [] for _ in header ]

    # Infer types simply: user can modify to provide types manually.
    # Basic heuristic: all ints -> INT32; all floats -> FLOAT64; else STRING
    from column_serializers import serialize_int32_column, serialize_float64_column
    from header_utils import TYPE_INT32, TYPE_FLOAT64, TYPE_STRING  # if you put TYPE constants elsewhere, adjust
    # If TYPE constants are in byte_utils, import there:
    try:
        from byte_utils import TYPE_INT32, TYPE_FLOAT64, TYPE_STRING
    except Exception:
        # fallback constants
        TYPE_INT32, TYPE_FLOAT64, TYPE_STRING = 0,1,2

    def infer_type_for_col(col_values: List[str]) -> int:
        if len(col_values) == 0:
            return TYPE_STRING
        # try int
        try:
            for v in col_values:
                if v == '':
                    raise ValueError
                int(v)
            return TYPE_INT32
        except Exception:
            pass
        try:
            for v in col_values:
                if v == '':
                    raise ValueError
                float(v)
            return TYPE_FLOAT64
        except Exception:
            return TYPE_STRING

    column_names = header
    column_types = [ infer_type_for_col(col) for col in columns ]

    # write using writer.write_cstm
    write_cstm(out_cstm, column_names, column_types, columns)
    print(f"Wrote {out_cstm} with {len(column_names)} columns and {len(columns[0]) if columns else 0} rows")

def custom_to_csv_cli(in_cstm: str, out_csv: str, cols: Optional[List[str]] = None) -> None:
    """
    Read CSTM file and write CSV. If cols is provided it restricts to those column names.
    """
    if cols is None:
        # Use the convenience helper that writes CSV directly
        read_cstm_to_csv(in_cstm, out_csv)
    else:
        # read selected columns and write
        names, rows = read_cstm(in_cstm, select_columns=cols)
        with open(out_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(names)
            for r in rows:
                writer.writerow([str(x) for x in r])
    print(f"Wrote CSV {out_csv}")

def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(prog='custom_columnar.py',
                                     description='CSV <-> CSTM (custom columnar) converter')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p1 = sub.add_parser('csv_to_custom', help='Convert CSV to CSTM')
    p1.add_argument('in_csv', help='Input CSV path')
    p1.add_argument('out_cstm', help='Output .cstm file path')

    p2 = sub.add_parser('custom_to_csv', help='Convert CSTM back to CSV')
    p2.add_argument('in_cstm', help='Input .cstm file path')
    p2.add_argument('out_csv', help='Output CSV file path')
    p2.add_argument('--cols', help='Comma-separated list of columns to extract (optional)', default='')

    try:
        args = parser.parse_args(argv)
    except SystemExit:
        return 2

    try:
        if args.cmd == 'csv_to_custom':
            csv_to_custom_cli(args.in_csv, args.out_cstm)
        elif args.cmd == 'custom_to_csv':
            cols = [c for c in args.cols.split(',') if c] if args.cols else None
            custom_to_csv_cli(args.in_cstm, args.out_csv, cols)
        else:
            print("Unknown command", file=sys.stderr)
            return 2
        return 0
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())
