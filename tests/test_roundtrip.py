import os
import csv
from writer import write_cstm
from reader import read_cstm

def write_csv(path, header, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def test_small_roundtrip(tmp_path):
    header = ['id', 'age', 'name']
    rows = [[1, 30, 'Alice'], [2, 25, 'Bob'], [3, 22, 'Chandra']]
    csv_path = tmp_path / "in.csv"
    write_csv(str(csv_path), header, rows)

    cstm_path = tmp_path / "t.cstm"
    columns = list(zip(*rows))
    columns = [list(col) for col in columns]

    # INT32, INT32, STRING
    types = [0, 0, 2]

    write_cstm(str(cstm_path), header, types, columns)

    names, out_rows = read_cstm(str(cstm_path))

    assert names == header

    expected = [
        [1, 30, 'Alice'],
        [2, 25, 'Bob'],
        [3, 22, 'Chandra']
    ]

    assert out_rows == expected


def test_selective_read(tmp_path):
    header = ['id', 'age', 'name']
    rows = [[1, 30, 'Alice'], [2, 25, 'Bob'], [3, 22, 'Chandra']]
    csv_path = tmp_path / "in.csv"
    write_csv(str(csv_path), header, rows)

    cstm_path = tmp_path / "t2.cstm"
    columns = list(zip(*rows))
    columns = [list(col) for col in columns]
    types = [0, 0, 2]

    write_cstm(str(cstm_path), header, types, columns)

    names, out_rows = read_cstm(str(cstm_path))
    assert names == header

    expected = rows
    assert out_rows == expected
