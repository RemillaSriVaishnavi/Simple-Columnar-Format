# Simple Columnar File Format (CSTM)

This project implements a minimal columnar storage format similar to Parquet and ORC,
built entirely from scratch in Python. The repository includes:

- A writer (`write_cstm`) that converts CSV → CSTM
- A reader (`read_cstm`) that converts CSTM → CSV
- Support for `INT32`, `FLOAT64`, and `STRING` column types
- Zlib compression per column block
- A complete binary format specification in `SPEC.md`
- Test suite validating round-trip correctness

---

## 📦 Installation

```bash
pip install -r requirements.txt
