import time, csv, argparse
# Ensure project root is on PYTHONPATH so 'src' package is importable
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from pathlib import Path
from src.writer import infer_schema_from_csv, read_csv_columns, write_cfile
from src.reader import CFileReader

def build_large_sample(csv_in="samples/sample_small.csv", rows=20000, out_csv="samples/large.csv"):
    with open(csv_in, "r", encoding="utf-8") as fh:
        header = fh.readline().strip()
        lines = [l.strip() for l in fh if l.strip()]
    with open(out_csv, "w", encoding="utf-8", newline="") as out:
        out.write(header + "\n")
        for i in range(rows):
            out.write(lines[i % len(lines)] + "\n")
    return out_csv

def time_csv_read_column(csv_path, column_idx=1):
    start = time.time()
    with open(csv_path, newline='', encoding='utf-8') as fh:
        rdr = csv.reader(fh)
        hdr = next(rdr)
        out = []
        for row in rdr:
            out.append(row[column_idx])
    return time.time() - start, len(out)

def time_cfile_select(cfile_path, column_name):
    start = time.time()
    r = CFileReader(Path(cfile_path))
    vals = r.read_column(column_name)
    return time.time() - start, len(vals)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple benchmark: CSV scan vs CFILE selective read")
    parser.add_argument("--rows", type=int, default=20000, help="number of rows to generate")
    args = parser.parse_args()
    rows = args.rows

    print("Building large CSV with rows =", rows)
    large_csv = build_large_sample(rows=rows)
    print("Large CSV created:", large_csv)

    print("Creating CFILE from large CSV (this may take a few seconds)...")
    schema = infer_schema_from_csv(Path(large_csv))
    hdr, cols = read_csv_columns(Path(large_csv), schema)
    write_cfile(Path("samples/large.cfile"), schema, hdr, cols)
    print("Wrote samples/large.cfile")

    csv_time, n1 = time_csv_read_column(large_csv, 1)
    cfile_time, n2 = time_cfile_select("samples/large.cfile", schema[1]['name'])
    print(f"CSV scan time: {csv_time:.3f}s, CFILE selective read time: {cfile_time:.3f}s, rows={n1}")
