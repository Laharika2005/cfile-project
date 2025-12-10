# src/cli_custom_to_csv.py
import argparse
from pathlib import Path
from src.reader import CFileReader

def csv_escape(value):
    if value is None:
        return '""'
    s = str(value)
    if s == "":
        return '""'
    if ',' in s or '"' in s:
        s = s.replace('"', '""')
        return f'"{s}"'
    return s

def write_csv(path: str, headers, rows):
    path = Path(path)
    with path.open("w", encoding="utf-8", newline="") as f:
        # write header (headers must be strings)
        f.write(",".join(headers) + "\n")
        # write rows (each row is an ordered iterable of values)
        for row in rows:
            escaped = [csv_escape(v) for v in row]
            f.write(",".join(escaped) + "\n")

def main():
    parser = argparse.ArgumentParser(description="Convert CFILE → CSV")
    parser.add_argument("input", help="Input .cfile")
    parser.add_argument("output", help="Output .csv")
    parser.add_argument("--select", help="Comma-separated list of columns", default=None)

    args = parser.parse_args()

    r = CFileReader(Path(args.input))

    if args.select:
        cols = [c.strip() for c in args.select.split(",")]
        data = r.read_columns(cols)
        headers = cols
        # data values are lists; zip(*) gives rows as tuples
        rows = list(zip(*[data[c] for c in cols]))
    else:
        # r.read_all() returns list of dicts {colname: value}
        dict_rows = r.read_all()
        # extract header names from r.columns (ColumnMeta objects)
        headers = []
        for c in r.columns:
            # c may be ColumnMeta object or dict; handle both
            if hasattr(c, "name"):
                headers.append(c.name)
            elif isinstance(c, dict) and "name" in c:
                headers.append(c["name"])
            else:
                headers.append(str(c))
        # convert dict rows to lists following headers order
        rows = []
        for dr in dict_rows:
            rows.append([dr[h] for h in headers])

    write_csv(args.output, headers, rows)
    print(f"Wrote CSV to {args.output} (cols={len(headers)}, rows={len(rows)})")

if __name__ == "__main__":
    main()
