#!/usr/bin/env python3
"""
CLI: CSV -> CFILE

Usage:
    python -m src.cli_csv_to_custom input.csv output.cfile
"""

import argparse
from pathlib import Path
import src.writer as writer


def main():
    parser = argparse.ArgumentParser(description="Convert CSV -> CFILE")
    parser.add_argument("input", help="Input CSV file")
    parser.add_argument("output", help="Output CFILE file")
    args = parser.parse_args()

    csv_path = Path(args.input)
    out_path = Path(args.output)

    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")

    # use writer helpers
    schema = writer.infer_schema_from_csv(csv_path)
    header_names, columns = writer.read_csv_columns(csv_path, schema)
    writer.write_cfile(out_path, schema, header_names, columns)

    print(f"Wrote CFILE to {out_path} (rows={len(columns[0])}, cols={len(schema)})")


if __name__ == "__main__":
    main()
