# tests/test_roundtrip.py
from pathlib import Path
import src.writer as writer
from src.reader import CFileReader

def test_roundtrip(tmp_path):
    src_csv = Path("samples/sample_small.csv")
    out_cfile = tmp_path / "test.cfile"

    # build CFILE
    schema = writer.infer_schema_from_csv(src_csv)
    header_names, columns = writer.read_csv_columns(src_csv, schema)
    writer.write_cfile(out_cfile, schema, header_names, columns)

    # read back
    r = CFileReader(out_cfile)
    rows = r.read_all()

    assert r.num_rows == len(rows)
    assert r.num_columns == len(r.columns)
