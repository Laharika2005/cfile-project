#!/usr/bin/env python3
"""
src/writer.py
Simple writer for CFILE according to SPEC.md

Usage:
    python src\writer.py samples\sample_small.csv out.cfile
"""

import sys
import csv
import json
import struct
import zlib
from pathlib import Path
from typing import List, Tuple, Dict, Any

MAGIC = b"CFILEv01"
VERSION = 1
ENDIANNESS = 0
RESERVED = 0

DT_INT32 = 0
DT_FLOAT64 = 1
DT_STRING = 2

def pack_u8(v): return struct.pack("<B", v)
def pack_u16(v): return struct.pack("<H", v)
def pack_u32(v): return struct.pack("<I", v)
def pack_u64(v): return struct.pack("<Q", v)

def pad_magic(magic_bytes: bytes) -> bytes:
    return magic_bytes.ljust(8, b"\x00")[:8]

def infer_type(value: str) -> int:
    if value == "":
        return DT_STRING
    try:
        int(value)
        return DT_INT32
    except:
        try:
            float(value)
            return DT_FLOAT64
        except:
            return DT_STRING

def infer_schema_from_csv(csv_path: Path):
    with csv_path.open(newline='', encoding='utf-8-sig') as fh:
        rdr = csv.reader(fh)
        header = next(rdr)
        sample_row = next(rdr, None)
    if sample_row is None:
        return [{"name": n, "type": "STRING"} for n in header]
    schema = []
    for name, sample in zip(header, sample_row):
        dtype = infer_type(sample)
        typename = {0: "INT32", 1: "FLOAT64", 2: "STRING"}[dtype]
        schema.append({"name": name, "type": typename})
    return schema

def read_csv_columns(csv_path: Path, schema):
    with csv_path.open(newline='', encoding='utf-8-sig') as fh:
        rdr = csv.reader(fh)
        header = next(rdr)
        cols = [[] for _ in header]
        for row in rdr:
            row = list(row) + [""] * (len(header) - len(row))
            for i, v in enumerate(row[:len(header)]):
                cols[i].append(v)
    return header, cols

def build_uncompressed_block(col_type, values):
    if col_type == "INT32":
        ba = bytearray()
        for v in values:
            if v == "":
                ival = 0
            else:
                ival = int(float(v))
            ba += struct.pack("<i", ival)
        return bytes(ba)

    elif col_type == "FLOAT64":
        ba = bytearray()
        for v in values:
            if v == "":
                fval = 0.0
            else:
                fval = float(v)
            ba += struct.pack("<d", fval)
        return bytes(ba)

    elif col_type == "STRING":
        string_bytes = bytearray()
        offsets = []
        cur = 0
        for v in values:
            offsets.append(cur)
            b = v.encode("utf-8")
            string_bytes += b
            cur += len(b)
        offsets.append(cur)

        off_b = bytearray()
        for o in offsets:
            off_b += struct.pack("<I", o)
        return bytes(off_b + string_bytes)

    else:
        raise ValueError("Unknown type")

def write_cfile(out_path, schema, header_names, columns):
    num_rows = len(columns[0])
    num_columns = len(schema)

    schema_json = json.dumps({"columns": schema}, separators=(",", ":")).encode("utf-8")
    schema_len = len(schema_json)

    col_uncompressed = []
    col_compressed = []

    for col_meta, values in zip(schema, columns):
        ub = build_uncompressed_block(col_meta["type"], values)
        comp = zlib.compress(ub)
        col_uncompressed.append(ub)
        col_compressed.append(comp)

    header = bytearray()
    header += pad_magic(MAGIC)
    header += pack_u8(VERSION)
    header += pack_u8(ENDIANNESS)
    header += pack_u16(RESERVED)
    header += pack_u32(schema_len)
    header += schema_json
    header += pack_u64(num_rows)
    header += pack_u32(num_columns)

    directory_entries = []
    for col_meta in schema:
        name_b = col_meta["name"].encode("utf-8")
        header += pack_u16(len(name_b))
        header += name_b
        dtype = {"INT32":0,"FLOAT64":1,"STRING":2}[col_meta["type"]]
        header += pack_u8(dtype)
        header += pack_u64(0)
        header += pack_u64(0)
        header += pack_u64(0)
        header += pack_u64(num_rows)
        directory_entries.append((name_b, dtype))

    header_len = len(header)
    pad = (8 - (header_len % 8)) % 8
    start_blocks = header_len + pad

    offsets = []
    cur = start_blocks
    for comp in col_compressed:
        offsets.append(cur)
        cur += len(comp)
        if cur % 8 != 0:
            cur += (8 - (cur % 8))

    with out_path.open("wb") as fh:
        h = bytearray()
        h += pad_magic(MAGIC)
        h += pack_u8(VERSION)
        h += pack_u8(ENDIANNESS)
        h += pack_u16(RESERVED)
        h += pack_u32(schema_len)
        h += schema_json
        h += pack_u64(num_rows)
        h += pack_u32(num_columns)

        for i, col_meta in enumerate(schema):
            name_b = col_meta["name"].encode("utf-8")
            h += pack_u16(len(name_b))
            h += name_b
            dtype = {"INT32":0,"FLOAT64":1,"STRING":2}[col_meta["type"]]
            h += pack_u8(dtype)
            h += pack_u64(offsets[i])
            h += pack_u64(len(col_compressed[i]))
            h += pack_u64(len(col_uncompressed[i]))
            h += pack_u64(num_rows)

        fh.write(h)
        if pad:
            fh.write(b"\x00" * pad)

        for comp in col_compressed:
            fh.write(comp)
            curpos = fh.tell()
            padb = (8 - (curpos % 8)) % 8
            if padb:
                fh.write(b"\x00" * padb)

def main():
    if len(sys.argv) != 3:
        print("Usage: python src\\writer.py input.csv output.cfile")
        return

    csv_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    schema = infer_schema_from_csv(csv_path)
    header_names, columns = read_csv_columns(csv_path, schema)

    write_cfile(out_path, schema, header_names, columns)
    print(f"Wrote CFILE to {out_path}")

if __name__ == "__main__":
    main()
