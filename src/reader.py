#!/usr/bin/env python3
"""
src/reader.py
Reader for CFILE (SPEC.md). Provides read_all() and read_columns().
"""

import struct
import zlib
import json
from pathlib import Path
from typing import List, Dict, Any

DT_INT32 = 0
DT_FLOAT64 = 1
DT_STRING = 2

def read_u8(b, offset): return struct.unpack_from("<B", b, offset)[0], offset+1
def read_u16(b, offset): return struct.unpack_from("<H", b, offset)[0], offset+2
def read_u32(b, offset): return struct.unpack_from("<I", b, offset)[0], offset+4
def read_u64(b, offset): return struct.unpack_from("<Q", b, offset)[0], offset+8

class ColumnMeta:
    def __init__(self, name: str, dtype: int, block_offset: int,
                 compressed_size: int, uncompressed_size: int, num_values: int):
        self.name = name
        self.dtype = dtype
        self.block_offset = block_offset
        self.compressed_size = compressed_size
        self.uncompressed_size = uncompressed_size
        self.num_values = num_values

class CFileReader:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.schema = []  # list of {"name","type"}
        self.num_rows = 0
        self.num_columns = 0
        self.columns: List[ColumnMeta] = []
        self._parse_header()

    def _parse_header(self):
        with open(self.path, "rb") as fh:
            # read fixed small prefix first (12 bytes min + schema_len)
            prefix = fh.read(12)
            if len(prefix) < 12:
                raise ValueError("file too small")
            magic = prefix[0:8]
            if magic.rstrip(b"\x00") != b"CFILEv01":
                raise ValueError("bad magic: " + repr(magic))
            # version and endianness in bytes 8 and 9
            version = prefix[8]
            endianness = prefix[9]
            if version != 1:
                raise ValueError("unsupported version: " + str(version))
            if endianness != 0:
                raise ValueError("unsupported endianness: " + str(endianness))
            # now read schema_len (uint32)
            rest = fh.read(4)
            if len(rest) < 4:
                raise ValueError("truncated schema length")
            schema_len = struct.unpack("<I", rest)[0]
            schema_json = fh.read(schema_len)
            if len(schema_json) < schema_len:
                raise ValueError("truncated schema json")
            schema_obj = json.loads(schema_json.decode("utf-8"))
            self.schema = schema_obj.get("columns", [])
            # read global metadata
            gm = fh.read(12)
            if len(gm) < 12:
                raise ValueError("truncated global metadata")
            self.num_rows = struct.unpack("<Q", gm[0:8])[0]
            self.num_columns = struct.unpack("<I", gm[8:12])[0]
            # read per-column directory
            self.columns = []
            for _ in range(self.num_columns):
                # read name_len (2)
                nl_bytes = fh.read(2)
                if len(nl_bytes) < 2:
                    raise ValueError("truncated dir")
                name_len = struct.unpack("<H", nl_bytes)[0]
                name = fh.read(name_len).decode("utf-8")
                dtype_b = fh.read(1)
                dtype = struct.unpack("<B", dtype_b)[0]
                block_offset = struct.unpack("<Q", fh.read(8))[0]
                compressed_size = struct.unpack("<Q", fh.read(8))[0]
                uncompressed_size = struct.unpack("<Q", fh.read(8))[0]
                num_values = struct.unpack("<Q", fh.read(8))[0]
                self.columns.append(ColumnMeta(name, dtype, block_offset, compressed_size, uncompressed_size, num_values))

    def _read_column_uncompressed(self, col: ColumnMeta) -> bytes:
        with open(self.path, "rb") as fh:
            fh.seek(col.block_offset)
            comp = fh.read(col.compressed_size)
            if len(comp) != col.compressed_size:
                raise ValueError(f"failed to read full compressed block for {col.name}")
            uncompressed = zlib.decompress(comp)
            if len(uncompressed) != col.uncompressed_size:
                # warning but continue
                raise ValueError(f"uncompressed size mismatch for {col.name}: expected {col.uncompressed_size}, got {len(uncompressed)}")
            return uncompressed

    def read_column(self, name: str) -> List[Any]:
        meta = next((c for c in self.columns if c.name == name), None)
        if meta is None:
            raise KeyError("no such column: " + name)
        data = self._read_column_uncompressed(meta)
        # parse according to dtype
        vals = []
        if meta.dtype == DT_INT32:
            # unpack sequence of int32
            count = meta.num_values
            for i in range(count):
                v = struct.unpack_from("<i", data, i*4)[0]
                vals.append(v)
        elif meta.dtype == DT_FLOAT64:
            count = meta.num_values
            for i in range(count):
                v = struct.unpack_from("<d", data, i*8)[0]
                vals.append(v)
        elif meta.dtype == DT_STRING:
            # first (num_values+1) uint32 offsets
            offs = []
            for i in range(meta.num_values + 1):
                offs.append(struct.unpack_from("<I", data, i*4)[0])
            base = 4*(meta.num_values + 1)
            sblob = data[base:]
            for i in range(meta.num_values):
                a = offs[i]
                b = offs[i+1]
                vals.append(sblob[a:b].decode("utf-8"))
        else:
            raise ValueError("unknown dtype")
        return vals

    def read_columns(self, names: List[str]) -> Dict[str, List[Any]]:
        result = {}
        for n in names:
            result[n] = self.read_column(n)
        return result

    def read_all(self) -> List[Dict[str, Any]]:
        # materialize all columns, then produce rows
        col_names = [c.name for c in self.columns]
        col_data = [self.read_column(n) for n in col_names]
        rows = []
        for i in range(self.num_rows):
            row = {}
            for cname, cvals in zip(col_names, col_data):
                row[cname] = cvals[i]
            rows.append(row)
        return rows
