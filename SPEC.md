\# SPEC.md — Simple Columnar File Format (CFILE) v1



\## Overview

This document defines the binary layout for the CFILE columnar file format (version 1).  

All multi-byte numeric fields are stored in \*\*little-endian\*\* order.



The goal is to support:

\- Efficient columnar storage,

\- Fast selective column reads,

\- Compression of each column block,

\- Three types: INT32, FLOAT64, STRING.



---



\## 1. Fixed Header (start of file)



| Field         | Size (bytes) | Description |

|---------------|--------------|-------------|

| magic         | 8            | ASCII string "CFILEv01". If shorter, pad with 0x00 |

| version       | 1            | uint8. Current = 1 |

| endianness    | 1            | uint8. 0 = little-endian, 1 = big-endian |

| reserved      | 2            | uint16, always 0 |



Total fixed header size = 12 bytes.



---



\## 2. Schema Block



\### Fields:

\- `schema\_len` (4 bytes, uint32) — number of bytes in schema\_json

\- `schema\_json` (schema\_len bytes)



\#### Schema JSON Example:

```json

{

&nbsp; "columns": \[

&nbsp;   { "name": "id", "type": "INT32" },

&nbsp;   { "name": "price", "type": "FLOAT64" },

&nbsp;   { "name": "desc", "type": "STRING" }

&nbsp; ]

}



