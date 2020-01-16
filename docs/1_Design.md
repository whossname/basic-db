# Design

## Motivation

- Learn more about how databases work at a low level
- Practise using the Rust programming language

## Decisions

![SQLite Architecture Diagram](https://www.sqlite.org/images/arch2.gif "SQLite Architecture Diagram")

The design is heavily influenced by the sqlite architecture as seen above. The
main focus will be on the database backend (in the blue). As such the SQL
command processor will be very simple. For now only the following two commands
will be implemented:

```sql
INSERT INTO table (col1, col2, ...) VALUES (a1, a2, ...), (b1, b2, ...)
```

```sql
SELECT col1, col2, ... FROM table
```

These will be implemented with some simple adhoc string parsing. The will make
the SQL Compiler and virtual machine very simple. The OS interface will also
be simplified by only targeting the OS I am currently using (Ubuntu 16.04).
For now we will not implement the following features:

- hot journals (for rollbacks)
- indexes
- freelists (for deleting data)
- file locks/change counters (for concurrency)
- pointer maps (for vacuuming, only makes sense once deleting is implemented)
- versioning numbers and file validation checks
- the sqlite_sequence table
- the sqlite_stat tables
- page reserved regions

We also don't need the SQLite legacy features such as the lock-byte page and
several of the database header fields.

Finally the B\*-Tree implementaion for tables will be simplified by including a
64-bit unsigned integer "id" field in every table to act as the primary key.
The autoincrement value for this id field will be stored in the master table.
This simplifies the record sort order and auto increment feature,
removeing the need for the sqlite_sequence table.

## Database File Format

The file format design is heavily influenced by the [SQLite format](https://www.sqlite.org/fileformat.html). Based of the descisions made above the following secions are relevant:

1. The Database File  
   1.2. Pages  
   1.3. The Database Header  
   &nbsp; &nbsp; 1.3.2. Page Size  
   &nbsp; &nbsp; 1.3.7. In-header database size  
   1.6. B-tree Pages  
   1.7. Cell Payload Overflow Pages
2. Schema Layer  
   2.1. Record Format  
   2.3. Representation Of SQL Tables  
   2.6. Storage Of The SQL Database Schema _(includes details on the master table)_

All data is stored in the big-endian layout.

### Header

The first 100 bytes of the SQLite database file comprise the database file
header. This if far more than what we need for our database. SQLite doesn't
use all of this space, the remainder is reserved for expansion. We will adopt
the same design. The following is the header design we will use for our database:

| Offest | Size | Description                        |
| ------ | ---- | ---------------------------------- |
| 0      | 2    | The database page size in bytes    |
| 2      | 4    | Size of the database file in pages |
| 6      | 94   | Reserved for expansion             |

Our header is very small because of all of the features we have removed.
The SQLite header only has 20 bytes reserved for expansion compared to our 94.

When the database is first opened, the first 100 bytes of the database file (the database file header) are read as a sub-page size unit. The header is stored as
part of Page 1 of the database.

### Tables

Tables are implemented as B\*-trees with leaf pages and interior pages as the nodes.

The content of each SQL table row is stored in the database file by first
combining the values in the various columns into a byte array in the record
format, then storing that byte array as the payload in an entry in the table b-tree.
The order of values in the record is the same as the order of columns in the
SQL table definition.

Floats that can be stored as integers are automatically converted to integers for storage.

### B-tree Pages

Only three pages need to be implemented for our basic database:

- A table b-tree interior page
- A table b-tree leaf page
- A payload overflow page

Pages are numbered beginning with 1. Page 1 is always the first page of the
master table. Page 1 includes the database file header.

A b-tree page is divided into regions in the following order:

1. The 100-byte database file header (found on page 1 only, described above)
2. The 8 or 12 byte b-tree page header
3. The cell pointer array
4. Unallocated space
5. The cell content area

#### Page Header

The following is the structure of the page header. Note that freeblocks and
fragmented byte counters are no implemented as we are not implementing the
delete functionality in our database.

| Offset | Size | Description                                                    |
| ------ | ---- | -------------------------------------------------------------- |
| 0      | 1    | Page type                                                      |
| 1      | 2    | First freeblock on the page (not implemented)                  |
| 3      | 2    | Number of cells on the page                                    |
| 5      | 2    | Start of the cell content area                                 |
| 7      | 1    | Number of cell content fragmented free bytes (not implemented) |
| 8      | 4    | Rightmost pointer (interior b-tree pages only)                 |

The page type can be the following values:

- A value of 2 means the page is an interior index b-tree page
- A value of 5 means the page is an interior table b-tree page

The first freeblock is zero if there are no free blocks in the page.

#### Cell pointer array

The cell pointer array is an array of 2-byte integer offsets to the cell contents
from the start of the page.
The cell pointers are arranged in key order with left-most cell (the cell with
the smallest key) first and the right-most cell (the cell with the largest key) last.

### Overflow Pages

The following calculations are used to determine whether an overflow page should be used.

Variables:

- u: usable size of a database page, i.e. the total page size
- p: the payload size
- x: maximum payload that can be stored directly on the b-tree page without spilling onto an overflow page
- m: minimum payload that must be stored on the btree page before spilling is allowed

```rust
x = u - 35;
if(is_index) { // it isn't
    x = (u - 12) * 64 / 255 - 23;
}

m = (u - 12) * 32 / 255 - 23
k = m + (p - m) % (u - 4);

if (p <= x) {
    // store directly in page
} else if (k <= x) {
    // store the first k bytes directly
    // use overflow for the rest
} else {
    // store the first m bytes directly
    // use overflow for the rest
}
```

Overflow pages form a linked list. The first four bytes indicate the next page
in the chain, zero indicates the page is the last link in the chain.
The remaining space is used to hold the overflow content.

## Records

Payloads are always in the record format:

- header
  - header size (varint) _includes itself_
  - per column serial type (varint)
- body

Serial type:

| Type         | Size     | Description |
| ------------ | -------- | ----------- |
| 0            | 0        | Null        |
| 1            | 1        | Int         |
| 2            | 2        | Int         |
| 3            | 3        | Int         |
| 4            | 4        | Int         |
| 5            | 6        | Int         |
| 6            | 8        | Int         |
| 7            | 8        | Float       |
| 8            | 0        | Int 0       |
| 9            | 0        | Int 1       |
| N >=12, even | (N-12)/2 | Blob        |
| N >=13, odd  | (N-13)/2 | String      |

Follow this spec verbatim https://www.sqlite.org/fileformat.html#record_format

## varints

varints are big-endian variable-length 64-bit integers where the most significant
bit of each byte indicates whether the integer is complete. The ninth byte is
an exception, every bit in the ninth bit is used for the integer value.

Some examples:

| Number | int (i32)  | varint |
| ------ | ---------- | ------ |
| 1      | 0x00000001 | 0x01   |
| 27     | 0x0000001B | 0x1B   |
| 128    | 0x00000080 | 0x8100 |
| 256    | 0x00000100 | 0x8200 |
| 2048   | 0x00000800 | 0x9000 |

## Cells

The following defines the cell structures.

### B-Tree Leaf

- payload bytes (varint) _includes overflow_
- id (varint)
- payload (see record)
- first overflow page number (i32) _include if overflow exists_

### B-Tree Interior

- left child page number (i32)
- id (varint)

## Master table

Page 1 of a database file is the root page of the master table. The master table
stores the database schema. It has the following fields:

- schema type (i8) _1 for table_
- tbl_name (text)
- rootpage (int)
- sql (text)

## OS Integration

The following bash command gives the default pagesize on a Linux machine:

```bash
getconf PAGESIZE
```
