# Design

## Motivation

- Learn more about how databases work at a low level
- Practise using the Rust programming language

## Decisions

![SQLite Architecture Diagram](https://www.sqlite.org/arch.html "SQLite Architecture Diagram")

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
- pointer maps (for vacuming)
- versioning numbers and file validation checks
- the sqlite_sequence table
- the sqlite_stat tables

We also don't need the SQLite legacy features such as the lock-byte page and
several of the header fields.

Finally the B\*-Tree implementaion for tables will be simplified by including an
"id" field in every table to act as the primary key and be stored in the master
table. This simplifies the record sort order and auto increment features. This
removes the need to the sqlite_sequence table.

## Database File Format

The file format design is heavily influenced by the [SQLite format](https://www.sqlite.org/fileformat.html). Based of the descisions made above the following secions are relevant:

1. The Database File
   1.2. Pages
   1.3. The Database Header
   1.3.2. Page Size
   1.3.7. In-header database size
   1.6. B-tree Pages
   1.7. Cell Payload Overflow Pages
2. Schema Layer
   2.1. Record Format
   2.3. Representation Of SQL Tables
   2.6. Storage Of The SQL Database Schema _(details on the master table)_
