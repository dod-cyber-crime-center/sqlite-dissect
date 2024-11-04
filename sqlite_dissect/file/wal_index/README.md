
# sqlite_dissect.file.wal_index

This package will control parsing and access to the sqlite wal index files.

- header.py
- wal_index.py

TODO items for the "wal_index" package:

- [ ] Finish UML class diagrams.

<br>

### header.py
This script holds the header objects used for parsing the header of the wal index file.

This script holds the following object(s):
- WriteAheadLogIndexHeader(SQLiteHeader)
- WriteAheadLogIndexSubHeader(SQLiteHeader)
- WriteAheadLogIndexCheckpointInfo(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error and ord in classes.
- [ ] Implement big endian parsing (if needed).
- [ ] Create arrays for salt and checksum values rather than separate variables?  They are arrays in the sqlite c code.
    ##### WriteAheadLogIndexHeader Class:
    - [ ] Check the two sub headers against each other to ensure they are equal.
    - [ ] Document and handle exceptions that may be raised from creating subcomponents better.
    ##### WriteAheadLogIndexCheckpointInfo Class:
    - [ ] Handle the use case of 0xffffffff which is defined as READMARK_NOT_USED.
    - [ ] Handle the use case of the first reader mark always being 0.  (Check this)

<br>

### wal_index.py
This script holds the class to parse the wal index file.

This script holds the following object(s):
- WriteAheadLogIndex(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error in classes.
- [ ] Implement big endian parsing (if needed).
    ##### WriteAheadLogIndex Class:
    - [ ] This class was a test of parsing a (single page) wal index and needs to be fully implemented.
    - [ ] Should this be incorporated with the version/version history somehow?
    - [ ] Update to support a file object.
    - [ ] Constants for static integers.
    - [ ] Use cases for implementation of retrieving unallocated space for carving?
    - [ ] Check logging statements for correctness.
    - [ ] Account for the database text encoding in the file handle.
    - [ ] The file_size arg may not be needed since it is in the file handle and may be removed
