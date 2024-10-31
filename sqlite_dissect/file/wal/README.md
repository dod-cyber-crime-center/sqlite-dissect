
# sqlite_dissect.file.wal

This package will control parsing and access to the SQLite WAL files.

- commit_record.py
- frame.py
- header.py
- utilities.py
- wal.py

TODO items for the "wal" package:

- [ ] Finish UML class diagrams.

<br>

### commit_record.py
This script holds the objects used for parsing the write ahead log commit records.

This script holds the following object(s):
- WriteAheadLogCommitRecord(Version)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Better exception handling when creating objects, etc.
- [ ] Investigate where a database file has empty space beyond the page size (wal checkpoints were set).
- [ ] Is there a need (or way) to implement this without an initial database (just wal file)?
- [ ] Investigate where a database file has empty space beyond the page size (wal checkpoints were set).
    ##### WriteAheadLogCommitRecord Class:
    - [ ] Check lists and dictionaries for fields before adding.
    - [ ] Is there a better way to handle pointer map pages (parse on demand)?
    - [ ] Investigate when a set of frames does not have a commit frame.  (Warning currently thrown.)
    - [ ] Investigate root pages in commit records with no changes.  (Warning currently thrown.)
    - [ ] The incremental vacuum mode can change in the header from 1 to 2 or 2 to 1.
    - [ ] Investigate if the database text encoding/schema format number can change after set.
    - [ ] Investigate if the size in pages can differ on first update if last version < 3.7.0.

<br>

### frame.py
This script holds the objects used for parsing the WAL frame.

> Note:  The WriteAheadLogFrame class is not responsible for parsing the page data itself.  It is meant to give
> information on the WAL frame and offsets of the page data but in order to parse the page data, the set of all
> page changes to the commit record this frame belongs in is needed.  Therefore the commit record class
> (WriteAheadLogCommitRecord) will be responsible for parsing pages.
>
> There was some discussion about the page being stored back in the WriteAheadLogFrame once parsed but it was
> decided that this made little to no difference and should just be retrieved from the commit record.
>
> As a side note, there are some basic things parsed from the page such as the page type.  This is only for
> debugging and logging purposes.

This script holds the following object(s):
-WriteAheadLogFrame(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error and ord in classes.
    ##### WriteAheadLogFrame Class:
    - [ ] Are both the frame index and frame number needed?  Should the "frame" prefix be removed?
    - [ ] Handle exceptions that may be raised from creating the wal frame header.
    - [ ] The contains_sqlite_database_header attribute should apply to table b-trees, not all b-trees.
    - [ ] Document that the root page is not parsed or contained in the frame and why.

<br>

### header.py
This script holds the header objects used for parsing the header of the WAL file and WAL frames.

This script holds the following object(s):
- WriteAheadLogHeader(SQLiteHeader)
- WriteAheadLogFrameHeader(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error and ord in classes.
- [ ] Implement checking of the salt values.
- [ ] Implement checking of checksums in either big/little endian.
- [ ] Investigate if the big/little endian applies to both checksums in the file header and frame header.
- [ ] Create arrays for salt and checksum values rather than separate variables?  They are arrays in the sqlite c code.
    ##### WriteAheadLogHeader Class:
    - [ ] Investigate use cases where the checkpoint != 0.  A warning is thrown currently.

<br>

### utilities.py
This script holds utility functions for dealing with WAL specific objects such as comparing database header rather
than more general utility methods.

This script holds the following function(s):
- compare_database_headers(previous_database_header, new_database_header)
<br><br>

TODO:
- [ ] Documentation improvements.
    ##### compare_database_headers Function:
    - [ ] The \_\_dict\_\_ also returns class objects that may cause issues.

<br>

### wal.py
This script holds the WAL objects used for parsing the WAL file.

This script holds the following object(s):
- WriteAheadLog(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
    ##### WriteAheadLog Class:
    - [ ] Note that this does not extend the version object, instead the commit record does.
    - [ ] Handle exceptions that may be raised from creating the wal frame.
    - [ ] Check the salts and checksums across the frames to the header.
    - [ ] Address the use case of having additional frames past the last committed frame.
    - [ ] Update the commit record number when invalid frames are implemented.
    - [ ] Implement wal files with invalid frames.
    - [ ] Expand on salt 1 and checkpoint referencing documentation and in stringify() functions.
    - [ ] Check the last valid frame index matches that in the wal index file (if found).
    - [ ] Check the database size in pages in the wal index file (if found) against the last commit record.
    - [ ] The file_size arg may not be needed since it is in the file handle and may be removed.
