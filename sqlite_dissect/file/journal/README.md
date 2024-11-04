
# sqlite_dissect.file.journal

This package will control parsing and access to the sqlite journal files.

- header.py
- journal.py

TODO items for the "journal" package:

- [ ] Finish UML class diagrams.

<br>

### header.py
This script holds the header objects for the rollback journal file and page record.

This script holds the following object(s):
- RollbackJournalHeader(SQLiteHeader)
- RollbackJournalPageRecordHeader(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error and ord in classes.
    ##### RollbackJournalHeader Class:
    - [ ] Investigate invalid rollback journal header strings (warning currently raised).
    - [ ] How to handle "zero'd out" headers.
    - [ ] Calling classes should check the auto-vacuum mode in the database header for validity.
    - [ ] Investigate why most headers observed aren't zero padded like sqlite documentation states.
    - [ ] Check if there are use cases of different endianness for journals in sqlite documentation.
    ##### RollbackJournalPageRecordHeader Class:
    - [ ] Needs to be implemented.

<br>

### journal.py
This script holds the class to parse the rollback journal file.

This script holds the following object(s):
- RollbackJournal(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error in classes.
- [ ] Investigate if rollback journals can store data from multiple transactions.
    ##### RollbackJournal Class:
    - [ ] Account for the database text encoding in the file handle.
    - [ ] This class needs to be fully implemented.
    - [ ] Should this be incorporated with the version/version history somehow?
    - [ ] The file_size arg may not be needed since it is in the file handle and may be removed
    - [ ] Implement the stringify method correctly.
