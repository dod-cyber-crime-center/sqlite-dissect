
# sqlite_dissect

This package will have scripts for overall usage throughout the SQLite Dissect library allowing the functionality
to parse through the data and access to underlying functions through an interface.

The init script will initialize the logger for this library with a NullHandler to prevent unexpected output
from applications that may not be implementing logging.  It will also ignore warnings reported by the python
warning by default.  (Warnings are also thrown to the logger when they occur in addition to the warnings
framework.)

>Note:  This library will use warnings for things that may not be fully implemented or handled yet.  (In other cases,
>       NotImplementedErrors may be raised.)  To turn off warnings use the "-W ignore" option.  See the Python
>       documentation for further options.

- constants.py
- exception.py
- interface.py
- output.py
- utilities.py
- version_history.py

TODO items for the "sqlite_dissect" package:

- [ ] Finish UML class diagrams.
- [ ] \_\_init\_\_.py: Create a raise exception function to call to reduce lines of code that will log inside of it.
- [ ] \_\_init\_\_.py: Create global static variables to be used for store_in_memory, strict_format_checking, etc.
- [ ] \_\_init\_\_.py: Implement strict_format_checking into journal, other types besides database, wal
- [ ] \_\_init\_\_.py: Investigate differences in use of logging.warn vs. warning.warn.
- [ ] \_\_init\_\_.py: Create custom warnings for the library.

<br>

### constants.py

This script holds constants defined for reference by the sqlite carving library.  Additionally, a class has been
added to this script for constant enumerations.

This script holds the following object(s):
- Enum(MutableMapping)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.

<br>

### exception.py

This script holds the custom exceptions used in this library.

This script holds the following object(s):
- SqliteError(Exception)
- ParsingError(SqliteError)
- HeaderParsingError(ParsingError)
- MasterSchemaParsingError(ParsingError)
- MasterSchemaRowParsingError(MasterSchemaParsingError)
- PageParsingError(ParsingError)
- BTreePageParsingError(PageParsingError)
- CellParsingError(BTreePageParsingError)
- RecordParsingError(CellParsingError)
- VersionParsingError(ParsingError)
- DatabaseParsingError(VersionParsingError)
- WalParsingError(VersionParsingError)
- WalFrameParsingError(WalParsingError)
- WalCommitRecordParsingError(WalParsingError)
- SignatureError(SqliteError)
- CarvingError(SqliteError)
- CellCarvingError(CarvingError)
- InvalidVarIntError(CarvingError)
- OutputError(SqliteError)
- ExportError(SqliteError)
<br><br>

TODO:
- [ ] Documentation improvements.

<br>

### interface.py

This script acts as a simplified interface for common operations for the sqlite carving library.

This script holds the following object(s):
- create_database(file_identifier, store_in_memory=False, strict_format_checking=True)
- create_write_ahead_log(file_name, file_object=None)
- create_version_history(database, write_ahead_log=None)
- get_table_names(database)
- get_index_names(database)
- select_all_from_table(table_name, version)
- select_all_from_index(index_name, version)
- create_table_signature(table_name, version, version_history=None)
- carve_table(table_name, signature, version)
- get_version_history_iterator(table_or_index_name, version_history, signature=None)
- export_table_or_index_version_history_to_csv(export_directory, version_history, table_or_index_name, signature=None, carve_freelist_pages=False)
- export_version_history_to_csv(export_directory, version_history, signatures=None, carve_freelist_pages=False)
- export_table_or_index_version_history_to_sqlite(export_directory, sqlite_file_name, version_history, table_or_index_name, signature=None, carve_freelist_pages=False):
- export_version_history_to_sqlite(export_directory, sqlite_file_name, version_history, signatures=None, carve_freelist_pages=False):
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Account for schema changes across the versions.
- [ ] Implement index signatures.
- [ ] Update documentation on the BASE_VERSION_NUMBER where it is used.
- [ ] create_table_signature: Note on how the version history is recommended if possible.

<br>

### output.py

This script holds general output functions used for debugging, logging, and general output for the
sqlite carving library.

This script holds the following object(s):
- get_page_breakdown(pages)
- get_pointer_map_entries_breakdown(version)
- stringify_b_tree(version_interface, b_tree_root_page, padding="")
- stringify_cell_record(cell, database_text_encoding, page_type)
- stringify_cell_records(cells, database_text_encoding, page_type)
- stringify_master_schema_version(version)
- stringify_master_schema_versions(version_history)
- stringify_page_history(version_history, padding="")
- stringify_page_information(version, padding="")
- stringify_page_structure(version, padding="")
- stringify_unallocated_space(version, padding="", include_whitespace=True, whitespace_threshold=0)
- stringify_version_pages(version, padding="")
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Implement better exception handling when parsing objects.
- [ ] Make sure different encodings are handled in every function in this script where applicable.
- [ ] get_pointer_map_entries_breakdown: Handle the pointer map page breakdown tuple better.
- [ ] stringify_unallocated_space: Implement a whitespace threshold for trimming, etc.

<br>

### utilities.py

This script holds general utility functions for reference by the sqlite carving library.

This script holds the following object(s):
- calculate_expected_overflow(overflow_byte_size, page_size)
- decode_varint(byte_array, offset)
- encode_varint(value)
- get_class_instance(class_name)
- get_md5_hash(string)
- get_record_content(serial_type, record_body, offset=0)
- get_serial_type_signature(serial_type)
- has_content(byte_array)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Implement try/except exception handling for struct.error and ord.
- [ ] The varint related functions only work in big endian.  Are there use cases for little endian?

<br>

### version_history.py

This script holds the superclass objects used for parsing the database and write ahead log in a sequence of versions
throughout all of the commit records in the write ahead log.

This script holds the following object(s):
- VersionHistory(object)
- VersionHistoryParser(VersionParser) (with VersionHistoryParserIterator(object) as an inner class)
- Commit(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Incorporate journal files once they are implemented.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Handle exceptions that may be raised from creating and working with objects better.
    ##### VersionHistory Class:
    - [ ] Better exception handling when creating objects such as commit records, etc.
    - [ ] Investigate what occurs if the last commit record is not committed (warning currently thrown).
    ##### VersionHistoryParser Class:
    - [ ] Support the same master schema entry being deleted and then re-added (Keep in mind row id).
    - [ ] How to handle master schema entries not found in specified versions?
    - [ ] Support for virtual table modules of master schema entry table type.
    - [ ] Support for "without rowid" tables (index b-tree pages).
    - [ ] Support for index b-trees that are internal schema objects with no SQL.
    - [ ] Investigate issues with same rows in index b-tree leaf pages that might get deleted.
    - [ ] Track pages being moved to the freelist to account for carving with other signatures?
    - [ ] Handle master schema entries that have no entries (view, trigger, etc.) in the iterator.
    - [ ] Handle master schema entries that are not supported yet (virtual, etc.) in the iterator.
    - [ ] Use accounted for cell digests for deleted cells in the aggregate leaf cells function?
    - [ ] How to detect index leaf page cell updates (file offset may not work and no row id).
    - [ ] Is checking on the row id sufficient for detecting updates on table leaf pages for cells.
    - [ ] Does this class belong here and should carving be incorporated or separate to this class?
    - [ ] Have a better way to specify if carving was enabled or not (possibly in Commit?).
    - [ ] VersionParserIterator: Investigate what to return for version with no modification.
    - [ ] VersionParserIterator: Extend carving capabilities beyond tables once implemented.
    - [ ] VersionParserIterator: Check carvings are correctly being detected as duplicates per md5.
    - [ ] VersionParserIterator: Use dictionary comprehension for added and deleted cells for loops.
    ##### Commit Class:
    - [ ] Handle the updated property differently depending on differences in b-tree and freelist changes.
