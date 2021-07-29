
# sqlite_dissect.export

This package will have scripts for writing results from the SQLite carving framework to files such
as csv, sqlite, and so on.

- csv_export.py
- sqlite_export.py
- text_export.py
- xlsx_export.py

TODO items for the "export" package:

- [ ] Finish UML class diagrams.
- [ ] Create a interface/super class that is extended from for exporters in order to simplify interaction with them.
- [ ] Redo the exporters to allow multiple exports instead of having to re-parse the file each time.
- [ ] Incorporate a base export class that takes in a version history and set of exporters.
- [ ] Normalize the inputs of the exporters so that they address postfix and file names similarly (ex. .csv postfix).
- [ ] Check inconsistencies among exporters on overwriting or renaming files (also enter/exit methodology).
- [ ] Investigate pyexcel as a possible alternative to openpyxl for writing xlsx files and possibly csv files.

<br>

### csv_export.py

This script holds the objects used for exporting results of the SQLite carving framework to csv files.

This script holds the following object(s):
- VersionCsvExporter(object)
- CommitCsvExporter(object)
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Better exception handling when working with python and SQLite carving objects.
- [ ] Address superclass/subclass structure.
- [ ] Augment classes to not have to continuously open and close the file (maybe by using the "with" syntax).
- [ ] Work on fixing up column headers and hard coded values in columns.
- [ ] Fix the "column definitions" for names once implemented in b-tree index pages.
- [ ] Use cases if empty tables and no carvable rows which result in no files?
- [ ] Use of "iso_" like in the sqlite_export for internal schema object indexes?
- [ ] Figure out naming conventions (or how to handle) the "Row ID" vs the integer primary key which is NULL.
- [ ] Do not overwrite files but instead move them to a different name as in the SQLite and text exporters?
- [ ] Investigate how other applications handle different database text encodings in reference to output.
- [ ] Investigate decoding and re-encoding affects on carved entries.
- [ ] Handle the "=" use case better than just replacing with a space.
- [ ] Investigate why blob objects show up as isinstance of str objects.
    ##### VersionCsvExporter Class
    - [ ] Check virtual table rows for any use cases that could cause errors when writing.
    - [ ] Address use cases with files, directories, multiple files, etc.
    - [ ] Check if file or directory exists, etc.
    - [ ] Figure out a better way to handle the carved records.
    - [ ] Check the carved records dictionary that all carved records are accounted for.
    - [ ] Fix the carved records once the carving package has been fixed.
    - [ ] Address the located/carved/status status of the entries.
    - [ ] Figure out a better way to calculate absolute offsets in write functions better.
    - [ ] Fix the "Unknown" status of freeblocks and unallocated space carved entries.
    - [ ] Either note or let the user control overwrite/append mode functionality
    - [ ] Handle issues with truncation of carved entries (partial records).
    - [ ] Account for truncated carved entries (status?) and remove NULL for values if truncated.
    - [ ] _write_b_tree_index_leaf_records: Check how index interior/leaf pages work with records.
    ##### CommitCsvExporter Class
    - [ ] _write_cells: Address the use of "NULL" vs None in SQLite for cells.
    - [ ] write_commit: Remove the master schema entry argument?
    - [ ] write_commit: Handle the b-tree table interior page better since it is only for journal files.
  
<br>

### sqlite_export.py

This script holds the objects used for exporting results of the SQLite carving framework to SQLite files.

>Note:
> <br>
> During development this script was written testing and using SQLite version 3.9.2.  The pysqlite version
> was 2.6.0.  Keep in mind that sqlite3.version gives version information on the pysqlite sqlite interface code,
> whereas sqlite3.sqlite_version gives the actual version of the SQLite driver that is used.

This script holds the following object(s):
- CommitSqliteExporter(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Better exception handling when working with python and SQLite carving objects.
- [ ] Implement a version form similar to the VersionCsvExporter.
- [ ] Work on fixing up column headers and hard coded values in columns.
- [ ] Fix the "column definitions" for names once implemented in b-tree index pages.
- [ ] Use cases if empty tables and no carvable rows which result in no files?
- [ ] Investigate differences in efficiency in respect to inserting one or many cells (rows) at a time.
- [ ] Figure out number of columns instead of pulling out the length of each cell over and over again.
- [ ] Empty tables or those with no "updated commits" do not show up in the file.  Should empty tables be created?
- [ ] Create a constant for "iso_" for internal schema object indexes?
- [ ] Figure out naming conventions (or how to handle) the "Row ID" vs the integer primary key which is NULL.
- [ ] Investigate how other applications handle different database text encodings in reference to output.
- [ ] Consolidate documentation information so that it is not repeated. 
    ##### CommitSqliteExporter Class:
    -[ ] _write_cells: Address the use of "NULL" vs None in SQLite for cells.
    -[ ] _write_cells: Address the use case above with the advent of tables with added columns.
    -[ ] _write_cells: Clean up coding of the for loop for writing cell record column values.
    -[ ] _write_cells: Handle the failing "str" encodings instead of just setting in a buffer.
    -[ ] write_commit: Remove the master schema entry argument?
    -[ ] write_commit: Figure out a way to handle additional columns other than a "sd_" preface.
    -[ ] write_commit: Address issues that may be caused by prefacing additional columns with "sd_".

<br>

### text_export.py

This script holds the objects used for exporting results of the SQLite carving framework to text files.

This script holds the following object(s):
- CommitConsoleExporter(object)
- CommitTextExporter(object)
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Better exception handling when working with python and SQLite carving objects.
- [ ] Implement a version form similar to the VersionCsvExporter.
- [ ] Work on fixing up column headers and hard coded values in columns.
- [ ] Fix the "column definitions" for names once implemented in b-tree index pages.
- [ ] Use cases if empty tables and no carvable rows which result in no files?
- [ ] Use of "iso_" like in the sqlite_export for internal schema object indexes?
- [ ] Figure out naming conventions (or how to handle) the "Row ID" vs the integer primary key which is NULL.
- [ ] Investigate how other applications handle different database text encodings in reference to output.
- [ ] Empty tables or those with no "updated commits" do not show up in the file.  Should empty tables be ignored?
    ##### CommitTextExporter Class:
    -[ ] _write_cells: Address the use of "NULL" vs None in SQLite for cells.
    -[ ] write_header: Remove the master schema entry argument?
  
<br>

### xlsx_export.py

This script holds the objects used for exporting results of the SQLite carving framework to xlsx files.

This script holds the following object(s):
- CommitXlsxExporter(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Better exception handling when working with python and SQLite carving objects.
- [ ] Address superclass/subclass structure (the CommitXlsxExporter shares a lot with the CommitCsvExporter).
- [ ] Implement a version form similar to the VersionCsvExporter.
- [ ] Work on fixing up column headers and hard coded values in columns.
- [ ] Fix the "column definitions" for names once implemented in b-tree index pages.
- [ ] Use cases if empty tables and no carvable rows which result in no files?
- [ ] Use of "iso_" like in the sqlite_export for internal schema object indexes?
- [ ] Figure out naming conventions (or how to handle) the "Row ID" vs the integer primary key which is NULL.
- [ ] Investigate decoding and re-encoding affects on carved entries.
- [ ] Investigate how other applications handle different database text encodings in reference to output.
    ##### CommitXlsxExporter Class:
    -[ ] Document and address issues with encoding of unicode.
    -[ ] Document and address issues with the 31 max length sheet names (ie. the max 10 similar names).
    -[ ] write_commit: Remove the master schema entry argument?
    -[ ] _write_cells: Address the use of "NULL" vs None in SQLite for cells.
    -[ ] _write_cells: Handle the "=" use case better than just replacing with a space.
    -[ ] _write_cells: Investigate why blob objects show up as isinstance of str objects.
    -[ ] _write_cells: Check the operation is "Carved" when decoding text values with "replace".
