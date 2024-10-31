
# sqlite_dissect.carving

This package will control signature generation and carving of SQLite files.

- carved_cell.py
- carver.py
- rollback_journal_carver.py
- signature.py
- utilities.py

TODO items for the "carving" package:

- [ ] Finish UML class diagrams.

<br>

### carved_cell.py

This script holds the objects used for carving cells from the unallocated and freeblock space in SQLite
b-tree pages used in conjunction with other classes in the carving package.  These objects subclass their
respective higher level SQLite database object type and add to them while parsing the data in a different way.

This script holds the following object(s):
- CarvedBTreeCell(BTreeCell)
- CarvedRecord(Payload)
- CarvedRecordColumn(RecordColumn)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Investigate a way to account for overflow.
- [ ] Investigate if fragments exist, have any affect on carving.
- [ ] Subclass CarvedBTreeCell for b-tree cell types.
- [ ] Subclass CarvedRecord for freeblock and unallocated space algorithms for carving.
- [ ] Handle multi-byte varints (blob and text serial types) better.
- [ ] How to account for use cases where carved data is all 0x00 bytes.
- [ ] Handle use cases where the primary key is an integer and negative resulting in negative (9-byte) varints.
- [ ] Fix the start and end offset and account for the freeblock, freeblock_size, and next_freeblock_offset.
- [ ] For the first serial types need to cross reference first column if integer primary key in table b-tree leaf table == null 00
- [ ] For the first serial types need to cross reference with row signatures (if not schema) (prob + focued + schema + first removed on unalloc etc)
- [ ] Address the row_id as being set initially to "Unknown" which was temporarily added for consistence with other cells (b-tree) and need to check other use cases.
- [ ] Check that the payload size is less than the length or else partial entry.
- [ ] Add better logging.
- [ ] Calculate or analyze MD5s of headers.
- [ ] Figure out how MD5 hashes will work on carved record, carved record columns, and carved b-tree cells.
- [ ] Look into the calculated body content size assuming one (correct) entry in the signature.
- [ ] Address header/body/etc byte sizes.
- [ ] Check size of record columns to expected columns.
    ##### CarvedBTreeCell Class
    - [ ] Remove the first column serial types now that the signature is sent in?
    - [ ] handle the version and page version number correctly in reference to journal file parsing.
    ##### CarvedRecord Class
      - [ ] See if basing the first_serial_type off of other carved cells if found before redoing unallocated/freeblocks if possible.
    - [ ] When checking the signature, see if is there a better way to utilize it if there are no entries like switching to the schema signature (b-tree leaf?).
    - [ ] Address the truncated record column index/column name.
    - [ ] Handle cutoff_offset relation to truncated and indexing.
    - [ ] Handle overflow.
    - [ ] Fragment parsing.
    - [ ] Subclass types of cells, freeblock.
    - [ ] What if the assumed preceding serial type is not in the first serial types sign (use prob?).
    - [ ] Address issues that can occur when first_serial_type_varint_length != -1.
    - [ ] Need documentation on how the serial type is always obtainable for freeblocks at least only if the next two bytes != size (ie. sub freeblock) if the start offset >= 2 and it is a freeblock.
    - [ ] Check the equals (>= and <) for start offset >= 2 and is a freeblock while iterating through the carved record columns.
    - [ ] Update debugging messages (for example, after except like with InvalidVarIntError)
    - [ ] If string or blob may be able to iterate backwards until proper offsets are found and look into other use cases.
    - [ ] Document use cases for first_column_serial_types (4?).
    - [ ] Report size of missing data/columns/etc if truncated for carved_record_column objects.
    - [ ] Look into sending unallocated byte size in the constructor for carved_record_column objects.
    - [ ] Specify if the unallocated information is included or overwritten in the header for carved_record_column objects.
    - [ ] Document after adjusting the serial type definition size off of the first serial type specified for carved_record_column objects.
    - [ ] Need documentation on the "32" number [ (9 - 4) + 9 + 9 + 9 ] = up to 32 bytes preceding (derived header byte size).
    - [ ] Using the simplified_probabilistic_signature can give bad data.
    - [ ] Fix when the serial type is 12 or 13.  If the signatures is -1 or -2 should be 0->57 (min/max).
    - [ ] Try doing a reverse search for row id and payload length (assuming 1 varint length for row id).
    - [ ] Derive differences between derived payload and actual payload if actual is not found (and other fields).
    - [ ] Need to reverse search for row id and payload length (assuming 1 varint length for row id).
    ##### CarvedRecordColumn Class
    - [ ] Incorporate absolute offsets.
    - [ ] Calculate and set the md5 hex digest.
    - [ ] Handle the value and md5 hex digest (and probably others) so values are sent into \_\_init\_\_?
    - [ ] Handle table interior, index interior, index leaf, and additional use cases.
    - [ ] Make sure string values are in the correct text encoding for the database.
    - [ ] Use \_\_slots\_\_ or some other way to reduce memory since many of these objects will be created.
    - [ ] Update documentation around the no bytes preceding note.

<br>

### carver.py

This script holds carver objects for identifying and parsing out cells from unallocated and
freeblock space in SQLite b-tree pages.

This script holds the following object(s):
- SignatureCarver(Carver)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] On some files (ex. talk.sqlite), lots of "empty space" signatures were printed.  Fix these use cases.
- [ ] Account for changing schemas (schema cookie, etc.).
- [ ] Investigate if there is a way to handle fragments (fragment "sizes" can be > 3).
- [ ] Better handling of errors thrown while generating carved cells.
- [ ] Handle use cases where the primary key is an integer and negative resulting in negative (9-byte) varints.
- [ ] Investigate if there is any need to account for different database encodings.
    ##### SignatureCarver Class
    - [ ] Incorporate altered tables within the signature in carving, not just the full signature.
    - [ ] Address overflow.
    - [ ] Specify which signatures to carve with (if important or schema vs simplified)?
    - [ ] Currently matches are done in reverse for better findings.  Should this also be done in order?
    - [ ] Update the cutoff offset based on the earliest offset found in the carved b-tree cell.
    - [ ] Remove the cutoff offset by sending in a truncated data array in to the CarvedBTreeCell?
    - [ ] Change the first column serial types from an array to boolean since signature is now sent in.
    - [ ] carve_freeblocks: Handle use cases where the first serial type in the record header exists.
    - [ ] carve_freeblocks: Check why originally there was an exception if the first serial types > 1.
    - [ ] carve_freeblocks: Handle multi-byte varints in the first serial types (warning currently raised).
    - [ ] carve_freeblocks: Apply additional use cases to the use of the cutoff offset.
    - [ ] carve_freeblocks: Check why search was used if len(signature) == 2 and -1/-2 in signature\[1\].
    - [ ] carve_unallocated_space: Address carving of the cell pointer array for deleted cells.
    - [ ] carve_unallocated_space: Handle carving of freeblocks (see documentation in section of code).
    - [ ] carve_unallocated_space: Handle varint first serial type (see documentation in section of code).
    - [ ] carve_unallocated_space: Support for other cell types than b-tree table leaf cells.
    - [ ] carve_unallocated_space: Address parsing of fields such as payload size, row id, etc.
    - [ ] carve_unallocated_space: Update partial carving indices (see documentation in section of code).
    - [ ] carve_unallocated_space: Have an option for partial/freeblock carving of unallocated space?
    - [ ] carve_unallocated_space: Revise the partial carving algorithm.

<br>

### rollback_journal_carver.py

This script carves through a journal file with the specified master schema entry and signature and returns the entries.

This script holds the following object(s):
- RollBackJournalCarver(Carver)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Investigate possible alternatives to computing or reading the database page size from the journal file.

<br>

### signature.py

This script holds the objects for the signature generation of SQLite table and index b-trees for carving.

This script holds the following object(s):
- Signature(VersionParser)
- SchemaColumnSignature(object)
- TableColumnSignature(object)
- TableRowSignature(object)
- ColumnSignature(object)
- ColumnFixedLengthSignature(ColumnSignature)
- ColumnVariableLengthSignature(ColumnSignature)
- ColumnReducedVariableLengthSignature(ColumnVariableLengthSignature)
- ColumnNonReducedVariableLengthSignature(ColumnVariableLengthSignature)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Handle exceptions that may be raised from creating and working with objects such as signatures better.
- [ ] Incorporate any column and/or table constraint use cases that may affect the signature.
- [ ] Create superclass for schema, table row, and table column signatures?
- [ ] Create constants for serial type arrays in signatures?
- [ ] Updated signature classes to take in a column signature argument instead of sending in individual fields of it.
- [ ] Right now signatures are only derived from leaf pages.  Interior pages should be have signatures as well.
- [ ] Have a way to send in a maximum amount of (unique) records to generate the signature from (reduces time)?
- [ ] Have an extension to the Epilog XSD that can be used for signature exportation.
- [ ] Have a way to merge like signatures from external files.
- [ ] Investigate if it is better to put the altered columns flag in a master schema associated class or leave here?
    ##### Signature Class
    - [ ] Create a field that has a max number of rows to look at to determine a signature to reduce time?
    - [ ] Test and investigation on how to handle virtual tables with signatures.
    - [ ] Note on how table interior pages cannot have (serial type header) signatures since no records exist.
    - [ ] Change the signature to take in a master schema entry identifier instead of the entry itself?
    - [ ] Signatures need to be made for the master schema pages.
    - [ ] Check support for index b-tree pages and ensure it is working correctly (warning currently raised).
    - [ ] The accounted_for_cell_digests may not work for index pages since there is no row id.
    - [ ] There may not be a page type in reference to a virtual table since it is not required to have pages.
    - [ ] Support for virtual table modules of master schema entry table type.
    - [ ] Support for index b-trees that are internal schema objects with no SQL (warning currently raised).
    - [ ] Check to make sure index b-tree internal schema objects can not have column definitions (SQL).
    - [ ] How do 0 serial types (NULL) work with signatures (like epilog signatures)?
    - [ ] Combines simple (or focused) and schema epilog signatures for a more complete epilog signature?
    - [ ] Check 8 and 9 serial type on non-integer storage classes for simplified and focused epilog signatures.
    - [ ] Is there a use case for only parsing the schema signature and nothing else?
    - [ ] How to handle master schema entries not found in specified versions?
    - [ ] Have a b-tree page type (either table or index).
    - [ ] Investigate better ways for probability calculations between altered columns and column breakdown.
    - [ ] How does defaulting fields work in reference to virtual tables.  How does is the signature generated?
      ##### SchemaColumnSignature Class
    - [ ] Handle NULL serial types in the recommended signatures.
    - [ ] Incorporate NOT NULL column constraints (and other uses - primary key?) as not having a 0.

<br>

### utilities.py

This script holds carving utility functions for reference by the SQLite carving module.

This script holds the following object(s):
- decode_varint_in_reverse(byte_array, offset)
- calculate_body_content_size(serial_type_header)
- calculate_serial_type_definition_content_length_min_max(simplified_serial_types, allowed_varint_length=5)
- calculate_serial_type_varint_length_min_max(simplified_serial_types)
- generate_regex_for_simplified_serial_type(simplified_serial_type)
- generate_signature_regex(signature, skip_first_serial_type=False)
- get_content_size(serial_type)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Handle use cases where the primary key is an integer and negative resulting in negative (9-byte) varints.
- [ ] decode_varint_in_reverse: Handle the 9 byte varints correctly.
- [ ] decode_varint_in_reverse: Should the InvalidVarIntError be logged as an error?
- [ ] decode_varint_in_reverse: Document on how conclusiveness/truncation can not be certain.
- [ ] generate_regex_for_simplified_serial_type: Fix to account for 9 byte varint serial types.
- [ ] generate_signature_regex: Account for small signatures.
- [ ] generate_signature_regex: Account for regular expressions that skip the first byte of a multi-byte serial type.
