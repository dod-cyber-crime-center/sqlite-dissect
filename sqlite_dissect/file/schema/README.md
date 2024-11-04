
# sqlite_dissect.file.schema

This package will control parsing and access to the sqlite master schema files.

- column.py
- master.py
- table.py
- utilities.py

TODO items for the "schema" package:

- [ ] Finish UML class diagrams.

<br>

### column.py
This script holds the objects needed for parsing column related objects to the master schema.

This script holds the following object(s):
- ColumnDefinition(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Create variables/constants for regular expressions used?
    ##### ColumnDefinition Class:
    - [ ] Improve the handling of finding and skipping comments.
    - [ ] Handle column constraints correctly.
    - [ ] Address the "(one/\*comment\*/two)" comment use case where sqlite allows this but ignores "two".
    - [ ] Decide if static methods should be moved to a utility class (ie. do they have a reuse need).
    - [ ] When getting the next segment index FOREIGN KEY constraints will cause issues when implemented.
    - [ ] Test where the trim replaced all whitespace removed for segment in else for data types.
    - [ ] Add additional documentation on the "NOT SPECIFIED" being a data type in addition to "INVALID".
    - [ ] Address additional token use cases possibly.
    - [ ] _get_next_segment_ending_index: The specific data type checking is not needed.
    - [ ] _get_next_segment_ending_index: Document that the string should be trimmed.
    - [ ] _get_next_segment_ending_index: Check on constraint strings such as "DEFAULT 0".
    - [ ] _get_column_affinity: Check if this has duplicate functionality to other utility methods.
    ##### ColumnConstraint Class:
    - [ ] Implement comments.
    - [ ] Needs to be implemented.

<br>

### master.py
This script holds the main objects used for parsing the master schema and master schema entries (ie. rows).

This script holds the following object(s):
- MasterSchema(object)
- MasterSchemaRow(object)
- TableRow(MasterSchemaRow)
- OrdinaryTableRow(TableRow)
- VirtualTableRow(TableRow)
- IndexRow(MasterSchemaRow)
- ViewRow(MasterSchemaRow)
- TriggerRow(MasterSchemaRow)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Investigate use cases quotes may be used in sql outside index, table, and column names.
- [ ] Investigate if trigger statements can be used in regards to modifying the master schema.
- [ ] Does it make more sense to have a WithoutRowIdTableRow instead of the OrdinaryTableRow with a flag?
- [ ] All table and index rows should have column definitions of some sort.
- [ ] Create variables/constants for regular expressions used?
    ##### MasterSchema Class:
    - [ ] Rename master_schema_entries just entries?
    - [ ] Check if indexes are created on virtual tables for validation.
    - [ ] Check to make sure every index has an associated table.
    - [ ] Check to make sure every view has associated tables.
    - [ ] Check to make sure trigger has associated tables and/or views.
    - [ ] Validation on the master schema entries such as if indexes exist without any tables defined.
    - [ ] When adding entries to the master schema entries, check if they already exist or not.
    - [ ] Change the master schema entries to be better defined (for example a type keyed dictionary).
    - [ ] Additional validation for the 0 root page use case in master_schema_b_tree_root_page_numbers.
    - [ ] Remove the "master schema" in front of class attributes?
    ##### MasterSchemaRow Class:
    - [ ] Validate use cases of 0 or None for root page in rows (see root_page property).
    - [ ] Implement comments in virtual tables, index, trigger and view rows once implemented.
    - [ ] Address the "(one/*comment*/two)" comment use case where sqlite allows this but ignores "two".
    - [ ] Investigate removal of the sql_has_comments flag.
    - [ ] The row id is incorporated in the identifier and may be able to change upon alter statements.
    - [ ] The root page nomenclature can be confusing since there is a master schema root and b-tree root.
    - [ ] master_schema_b_tree_root_page_numbers: Test with a empty schema.
    ##### TableRow Class:
    - [ ] If a virtual table is found, the database version must be >= 3.8.2.
    ##### OrdinaryTableRow Class:
    - [ ] Provide better "sqlite_" internal schema object support (may not be needed).
    - [ ] Implement parsing of the "AS" use case in the create table statement.
    - [ ] The sql parsing is a bit complicated.  This should be able to be done easier.
    - [ ] During sql parsing use the size of the constraints array to check against instead of a boolean.
    ##### VirtualTableRow Class:
    - [ ] Provide better support for modules and a ModuleArgument class.  Currently a warning is given.
    - [ ] Virtual tables are assumed to always have a root page of 0.  Investigate and enforce this.
    ##### IndexRow Class:
    - [ ] Handle the use case of indexes on table rows that have "without rowid" specified on them.
    - [ ] Implement "sqlite_autoindex_TABLE_N" index internal schema objects.  Currently a warning is given.
    - [ ] Implement parsing of index columns.
    - [ ] Implement partial indexes.
    ##### ViewRow Class:
    - [ ] Implement.
    - [ ] Check tables exist for view information and validation.
    ##### TriggerRow Class:
    - [ ] Implement.
    - [ ] Check tables and views exist for trigger information and validation.

<br>

### table.py
This script holds the objects needed for parsing table related objects to the master schema.

This script holds the following object(s):
- TableConstraint(object)
<br><br>

TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
    ##### TableConstraint Class:
    - [ ] Needs to be implemented.

<br>

### utilities.py
This script holds utility functions for dealing with schema specific objects such as parsing comments from sql rather
than more general utility methods.

This script holds the following function(s):
- get_index_of_closing_parenthesis(string, opening_parenthesis_offset=0)
- parse_comment_from_sql_segment(sql_segment)
<br><br>

TODO:
- [ ] Documentation improvements.
