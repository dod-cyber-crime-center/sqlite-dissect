
# sqlite_dissect.file.wal_index

This package will control parsing and access to the SQLite database files.

- database.py
- header.py
- page.py
- payload.py
- utilities.py

TODO items for the "database" package:

- [ ] Finish UML class diagrams.

<br>

### database.py
This script holds the objects used for parsing the database file.

This script holds the following object(s):
- Database(Version)
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Investigate where a database file has empty space beyond the page size (wal checkpoints were set).
    ##### Database Class:
    - [ ] Better exception handling when creating objects such as pages, etc.
    - [ ] Check the use case in regards to a database size in pages of 0 in the header and it's calculated.
    - [ ] Handle where the version valid for number != file change counter (warning currently thrown).
    - [ ] Test out code with a empty database file with no schema (especially the master schema parsing).
    - [ ] More detailed documentation on pages stored in memory.  (Trade offs in speed/memory.)
    - [ ] Check lists and dictionaries for fields before adding.
    - [ ] The file_size arg may not be needed since it is in the file handle and may be removed

<br>

### header.py
This script holds the header objects used for parsing the header of the database file structure from the root page.

This script holds the following object(s):
- DatabaseHeader(SQLiteHeader)
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error and ord in classes.
    ##### DatabaseHeader Class:
    - [ ] Document the database size in pages is going to be 0 if < version 3.7.0 for calling classes.
    - [ ] Investigate why the sqlite version number is 0 in some sqlite files.
    - [ ] Figure a way to determine the number of pages and version number for a suspected empty schema.
    ##### BTreePageHeader Class:
    - [ ] The contains_sqlite_database_header attribute should apply to table b-trees, not all b-trees.
    - [ ] The root_page_only_md5_hex_digest attribute should apply to table b-trees, not all b-trees.
  
<br>

### page.py
This script holds the Page and Cell related objects for parsing out the different types of SQLite pages in the
SQLite database file.  This also includes freeblock and fragment related objects.

This script holds the following object(s):
Page(object)
OverflowPage(Page)
FreelistTrunkPage(Page)
FreelistLeafPage(Page)
PointerMapPage(Page)
PointerMapEntry(object)
BTreePage(Page)
TableInteriorPage(BTreePage)
TableLeafPage(BTreePage)
IndexInteriorPage(BTreePage)
IndexLeafPage(BTreePage)
BTreeCell(object)
TableInteriorCell(BTreeCell)
TableLeafCell(BTreeCell)
IndexInteriorCell(BTreeCell)
IndexLeafCell(BTreeCell)
Freeblock(BTreeCell)
Fragment(BTreeCell)

>Note:  In some places, like with unallocated data on the page, it was decided to not store this data in memory
>       and pull it from the file on demand and/or calculate information from it if needed on demand.  This was done
>       to prevent the memory used by this program becoming bloated with unneeded data.

Assumptions:
1. OverflowPage: All overflow pages are replaced in a chain on modification.  This assumes that whenever a cell is
                  modified, that even if the content of the overflow portion does not change, the whole cell including
                  overflow need to be replaced due to the way the cells are stored in SQLite.
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
- [ ] Finish try/except exception handling for struct.error and ord in classes.
- [ ] Replace version_interface with a more appropriately named variable.
- [ ] Investigate if there is a correct way to enforce class variables to subclasses.
- [ ] Calculation for overflow across the b-tree pages could be pulled out to condense code or for use with carving.
- [ ] Retrieval of cells on demand as well as other fields should be analyzed for better memory handling.
- [ ] Research the documentation on how it says certain things are done with freelists for backwards compatibility.
- [ ] Figure out a better way to read out overflow content on demand in regards to payloads/records.
- [ ] Have a iterator for overflow pages in table leaf and index b-tree pages.
    ##### FreelistTrunkPage Class:
    - [ ] Make sure a freelist trunk page can be updated without updating following freelist pages.
    ##### PointerMapPage Class:
    - [ ] See documentation in class regarding unallocated space in pointer maps that may be carvable.
    ##### TableInteriorPage Class:
    - [ ] Verify that the right-most pointer must always exist.
    ##### IndexInteriorPage Class:
    - [ ] Verify that the right-most pointer must always exist.
    ##### BTreeCell Class:
    - [ ] Cells with payloads do not have overflow calculated in their md5 hash.  Should this be changed?
    - [ ] Rename start_offset to just offset (and in other objects as well)?
    ##### TableInteriorCell Class:
    - [ ] Verify that the left child pointer must always exist.
    ##### IndexInteriorCell Class:
    - [ ] Verify that the left child pointer must always exist.

<br>

### payload.py
This script holds the objects used for parsing payloads from the cells in SQLite b-tree pages for
index leaf, index  interior, and table leaf.  (Table Interior pages do not have payloads in their cells.)

This script holds the following object(s):
Payload(object)
Record(Payload)
RecordColumn(object)
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] Check variables against None and Type constraints, possibly by using descriptors and/or decorators.
    ##### Record Class:
    - [ ] Incorporate absolute offsets.
    - [ ] Use \_\_slots\_\_ or some other way to reduce memory since many of these objects will be created.

<br>

### utilities.py
This script holds utility functions for dealing with database specific objects such as pages rather than more general
utility methods.

This script holds the following function(s):
aggregate_leaf_cells(b_tree_page, accounted_for_cell_md5s=None, records_only=False)
create_pointer_map_pages(version, database_size_in_pages, page_size)
get_maximum_pointer_map_entries_per_page(page_size)
get_page_numbers_and_types_from_b_tree_page(b_tree_page)
get_pages_from_b_tree_page(b_tree_page)
<br><br>
  
TODO:
- [ ] Documentation improvements.
- [ ] aggregate_leaf_cells: Investigate ways of making this faster like with intersections of sets.
- [ ] aggregate_leaf_cells: Check if not using accounted for cell md5s if not specified speeds the function up.
- [ ] aggregate_leaf_cells: Investigate how do index b-tree pages work with fields in interior vs leaf b-tree pages?
- [ ] aggregate_leaf_cells: Account for "without rowid" tables (where they are stored on index b-tree pages).
- [ ] create_pointer_map_pages: Handle exceptions that may occur if the page is not a pointer map page.
- [ ] get_all_pages_from_b_tree_page: Check for duplicates in dictionary when adding?
- [ ] get_page_numbers_and_types_from_b_tree_page: Check for duplicates in dictionary when adding?
