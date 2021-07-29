from getopt import getopt
from logging import WARNING
from logging import basicConfig
from os import makedirs
from os.path import basename
from os.path import exists
from os.path import normpath
from os.path import sep
from re import sub
from sys import argv
from sqlite_dissect.carving.carver import SignatureCarver
from sqlite_dissect.carving.signature import Signature
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.constants import CELL_LOCATION
from sqlite_dissect.constants import CELL_SOURCE
from sqlite_dissect.constants import EXPORT_TYPES
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import ROLLBACK_JOURNAL_POSTFIX
from sqlite_dissect.constants import WAL_FILE_POSTFIX
from sqlite_dissect.constants import WAL_INDEX_POSTFIX
from sqlite_dissect.export.csv_export import CommitCsvExporter
from sqlite_dissect.file.database.database import Database
from sqlite_dissect.file.database.page import BTreePage
from sqlite_dissect.file.database.utilities import get_pages_from_b_tree_page
from sqlite_dissect.file.journal.jounal import RollbackJournal
from sqlite_dissect.file.schema.master import OrdinaryTableRow
from sqlite_dissect.file.schema.master import VirtualTableRow
from sqlite_dissect.file.utilities import validate_page_version_history
from sqlite_dissect.file.wal.wal import WriteAheadLog
from sqlite_dissect.file.wal_index.wal_index import WriteAheadLogIndex
from sqlite_dissect.interface import carve_table
from sqlite_dissect.interface import create_database
from sqlite_dissect.interface import create_table_signature
from sqlite_dissect.interface import create_version_history
from sqlite_dissect.interface import create_write_ahead_log
from sqlite_dissect.interface import export_table_or_index_version_history_to_csv
from sqlite_dissect.interface import export_table_or_index_version_history_to_sqlite
from sqlite_dissect.interface import export_version_history_to_csv
from sqlite_dissect.interface import export_version_history_to_sqlite
from sqlite_dissect.interface import get_index_names
from sqlite_dissect.interface import get_table_names
from sqlite_dissect.interface import get_version_history_iterator
from sqlite_dissect.interface import select_all_from_index
from sqlite_dissect.interface import select_all_from_table
from sqlite_dissect.output import stringify_cell_records
from sqlite_dissect.output import stringify_master_schema_versions
from sqlite_dissect.output import stringify_page_information
from sqlite_dissect.output import stringify_unallocated_space
from sqlite_dissect.version_history import VersionHistory
from sqlite_dissect.version_history import VersionHistoryParser

"""

example.py

This script shows examples of how this library can be used.

"""

# Setup logging
logging_level = WARNING
logging_format = '%(levelname)s %(asctime)s [%(pathname)s] %(funcName)s at line %(lineno)d: %(message)s'
logging_data_format = '%d %b %Y %H:%M:%S'
basicConfig(level=logging_level, format=logging_format, datefmt=logging_data_format)

file_name = None
export_directory = None
export_type = None
opts, args = getopt(argv[1:], "f:e:t:")
for opt, arg in opts:
    if opt == "-f":
        file_name = arg
    elif opt == "-e":
        export_directory = arg
    elif opt == "-t":
        export_type = arg

"""

Note:  Currently only the csv export_type is supported in this example.  The csv and sqlite export_types are used in
       the API example below.  Other specified types are currently ignored.

"""

if (export_directory and not export_type) or (not export_directory and export_type):
    print("The export directory (-e) and export type (-t) both need to be defined if either one is specified.")
    print("Export types are: {}.".format([export_type for export_type in EXPORT_TYPES]))
    exit(1)

if export_type and export_type.upper() not in EXPORT_TYPES:
    print("Invalid export type: {}.".format(export_type))
    print("Export types are: {}.".format(",".join([export_type.lower() for export_type in EXPORT_TYPES])))
    exit(1)

if not file_name:
    print("Please execute the application specifying the file name.")
    exit(1)
elif not exists(file_name):
    print("File: {} does not exist.".format(file_name))
    exit(1)
else:
    print("Starting to parse and carve: {}.\n".format(file_name))

file_prefix = basename(normpath(file_name))
padding = "\t"

"""

Load the Database File.

"""

database_file = Database(file_name)
print("Database File:\n{}\n".format(database_file.stringify(padding, False, False)))
print("Page Information:\n{}\n".format(stringify_page_information(database_file, padding)))

"""

Check if the Write-Ahead Log File exists and load it if it does.

"""

wal_file = None
wal_file_name = file_name + WAL_FILE_POSTFIX
if exists(wal_file_name):
    wal_file = WriteAheadLog(wal_file_name)
    print("WAL File:\n{}\n".format(wal_file.stringify(padding, False)))
else:
    print("No WAL File Found.\n")

"""

Check if the Write-Ahead Log Index File exists and load it if it does.

"""

wal_index_file = None
wal_index_file_name = file_name + WAL_INDEX_POSTFIX
if exists(wal_index_file_name):
    wal_index_file = WriteAheadLogIndex(wal_index_file_name)
    print("WAL Index File:\n{}\n".format(wal_index_file.stringify(padding)))
else:
    print("No WAL Index File Found.\n")

"""

Check if the Rollback Journal File exists and load it if it does.

"""

rollback_journal_file = None
rollback_journal_file_name = file_name + ROLLBACK_JOURNAL_POSTFIX
if exists(rollback_journal_file_name):
    rollback_journal_file = RollbackJournal(rollback_journal_file_name)
    print("Rollback Journal File:\n{}\n".format(rollback_journal_file.stringify(padding)))
else:
    print("No Rollback Journal File Found.\n")

"""

Print Unallocated Non-Zero Space from the Database File.

"""

unallocated_non_zero_space = stringify_unallocated_space(database_file, padding, False)
print("Unallocated Non-Zero Space from the Database File:\n{}\n".format(unallocated_non_zero_space))

"""

Create the version history from the database and WAL file (even if the WAL file was not found).

"""

version_history = VersionHistory(database_file, wal_file)

print("Number of versions: {}\n".format(version_history.number_of_versions))

print("Validating Page Version History...")
page_version_history_validated = validate_page_version_history(version_history)
print("Validating Page Version History (Check): {}\n".format(page_version_history_validated))
if not page_version_history_validated:
    print("Error in validating page version history.")
    exit(1)

print("Version History of Master Schemas:\n")
for version_number, version in version_history.versions.iteritems():
    if version.master_schema_modified:
        master_schema_entries = version.master_schema.master_schema_entries
        if master_schema_entries:
            print("Version {} Master Schema Entries:".format(version_number))
            for master_schema_entry in master_schema_entries:
                string = padding + "Master Schema Entry: Root Page Number: {} Type: {} Name: {} " \
                                   "Table Name: {} SQL: {}."
                print(string.format(master_schema_entry.root_page_number, master_schema_entry.row_type,
                                    master_schema_entry.name, master_schema_entry.table_name,
                                    master_schema_entry.sql))

print("Version History:\n")
for version_number, version in version_history.versions.iteritems():
    print("Version: {} has updated page numbers: {}.".format(version_number, version.updated_page_numbers))
    print("Page Information:\n{}\n".format(stringify_page_information(version, padding)))

last_version = version_history.number_of_versions - 1
print("Version: {} has updated page numbers: {}.".format(version_history.number_of_versions - 1,
                                                         last_version.updated_page_numbers))
print("Page Information:\n{}\n".format(stringify_page_information(last_version, padding)))

print("Version History of Master Schemas:\n{}\n".format(stringify_master_schema_versions(version_history)))

print("Master Schema B-Trees (Index and Table) Version Histories:")
for master_schema_entry in database_file.master_schema.master_schema_entries:
    if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE] and \
            not isinstance(master_schema_entry, VirtualTableRow) and \
            not (isinstance(master_schema_entry, OrdinaryTableRow) and master_schema_entry.without_row_id):
        version_history_parser = VersionHistoryParser(version_history, master_schema_entry)
        page_type = version_history_parser.page_type
        string = "Master schema entry: {} type: {} on page type: {}:"
        string = string.format(version_history_parser.row_type, master_schema_entry.name, page_type,
                               version_history_parser.root_page_number_version_index)

        print(string)
        for commit in version_history_parser:
            if commit.updated:
                string = "Updated in version: {} with root page number: {} on b-tree page numbers: {} " \
                         "and updated root b-tree page numbers: {}:"
                string = string.format(commit.version_number, commit.root_page_number, commit.b_tree_page_numbers,
                                       commit.updated_b_tree_page_numbers)
                print(string)
                for added_cell_string in stringify_cell_records(commit.added_cells.values(),
                                                                database_file.database_text_encoding, page_type):
                    print("Added: {}".format(added_cell_string))
                for updated_cell_string in stringify_cell_records(commit.updated_cells.values(),
                                                                  database_file.database_text_encoding, page_type):
                    print("Updated: {}".format(updated_cell_string))
                for deleted_cell_string in stringify_cell_records(commit.deleted_cells.values(),
                                                                  database_file.database_text_encoding, page_type):
                    print("Deleted: {}".format(deleted_cell_string))
                for carved_cell_string in stringify_cell_records(commit.carved_cells.values(),
                                                                 database_file.database_text_encoding, page_type):
                    print("Carved: {}".format(carved_cell_string))
        print("\n")

signatures = {}
for master_schema_entry in database_file.master_schema.master_schema_entries:

    """

    Due to current implementation limitations we are restricting signature generation to table row types.

    """

    if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.TABLE:
        signature = Signature(version_history, master_schema_entry)
        signatures[master_schema_entry.name] = signature
        print("Signature:\n{}\n".format(signature.stringify(padding + "\t", False, False, False)))
    else:
        string = "No signature will be generated for master schema entry type: {} with name: {} on " \
                 "table name: {} and sql: {}"
        string = string.format(master_schema_entry.row_type, master_schema_entry.name, master_schema_entry.table_name,
                               master_schema_entry.sql)
        print(string + "\n")

print("Carving base version (main SQLite database file):")
version = version_history.versions[BASE_VERSION_NUMBER]

carved_records = {}
for master_schema_entry in database_file.master_schema.master_schema_entries:

    """

    Due to current implementation limitations we are restricting carving to table row types.

    Note:  This is not allowing "without rowid" or virtual tables until further testing is done. (Virtual tables
           tend to have a root page number of 0 with no data stored in the main table.  Further investigation 
           is needed.)

    """

    if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.TABLE \
       and not isinstance(master_schema_entry, VirtualTableRow) and not master_schema_entry.without_row_id:

        b_tree_pages = get_pages_from_b_tree_page(version.get_b_tree_root_page(master_schema_entry.root_page_number))
        b_tree_page_numbers = [b_tree_page.number for b_tree_page in b_tree_pages]

        string = "Carving Table Entry: Name: {} root page: {} on page numbers: {}"
        print(string.format(master_schema_entry.name, master_schema_entry.root_page_number, b_tree_page_numbers))

        carved_records[master_schema_entry.name] = []
        for b_tree_page_number in b_tree_page_numbers:
            page = database_file.pages[b_tree_page_number]
            source = CELL_SOURCE.B_TREE

            #  For carving freeblocks make sure the page is a b-tree page and not overflow
            if isinstance(page, BTreePage):
                carved_cells = SignatureCarver.carve_freeblocks(version, source, page.freeblocks,
                                                                signatures[master_schema_entry.name])
                carved_records[master_schema_entry.name].extend(carved_cells)
            carved_cells = SignatureCarver.carve_unallocated_space(version, source, b_tree_page_number,
                                                                   page.unallocated_space_start_offset,
                                                                   page.unallocated_space,
                                                                   signatures[master_schema_entry.name])

            carved_records[master_schema_entry.name].extend(carved_cells)

    else:
        string = "Not carving master schema entry row type: {} name: {} table name: {} and sql: {} since it is not " \
                 "a normal table."
        string = string.format(master_schema_entry.row_type, master_schema_entry.name, master_schema_entry.table_name,
                               master_schema_entry.sql)
        print(string)
print("\n")

print("Carved Entries:\n")
for master_schema_entry_name, carved_cells in carved_records.iteritems():

    print("Table Master Schema Entry Name {}:".format(master_schema_entry_name))

    carved_freeblock_records_total = len([carved_cell for carved_cell in carved_cells
                                          if carved_cell.location == CELL_LOCATION.FREEBLOCK])

    print("Recovered {} entries from freeblocks:".format(carved_freeblock_records_total))

    for carved_cell in carved_cells:
        if carved_cell.location == CELL_LOCATION.FREEBLOCK:
            payload = carved_cell.payload
            cell_record_column_values = [str(record_column.value) if record_column.value else "NULL"
                                         for record_column in payload.record_columns]
            string = "{}: {} Index: ({}, {}, {}, {}): ({})"
            string = string.format(carved_cell.page_number, carved_cell.index, carved_cell.file_offset,
                                   payload.serial_type_definition_start_offset,
                                   payload.serial_type_definition_end_offset,
                                   payload.cutoff_offset, " , ".join(cell_record_column_values))
            print(string)

    carved_unallocated_space_records_total = len([carved_cell for carved_cell in carved_cells
                                                  if carved_cell.location == CELL_LOCATION.UNALLOCATED_SPACE])
    print("Recovered {} entries from unallocated space:".format(carved_unallocated_space_records_total))

    for carved_cell in carved_cells:
        if carved_cell.location == CELL_LOCATION.UNALLOCATED_SPACE:
            payload = carved_cell.payload
            cell_record_column_values = [str(record_column.value) if record_column.value else "NULL"
                                         for record_column in payload.record_columns]
            string = "{}: {} Index: ({}, {}, {}, {}): ({})"
            string = string.format(carved_cell.page_number, carved_cell.index, carved_cell.file_offset,
                                   payload.serial_type_definition_start_offset,
                                   payload.serial_type_definition_end_offset,
                                   payload.cutoff_offset, " , ".join(cell_record_column_values))
            print(string)

    print("\n")
print("\n")

print("Master Schema B-Trees (Index and Table) Version Histories Including Carvings:")
for master_schema_entry in database_file.master_schema.master_schema_entries:
    if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

        # We only have signatures of the tables (not indexes)
        signature = signatures[master_schema_entry.name] \
            if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.TABLE else None

        version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None, signature)
        page_type = version_history_parser.page_type
        string = "Master schema entry: {} type: {} on page type: {}:"
        string = string.format(master_schema_entry.name, version_history_parser.row_type, page_type,
                               version_history_parser.root_page_number_version_index)
        print(string)
        for commit in version_history_parser:
            if commit.updated:
                string = "Updated in version: {} with root page number: {} on b-tree page numbers: {} " \
                         "and updated root b-tree page numbers: {}:"
                string = string.format(commit.version_number, commit.root_page_number, commit.b_tree_page_numbers,
                                       commit.updated_b_tree_page_numbers)
                print(string)
                for added_cell_string in stringify_cell_records(commit.added_cells.values(),
                                                                database_file.database_text_encoding, page_type):
                    print("Added: {}".format(added_cell_string))
                for updated_cell_string in stringify_cell_records(commit.updated_cells.values(),
                                                                  database_file.database_text_encoding, page_type):
                    print("Updated: {}".format(updated_cell_string))
                for deleted_cell_string in stringify_cell_records(commit.deleted_cells.values(),
                                                                  database_file.database_text_encoding, page_type):
                    print("Deleted: {}".format(deleted_cell_string))
                for carved_cell_string in stringify_cell_records(commit.carved_cells.values(),
                                                                 database_file.database_text_encoding, page_type):
                    print("Carved: {}".format(carved_cell_string))
        print("\n")

if export_type and export_type.upper() == EXPORT_TYPES.CSV:
    csv_prefix_file_name = basename(normpath(file_prefix))
    commit_csv_exporter = CommitCsvExporter(export_directory, csv_prefix_file_name)
    print("Exporting SQLite Master Schema B-Trees (Index and Table) Version Histories "
          "(Including Carvings) to CSV Directory: {}.".format(export_directory))
    for master_schema_entry in database_file.master_schema.master_schema_entries:
        if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:

            # We only have signatures of the tables (not indexes)
            signature = signatures[master_schema_entry.name] \
                if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.TABLE else None

            carve_freelist_pages = True if signature else False

            version_history_parser = VersionHistoryParser(version_history, master_schema_entry,
                                                          None, None, signature, carve_freelist_pages)
            page_type = version_history_parser.page_type
            for commit in version_history_parser:
                commit_csv_exporter.write_commit(master_schema_entry, commit)
print("\n")

"""

Below are examples on using the interface.

The functions used from the interface script are documented below (taken from documentation in the interface script):
create_database(file_name, file_object=None, store_in_memory=False, strict_format_checking=True)
create_write_ahead_log(file_name, file_object=None)
create_version_history(database, write_ahead_log=None)
get_table_names(database)
get_index_names(database)
select_all_from_table(table_name, version)
select_all_from_index(index_name, version)
create_table_signature(table_name, version, version_history=None)
carve_table(table_name, signature, version)
get_version_history_iterator(table_or_index_name, version_history, signature=None)
export_table_or_index_version_history_to_csv(export_directory, version_history,
                                             table_or_index_name, signature=None, carve_freelist_pages=False)
export_version_history_to_csv(export_directory, version_history, signatures=None, carve_freelist_pages=False)

"""

print("Example interface usage:\n")

# Create the database
database = create_database(file_name)

# Create the write ahead log
write_ahead_log = create_write_ahead_log(file_name + WAL_FILE_POSTFIX) if exists(file_name + WAL_FILE_POSTFIX) else None

# Create the version history
version_history = create_version_history(database, write_ahead_log)

# Get all of the table names
table_names = get_table_names(database)
print("Table Names: {}\n".format(table_names))

# Get all of the cells in each table and print the number of cells (rows) for each table
for table_name in table_names:
    select_all_data = select_all_from_table(table_name, database)
    print("Table: {} has {} rows in the database file.".format(table_name, len(select_all_data)))
print("\n")

# Get all of the index names
index_names = get_index_names(database)
print("Index Names: {}".format(index_names))
print("\n")

# Get all of the cells in each index and print the number of cells (rows) for each index
for index_name in index_names:
    select_all_data = select_all_from_index(index_name, database)
    print("Index: {} has {} rows in the database file.".format(index_name, len(select_all_data)))
print("\n")

# Get all of the signatures (for tables only - not including "without rowid" and virtual tables)
signatures = {}
for table_name in table_names:
    # Specify the version history here to parse through all versions for signature generation
    table_signature = create_table_signature(table_name, database, version_history)
    # Account for "without rowid" table signatures until supported
    if table_signature:
        signatures[table_name] = table_signature

# Carve each table with the generated signature and print the number of carved cells (rows) per table
for table_name in table_names:
    if table_name in signatures:
        carved_cells = carve_table(table_name, signatures[table_name], database)
        print("Found {} carved cells for table: {} in the database file.".format(len(carved_cells), table_name))
print("\n")

# Combine names for index and tables (they are unique) and get the version history iterator for each
names = []
names.extend(table_names)
names.extend(index_names)
for name in names:
    signature = signatures[name] if name in signatures else None
    version_history_iterator = get_version_history_iterator(name, version_history, signature)
    for commit in version_history_iterator:
        string = "For: {} commit: {} for version: {}.".format(name, commit.updated, commit.version_number)
        if commit.updated:
            string += "  Carved Cells: {}.".format(True if commit.carved_cells else False)
        print(string)
print("\n")

# Check to make sure exporting variables were setup correctly for csv
if export_type and export_type.upper() == EXPORT_TYPES.CSV:

    # Create two directories for the two types csv files can be exported through the interface
    export_version_directory = export_directory + sep + "csv_version"
    if not exists(export_version_directory):
        makedirs(export_version_directory)
    export_version_history_directory = export_directory + sep + "csv_version_history"
    if not exists(export_version_history_directory):
        makedirs(export_version_history_directory)

    # Iterate through all index and table names and export their version history to a csv file (one at a time)
    for name in names:
        print("Exporting {} to {} as {}.".format(name, export_version_directory, export_type))
        export_table_or_index_version_history_to_csv(export_version_directory, version_history, name, None, False)
    print("\n")

    # Export all index and table histories to csv files while supplying signatures to carve tables and carving freelists
    print("Exporting history to {} with carvings as {}.".format(export_version_history_directory, export_type))
    export_version_history_to_csv(export_version_history_directory, version_history, signatures.values(), True)
    print("\n")

# Check to make sure exporting variable were setup correctly for SQLite
if export_type and export_type.upper() == EXPORT_TYPES.SQLITE:

    # Create two directories for the two types SQLite files can be exported through the interface
    export_version_directory = export_directory + sep + "sqlite_version"
    if not exists(export_version_directory):
        makedirs(export_version_directory)
    export_version_history_directory = export_directory + sep + "sqlite_version_history"
    if not exists(export_version_history_directory):
        makedirs(export_version_history_directory)

    # Currently the file name is taken from the base version name
    sqlite_base_file_name = basename(normpath(file_prefix))
    sqlite_file_postfix = "-sqlite-dissect.db3"

    # Iterate through all index and table names and export their version history to a csv file (one at a time)
    for name in names:
        fixed_master_schema_name = sub(" ", "_", name)
        master_schema_entry_file_name = sqlite_base_file_name + "-" + fixed_master_schema_name + sqlite_file_postfix
        print("Exporting {} to {} in {} as {}.".format(name, master_schema_entry_file_name, export_version_directory,
                                                       export_type))
        export_table_or_index_version_history_to_sqlite(export_version_directory, master_schema_entry_file_name,
                                                        version_history, name)
    print("\n")

    # Export all index and table histories to csv files while supplying signatures to carve tables and carving freelists
    sqlite_file_name = sqlite_base_file_name + sqlite_file_postfix
    print("Exporting history to {} in {} with carvings as {}.".format(sqlite_file_name,
                                                                      export_version_history_directory, export_type))
    export_version_history_to_sqlite(export_version_history_directory, sqlite_file_name, version_history,
                                     signatures.values(), True)
    print("\n")
