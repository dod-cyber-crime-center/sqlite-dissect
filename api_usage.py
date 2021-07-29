import logging
import os
import sqlite_dissect.constants as sqlite_constants
import sqlite_dissect.interface as sqlite_interface

"""

api-usage.py

This script shows an example of the api usage for a specific test file.

"""

# Setup logging
logging_level = logging.ERROR
logging_format = '%(levelname)s %(asctime)s [%(pathname)s] %(funcName)s at line %(lineno)d: %(message)s'
logging_date_format = '%d %b %Y %H:%M:%S'
logging.basicConfig(level=logging_level, format=logging_format, datefmt=logging_date_format)

# Setup console logging
console_logger = logging.StreamHandler()
console_logger.setLevel(logging_level)
console_logger.setFormatter(logging.Formatter(logging_format, logging_date_format))
logging.getLogger(sqlite_constants.LOGGER_NAME).addHandler(console_logger)

"""

API Usage

The three fields below need to be filled in and are currently hardcoded:
file_name: The SQLite file to investigate (and associated WAL file if it exists in the same directory)
table_name: The table in the file to create a signature of and carve against the SQLite file with.
column_names: The columns in the table we are interested in printing out carved data from.

Note:  Below will carve entries from the b-tree page of the table and the freelists.  The use case of cross b-tree
       carving is not yet implemented yet in SQLite Dissect.

"""

# Specify the file details
file_name = "FILE_NAME"
table_name = "TABLE_NAME"
column_names = ["COLUMN_ONE", "COLUMN_TWO"]

# Create the database
database = sqlite_interface.create_database(file_name)

# Create the write ahead log
wal_file_name = file_name + sqlite_constants.WAL_FILE_POSTFIX
write_ahead_log = sqlite_interface.create_write_ahead_log(wal_file_name) if os.path.exists(wal_file_name) else None

# Create the version history
version_history = sqlite_interface.create_version_history(database, write_ahead_log)

# Create the signature we are interested in carving
table_signature = sqlite_interface.create_table_signature(table_name, database, version_history)

# Account for "without rowid"/virtual table signatures until supported
if not table_signature:
    print("Table signature not supported (\"without rowid\" table or virtual table)")
    exit(0)

# Get the column indices of the columns we are interested in
column_name_indices = {}
for column_name in column_names:
    column_name_indices[column_name] = sqlite_interface.get_column_index(column_name, table_name, version_history)

# Get a version history iterator for the table
carve_freelists = True
table_history_iterator = sqlite_interface.get_version_history_iterator(table_name, version_history,
                                                                       table_signature, carve_freelists)
# Iterate through the commits in the history for this table
for commit in table_history_iterator:
    # The table was only modified if the commit was updated for this table and make sure there were carved cells
    if commit.updated and commit.carved_cells:
        carved_cells = commit.carved_cells
        for carved_cell in carved_cells.itervalues():
            for column_name in column_name_indices.keys():
                record_column = carved_cell.payload.record_columns[column_name_indices.get(column_name)]
                print("Commit version: %s table record column: %s has serial type: %s with value of: \"%s\"." %\
                  (commit.version_number, column_name, record_column.serial_type, record_column.value))
