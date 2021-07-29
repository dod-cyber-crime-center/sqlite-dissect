from logging import getLogger
from os import rename
from os.path import exists
from os.path import sep
from re import sub
from sqlite3 import connect
from sqlite3 import sqlite_version
from sqlite3 import version
from uuid import uuid4
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.exception import ExportError

"""

sqlite_export.py

This script holds the objects used for exporting results of the SQLite carving framework to SQLite files.

Note:  During development this script was written testing and using SQLite version 3.9.2.  The pysqlite version
       was 2.6.0.  Keep in mind that sqlite3.version gives version information on the pysqlite SQLite interface code,
       whereas sqlite3.sqlite_version gives the actual version of the SQLite driver that is used.

This script holds the following object(s):
CommitSqliteExporter(object)

"""


class CommitSqliteExporter(object):

    def __init__(self, export_directory, file_name):

        """

        Constructor.

        The master schema entries created tables dictionary will hold the names of the created tables in the SQLite
        file being written to so consecutive writes to those tables will be able to tell if the table was already
        created or not.  The reason it is a dictionary and not just a list of names is that the value keyed off the
        master schema name will be the number of columns in that table.  This is needed since different rows within
        the same table may have a different number of columns in the case that the table was altered and columns were
        added at some point.  This way the number of columns can be specified and values that may be missing can be
        specified as being left NULL.

        Note:  According to documentation, it appears only tables can be altered.  However, we include the same logic
               with the number of rows for both tables and indexes for consistency and code reduction.

        Note:  If the file is detected as already existing, a uuid will be appended to the file name of the old file
               and a new file by the name specified will be created.

        :param export_directory:
        :param file_name:

        :return:

        """

        self._sqlite_file_name = export_directory + sep + file_name
        self._connection = None
        self._master_schema_entries_created_tables = {}

    def __enter__(self):

        # Check if the file exists and if it does rename it
        if exists(self._sqlite_file_name):

            # Generate a uuid to append to the file name
            new_file_name_for_existing_file = self._sqlite_file_name + "-" + str(uuid4())

            # Rename the existing file
            rename(self._sqlite_file_name, new_file_name_for_existing_file)

            log_message = "File: {} already existing when creating the file for commit sqlite exporting.  The " \
                          "file was renamed to: {} and new data will be written to the file name specified."
            log_message = log_message.format(self._sqlite_file_name, new_file_name_for_existing_file)
            getLogger(LOGGER_NAME).debug(log_message)

        self._connection = connect(self._sqlite_file_name)
        log_message = "Opened connection to {} using sqlite version: {} and pysqlite version: {}"
        log_message = log_message.format(self._sqlite_file_name, sqlite_version, version)
        getLogger(LOGGER_NAME).debug(log_message)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()
        log_message = "Closed connection to {} using sqlite version: {} and pysqlite version: {}"
        log_message = log_message.format(self._sqlite_file_name, sqlite_version, version)
        getLogger(LOGGER_NAME).debug(log_message)

    def write_commit(self, master_schema_entry, commit):

        """



        Note:  This function only writes the commit record if the commit record was updated.

        Note:  Any table or index names beginning with sqlite_ are not allowed since "sqlite_" is reserved for
               internal schema object names.  In the case that a table or index is an internal schema object, we
               will preface that name with an "iso_" representing an (i)nternal (s)chema (o)bject.

        :param master_schema_entry:
        :param commit:

        :return:

        """

        if not commit.updated:
            return

        logger = getLogger(LOGGER_NAME)

        # Check if the master schema entry name is a internal schema object and if so preface it with "iso_"
        internal_schema_object = master_schema_entry.internal_schema_object \
            if hasattr(master_schema_entry, "internal_schema_object") else False
        table_name = "iso_" + master_schema_entry.name if internal_schema_object else master_schema_entry.name

        # Check if we have created the table for this master schema entry name yet
        if master_schema_entry.name not in self._master_schema_entries_created_tables:

            column_headers = ["File Source", "Version", "Page Version", "Cell Source", "Page Number", "Location",
                              "Operation", "File Offset"]

            """

            Below we have to account for how the pages are stored.

            For the table master schema entry row type:
                1.) If the table is not a "without rowid" table, it will be stored on a table b-tree page with
                    row ids.
                2.) If the table is a "without rowid" table, it will be stored on an index b-tree page with no
                    row ids.

            For the index master schema entry row type:
                1.) It will be stored on an index b-tree page with no row ids.

            The commit object handles this by having a page type to make this distinction easier.  Therefore, we only
            need to check on the page type here.

            """

            if commit.page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

                """

                Note:  The index master schema entries are currently not fully parsed and therefore we do not have
                       column definitions in order to derive the column names from.

                       Since we need to have column headers defined for each of the fields, here we calculate the
                       number of additional columns that will be needed to output the fields from the index and expand
                       the table by that number using generic column names.

                       At least one of the added, updated, deleted, or carved cells fields must be set for the commit
                       to have been considered updated and for us to have gotten here.

                """

                cells = list()
                cells.extend(commit.added_cells.values())
                cells.extend(commit.updated_cells.values())
                cells.extend(commit.deleted_cells.values())
                cells.extend(commit.carved_cells.values())

                if len(cells) < 1:
                    log_message = "Found invalid number of cells in commit when specified updated: {} " \
                                  "found for sqlite export on master schema entry name: {} page type: {} " \
                                  "while writing to sqlite file name: {}."
                    log_message = log_message.format(len(cells), commit.name, commit.page_type, self._sqlite_file_name)
                    logger.warn(log_message)
                    raise ExportError(log_message)

                number_of_columns = len(cells[0].payload.record_columns)
                index_column_headers = []
                for i in range(number_of_columns):
                    index_column_headers.append("Column {}".format(i))

                column_headers.extend(index_column_headers)
                column_headers = [sub(" ", "_", column_header).lower() for column_header in column_headers]

            elif commit.page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

                column_definitions = [column_definition.column_name
                                      for column_definition in master_schema_entry.column_definitions]
                column_headers.append("Row ID")

                """

                In order to make sure there are no pre-existing columns with "sd_" prefacing them, we check for that
                use case and add another "sd_" to the beginning of the column header name until there are no conflicts.

                """

                updated_column_headers = []
                for column_header in column_headers:
                    updated_column_header_name = "sd_" + sub(" ", "_", column_header).lower()
                    while updated_column_header_name in column_definitions:
                        updated_column_header_name = "sd_" + updated_column_header_name
                    updated_column_headers.append(updated_column_header_name)

                updated_column_headers.extend(column_definitions)
                column_headers = updated_column_headers

            else:

                log_message = "Invalid commit page type: {} found for sqlite export on master " \
                              "schema entry name: {} while writing to sqlite file name: {}."
                log_message = log_message.format(commit.page_type, commit.name, self._sqlite_file_name)
                logger.warn(log_message)
                raise ExportError(log_message)

            create_table_statement = "CREATE TABLE {} ({})"
            create_table_statement = create_table_statement.format(table_name, " ,".join(column_headers))
            self._connection.execute(create_table_statement)
            self._connection.commit()

            self._master_schema_entries_created_tables[master_schema_entry.name] = len(column_headers)

        """

        Now write all of the cells to the SQLite file in their table.

        """

        column_count = self._master_schema_entries_created_tables[master_schema_entry.name]

        if commit.page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type,
                                              commit.added_cells.values(), "Added")
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type,
                                              commit.updated_cells.values(), "Updated")
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type,
                                              commit.deleted_cells.values(), "Deleted")
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type,
                                              commit.carved_cells.values(), "Carved")

        elif commit.page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

            # Sort the added, updated, and deleted cells by the row id
            sorted_added_cells = sorted(commit.added_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type, sorted_added_cells,
                                              "Added")
            sorted_updated_cells = sorted(commit.updated_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type, sorted_updated_cells,
                                              "Updated")
            sorted_deleted_cells = sorted(commit.deleted_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type, sorted_deleted_cells,
                                              "Deleted")

            # We will not sort the carved cells since row ids are not deterministic even if parsed
            CommitSqliteExporter._write_cells(self._connection, table_name, column_count, commit.file_type,
                                              commit.database_text_encoding, commit.page_type,
                                              commit.carved_cells.values(), "Carved")

        else:

            log_message = "Invalid commit page type: {} found for sqlite export on master " \
                          "schema entry name: {} while writing to sqlite file name: {}."
            log_message = log_message.format(commit.page_type, commit.name, self._sqlite_file_name)
            logger.warn(log_message)
            raise ExportError(log_message)

        """

        Commit any entries written to the SQLite file.

        Note:  This is done to speed up writing to the SQLite file and was previously in the "_write_cells" function
               and called after every set of cells written.  Now that it has been brought out here, it will execute
               for every commit record.  This will reduce calls to commit and also make sure at least one statement
               has been executed when calling a commit.  In addition the insert statement was changed to insert
               many at a time instead of individually.

        """

        self._connection.commit()

    @staticmethod
    def _write_cells(connection, table_name, column_count, file_type,
                     database_text_encoding, page_type, cells, operation):

        """

        This function will write the list of cells sent in to the connection under the table name specified including
        the metadata regarding to the file type, page type, and operation.

        Note:  The types of the data in the values can prove to be an issue here.  For the most part we want to write
               back the value as the type that we read it out of the file as even though the data has the possibility
               of still being stored differently since we are leaving all data types to be undefined causing the storage
               algorithm internal to SQLite to slightly change.  Despite this, we make the following modifications in
               order to best ensure data integrity when writing the data back to the SQLite file:
               1.) If the value is a bytearray, the value is interpreted as a blob object.  In order to write this
                   back correctly, we set it to buffer(value) in order to write it back to the SQLite database as
                   a blob object.  Before we write it back, we make sure that the object does not have text affinity,
                   or if it does we decode it in the database text encoding before writing it.
               2.) If the value is a string, we encode it using UTF-8.  If this fails, that means it had characters
                   not supported by the unicode encoding which caused it to fail.  Since we are writing back carved
                   records that may have invalid characters in strings due to parts being overwritten or false
                   positives, this can occur a lot.  Therefore, if the unicode encoding fails, we do the same
                   as above for blob objects and create a buffer(value) blob object and write that back to the
                   database in order to maintain the original data.  Therefore, in some tables, depending on the
                   data parsed or strings retrieved may be stored in either a string (text) or blob storage class.
               3.) If the value does not fall in one of the above use cases, we leave it as is and write it back to the
                   database without any modifications.

        Note:  If the value is None, we leave it as None.  We used to update the None value with the string "NULL"
               since issues could be seen when carving cells where the value is None not because it was NULL originally
               in the database, but because it was unable to be parsed out when it may have actually had a value (when
               it was truncated).  Distinction is needed between these two use cases.

        Note:  Since the amount of columns found may be less than the number of columns actually in the SQL/schema
               due to alter table statements over time that may have added columns, we account for the difference
               in the number of columns.  This is done by taking the difference of the number of columns in the
               SQL/schema and subtracting the number of columns for the particular row that is being worked on
               and multiply that number by the "None" field in order to pad out the row in the SQLite database
               with no data for the remaining columns.

        :param connection:
        :param table_name:
        :param column_count:
        :param file_type:
        :param database_text_encoding:
        :param page_type:
        :param cells:
        :param operation:

        :return:

        """

        if cells:

            entries = []

            for cell in cells:

                cell_record_column_values = []
                for record_column in cell.payload.record_columns:
                    serial_type = record_column.serial_type
                    text_affinity = True if serial_type >= 13 and serial_type % 2 == 1 else False
                    value = record_column.value

                    if value is None:
                        pass
                    elif isinstance(value, bytearray):
                        if text_affinity:
                            value = value.decode(database_text_encoding, "replace")
                        else:
                            value = buffer(value)
                    elif isinstance(value, str):
                        try:
                            if text_affinity:
                                value = value.decode(database_text_encoding, "replace")
                            else:
                                value = buffer(value)
                        except UnicodeDecodeError:

                            """

                            Note:  Here we do not decode or encode the value, since the above failed the value will
                                   contain text that cannot be properly decoded and most likely due to random bytes
                                   in a carving.  In this case, we just print the value without trying to account
                                   for the database text encoding which may mean the text may appear differently
                                   (ie. with spaces between each character), but it is better to do it this way
                                   rather then to risk replacing characters since we don't know if it is indeed text.

                            """

                            value = buffer(value)

                    cell_record_column_values.append(value)

                row = [file_type, cell.version_number, cell.page_version_number, cell.source, cell.page_number,
                       cell.location, operation, cell.file_offset]
                if page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:
                    row.append(cell.row_id)
                row.extend(cell_record_column_values)

                # Check the length of the row against the column count and pad it out with NULLs if necessary
                if len(row) < column_count:
                    row.extend([None] * (column_count - len(row)))

                if len(row) > column_count:
                    log_message = "The number of columns found in the row: {} were more than the expected: {} " \
                                  "for sqlite export on master schema entry name: {} with file type: {} " \
                                  "and page type: {}."
                    log_message = log_message.format(len(row), column_count, table_name, file_type, page_type)
                    getLogger(LOGGER_NAME).warn(log_message)
                    raise ExportError(log_message)

                entries.append(tuple(row))

            if not entries:
                log_message = "Did not find any entries to write when cells were specified for sqlite export on " \
                              "master schema entry name: {} with file type: {} and page type: {}."
                log_message = log_message.format(table_name, file_type, page_type)
                getLogger(LOGGER_NAME).warn(log_message)
                raise ExportError(log_message)

            number_of_rows = (len(entries[0]) - 1)

            column_fields = "?" + (", ?" * number_of_rows)
            insert_statement = "INSERT INTO {} VALUES ({})".format(table_name, column_fields)
            connection.executemany(insert_statement, entries)
