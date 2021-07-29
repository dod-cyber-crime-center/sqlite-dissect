from logging import getLogger
from os import rename
from os.path import exists
from os.path import sep
from uuid import uuid4
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.exception import ExportError
from sqlite_dissect.output import stringify_cell_record

"""

text_export.py

This script holds the objects used for exporting results of the SQLite carving framework to text files.

This script holds the following object(s):
CommitConsoleExporter(object)
CommitTextExporter(object)

"""


class CommitConsoleExporter(object):

    @staticmethod
    def write_header(master_schema_entry, page_type):
        header = "\nMaster schema entry: {} row type: {} on page type: {} with sql: {}."
        header = header.format(master_schema_entry.name, master_schema_entry.row_type,
                               page_type, master_schema_entry.sql)
        print(header)

    @staticmethod
    def write_commit(commit):

        """



        Note:  This function only prints the commit record if the commit record was updated.

        :param commit:

        :return:

        """

        if not commit.updated:
            return

        logger = getLogger(LOGGER_NAME)

        commit_header = "Commit: {} updated in version: {} with root page number: {} on b-tree page numbers: {}."
        print(commit_header.format(commit.name, commit.version_number,
                                   commit.root_page_number, commit.b_tree_page_numbers))

        if commit.page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               commit.added_cells.values(), "Added")
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               commit.updated_cells.values(), "Updated")
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               commit.deleted_cells.values(), "Deleted")
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               commit.carved_cells.values(), "Carved")

        elif commit.page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

            # Sort the added, updated, and deleted cells by the row id
            sorted_added_cells = sorted(commit.added_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               sorted_added_cells, "Added")
            sorted_updated_cells = sorted(commit.updated_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               sorted_updated_cells, "Updated")
            sorted_deleted_cells = sorted(commit.deleted_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               sorted_deleted_cells, "Deleted")

            # We will not sort the carved cells since row ids are not deterministic even if parsed
            CommitConsoleExporter._write_cells(commit.file_type, commit.database_text_encoding, commit.page_type,
                                               commit.carved_cells.values(), "Carved")

        else:

            log_message = "Invalid commit page type: {} found for text export on master " \
                          "schema entry name: {} while writing to sqlite file name: {}."
            log_message = log_message.format(commit.page_type, commit.name)
            logger.warn(log_message)
            raise ExportError(log_message)

    @staticmethod
    def _write_cells(file_type, database_text_encoding, page_type, cells, operation):

        """

        This function will write the list of cells sent in to the connection under the table name specified including
        the metadata regarding to the file type, page type, and operation.

        Note:  Since we are writing out to text, all values are written as strings.

        :param file_type:
        :param database_text_encoding:
        :param page_type:
        :param cells:
        :param operation:

        :return:

        """

        base_string = "File Type: {} Version Number: {} Page Version Number: {} Source: {} " \
                      "Page Number: {} Location: {} Operation: {} File Offset: {}"
        for cell in cells:
            preface = base_string.format(file_type, cell.version_number, cell.page_version_number, cell.source,
                                         cell.page_number, cell.location, operation, cell.file_offset)
            row_values = stringify_cell_record(cell, database_text_encoding, page_type)
            print(preface + " " + row_values + ".")


class CommitTextExporter(object):

    def __init__(self, export_directory, file_name):

        """



        Note:  If the file is detected as already existing, a uuid will be appended to the file name of the old file
               and a new file by the name specified will be created.

        :param export_directory:
        :param file_name:

        :return:

        """

        self._text_file_name = export_directory + sep + file_name
        self._file_handle = None

    def __enter__(self):

        # Check if the file exists and if it does rename it
        if exists(self._text_file_name):

            # Generate a uuid to append to the file name
            new_file_name_for_existing_file = self._text_file_name + "-" + str(uuid4())

            # Rename the existing file
            rename(self._text_file_name, new_file_name_for_existing_file)

            log_message = "File: {} already existing when creating the file for commit text exporting.  The " \
                          "file was renamed to: {} and new data will be written to the file name specified."
            log_message = log_message.format(self._text_file_name, new_file_name_for_existing_file)
            getLogger(LOGGER_NAME).debug(log_message)

        self._file_handle = open(self._text_file_name, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_handle.close()

    def write_header(self, master_schema_entry, page_type):
        header = "\nMaster schema entry: {} row type: {} on page type: {} with sql: {}."
        header = header.format(master_schema_entry.name, master_schema_entry.row_type,
                               page_type, master_schema_entry.sql)
        self._file_handle.write(header + "\n")

    def write_commit(self, commit):

        """



        Note:  This function only writes the commit record if the commit record was updated.

        :param commit:

        :return:

        """

        if not commit.updated:
            return

        logger = getLogger(LOGGER_NAME)

        commit_header = "Commit: {} updated in version: {} with root page number: {} on b-tree page numbers: {}.\n"
        self._file_handle.write(commit_header.format(commit.name, commit.version_number,
                                                     commit.root_page_number, commit.b_tree_page_numbers))

        if commit.page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, commit.added_cells.values(), "Added")
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, commit.updated_cells.values(), "Updated")
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, commit.deleted_cells.values(), "Deleted")
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, commit.carved_cells.values(), "Carved")

        elif commit.page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

            # Sort the added, updated, and deleted cells by the row id
            sorted_added_cells = sorted(commit.added_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, sorted_added_cells, "Added")
            sorted_updated_cells = sorted(commit.updated_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, sorted_updated_cells, "Updated")
            sorted_deleted_cells = sorted(commit.deleted_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, sorted_deleted_cells, "Deleted")

            # We will not sort the carved cells since row ids are not deterministic even if parsed
            CommitTextExporter._write_cells(self._file_handle, commit.file_type, commit.database_text_encoding,
                                            commit.page_type, commit.carved_cells.values(), "Carved")

        else:

            log_message = "Invalid commit page type: {} found for text export on master " \
                          "schema entry name: {}."
            log_message = log_message.format(commit.page_type, commit.name, self._text_file_name)
            logger.warn(log_message)
            raise ExportError(log_message)

    @staticmethod
    def _write_cells(file_handle, file_type, database_text_encoding, page_type, cells, operation):

        """

        This function will write the list of cells sent in to the connection under the table name specified including
        the metadata regarding to the file type, page type, and operation.

        Note:  Since we are writing out to text, all values are written as strings.

        :param file_handle:
        :param file_type:
        :param database_text_encoding:
        :param page_type:
        :param cells:
        :param operation:

        :return:

        """

        base_string = "File Type: {} Version Number: {} Page Version Number: {} Source: {} " \
                      "Page Number: {} Location: {} Operation: {} File Offset: {}"
        for cell in cells:
            preface = base_string.format(file_type, cell.version_number, cell.page_version_number, cell.source,
                                         cell.page_number, cell.location, operation, cell.file_offset)
            row_values = stringify_cell_record(cell, database_text_encoding, page_type)
            file_handle.write(preface + " " + row_values + ".\n")
