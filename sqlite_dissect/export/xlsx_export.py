from logging import getLogger
from openpyxl import Workbook
from os import rename
from os.path import exists
from os.path import sep
from uuid import uuid4
from sqlite_dissect.constants import ILLEGAL_XML_CHARACTER_PATTERN
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.constants import UTF_8
from sqlite_dissect.exception import ExportError

"""

xlsx_export.py

This script holds the objects used for exporting results of the SQLite carving framework to xlsx files.

This script holds the following object(s):
CommitXlsxExporter(object)

"""


class CommitXlsxExporter(object):

    def __init__(self, export_directory, file_name):
        self._workbook = Workbook(write_only=True)
        self._xlsx_file_name = export_directory + sep + file_name
        self._sheets = {}
        self._long_sheet_name_translation_dictionary = {}

    def __enter__(self):

        # Check if the file exists and if it does rename it
        if exists(self._xlsx_file_name):

            # Generate a uuid to append to the file name
            new_file_name_for_existing_file = self._xlsx_file_name + "-" + str(uuid4())

            # Rename the existing file
            rename(self._xlsx_file_name, new_file_name_for_existing_file)

            log_message = "File: {} already existing when creating the file for commit xlsx exporting.  The " \
                          "file was renamed to: {} and new data will be written to the file name specified."
            log_message = log_message.format(self._xlsx_file_name, new_file_name_for_existing_file)
            getLogger(LOGGER_NAME).debug(log_message)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._workbook.save(self._xlsx_file_name)
        log_message = "Saving file {} after xlsx export."
        log_message = log_message.format(self._xlsx_file_name)
        getLogger(LOGGER_NAME).debug(log_message)

    def write_commit(self, master_schema_entry, commit):

        """



        Note:  This function only writes the commit record if the commit record was updated.

        :param master_schema_entry:
        :param commit:

        :return:

        """

        if not commit.updated:
            return

        logger = getLogger(LOGGER_NAME)

        """

        In xlsx files, there is a limit to the number of characters allowed to be specified in a sheet name.  This
        limit is 31 characters.  The openpyxl library also checks for this use case and if it finds a sheet name longer
        than 31 characters, raises an exception.  Therefore, we check that here and accommodate for that use case when
        it occurs.

        This is done by maintaining a dictionary of commit names longer than 31 characters and a sheet name
        based off of the commit name that is within the character limit.  If a commit name is longer than 31 characters,
        all characters past 30 are chopped off and then a integer is added to the end in the range of 0 to 9 depending
        on the number of collisions that may occur for multiple similar commit names.

        Note:  There needs to be a better way to distinguish between similar commit names and if there are more than 10
               names similar in the first 30 characters, an exception will be raised.  Right now a maximum of 10 similar
               names are support (0 to 9).

        """

        # Setup the name postfix increment counter
        name_postfix_increment = 0

        # Set the sheet name to be the commit name
        sheet_name = commit.name

        # Check if the sheet name is greater than 31 characters
        if len(sheet_name) > 31:

            # Check if the sheet name is already in the dictionary
            if sheet_name in self._long_sheet_name_translation_dictionary:

                # Set it to the name already made for it from a previous call
                sheet_name = self._long_sheet_name_translation_dictionary[sheet_name]

            # The sheet name was not already in the dictionary so we need to make a new name
            else:

                # Continue while we are between 0 and 9
                while name_postfix_increment < 10:

                    # Create the truncated sheet name from the first 30 characters of the sheet name and name postfix
                    truncated_sheet_name = sheet_name[:30] + str(name_postfix_increment)

                    # CHeck if the name does not already exist in the dictionary
                    if truncated_sheet_name not in self._long_sheet_name_translation_dictionary:

                        # Add the sheet name and truncated sheet name into the dictionary
                        self._long_sheet_name_translation_dictionary[sheet_name] = truncated_sheet_name

                        # Set the sheet name
                        sheet_name = truncated_sheet_name

                        # Log a debug message for the truncation of the commit name as a sheet name
                        log_message = "Commit name: {} was truncated to: {} since it had a length of {} characters " \
                                      "which is greater than the 31 allowed characters for a sheet name."
                        log_message = log_message.format(commit.name, sheet_name, len(commit.name))
                        logger.debug(log_message)

                        # Break from the while loop
                        break

                    # The name already exists
                    else:

                        # Increment the name postfix counter
                        name_postfix_increment += 1

                # Raise an exception if the name postfix increment counter reached 10
                if name_postfix_increment == 10:
                    log_message = "Max number of allowed (10) increments reached for renaming the sheet with " \
                                  "original name: {} for page type: {} due to having a length of {} characters " \
                                  "which is greater than the 31 allowed characters while writing to xlsx file name: {}."
                    log_message = log_message.format(commit.name, commit.page_type, len(commit.name),
                                                     self._xlsx_file_name)
                    logger.warn(log_message)
                    raise ExportError(log_message)

        sheet = self._sheets[sheet_name] if sheet_name in self._sheets else None
        write_headers = False

        if not sheet:
            sheet = self._workbook.create_sheet(sheet_name)
            self._sheets[sheet_name] = sheet
            write_headers = True

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

        column_headers = []
        if write_headers:
            column_headers.extend(["File Source", "Version", "Page Version", "Cell Source", "Page Number",
                                   "Location", "Operation", "File Offset"])

        if commit.page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

            """

            Note:  The index master schema entries are currently not fully parsed and therefore we do not have
                   column definitions in order to derive the column names from.

            """

            sheet.append(column_headers)

            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            commit.added_cells.values(), "Added")
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            commit.updated_cells.values(), "Updated")
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            commit.deleted_cells.values(), "Deleted")
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            commit.carved_cells.values(), "Carved")

        elif commit.page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

            if write_headers:
                column_headers.append("Row ID")
                column_headers.extend([column_definition.column_name
                                       for column_definition in master_schema_entry.column_definitions])
                sheet.append(column_headers)

            # Sort the added, updated, and deleted cells by the row id
            sorted_added_cells = sorted(commit.added_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            sorted_added_cells, "Added")
            sorted_updated_cells = sorted(commit.updated_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            sorted_updated_cells, "Updated")
            sorted_deleted_cells = sorted(commit.deleted_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            sorted_deleted_cells, "Deleted")

            # We will not sort the carved cells since row ids are not deterministic even if parsed
            CommitXlsxExporter._write_cells(sheet, commit.file_type, commit.database_text_encoding, commit.page_type,
                                            commit.carved_cells.values(), "Carved")

        else:

            log_message = "Invalid commit page type: {} found for xlsx export on master " \
                          "schema entry name: {} while writing to xlsx file name: {}."
            log_message = log_message.format(commit.page_type, commit.name, self._xlsx_file_name)
            logger.warn(log_message)
            raise ExportError(log_message)

    @staticmethod
    def _write_cells(sheet, file_type, database_text_encoding, page_type, cells, operation):

        """

        This function will write the list of cells sent in to the sheet specified including the metadata regarding
        to the file type, page type, and operation.

        Note:  The types of the data in the values can prove to be an issue here.  We want to write the value out as
               a string similarly as the text and csv outputs do for example even though it may contain invalid
               characters.  When data is sent into the openpyxl library to be written to the xml xlsx, if it is a
               string, it is encoded into the default encoding and then checked for xml illegal characters that may
               pose an issue when written to the xml.  In order to properly check the values and write them accordingly
               through the openpyxl library we address the following use cases for the value in order:
               1.)  If the value is a bytearray (most likely originally a blob object) or a string value, we want to
                    write the value as a string.  However, in order to do this for blob objects or strings that may
                    have a few bad characters in them from carving, we need to do our due diligence and make sure
                    there are no bad unicode characters and no xml illegal characters that may cause issues with
                    writing to the xlsx.  In order to do this we do the following:
                    a.)  We first convert the value to string if the affinity was not text, otherwise we decode
                         the value in the database text encoding.  When we decode using the database text encoding,
                         we specify to "replace" characters it does not recognize in order to compensate for carved
                         rows.
                    b.)  We then test encoding it to UTF-8.
                         i.)   If the value successfully encodes as UTF-8 nothing is done further for this step.
                         ii.)  If the value throws an exception encoding, we have illegal unicode characters in the
                               string that need to be addressed.  In order to escape these, we decode the string
                               as UTF-8 using the "replace" method to replace any illegal unicode characters
                               with '\ufffd' and set this back as the value.
                    c.)  After we have successfully set the value back to a UTF-8 compliant value, we need to check
                         the value for xml illegal characters.  If any of these xml illegal characters are found,
                         they are replaced with a space.  This behaviour may be different from how values are output
                         into text or csv since this is being written to xml and additional rules apply for certain
                         characters.
                         between the xlsx output and text/csv output in reference to xml illegal characters.
                    d.)  After all the illegal characters are removed, due to the way openpyxl determines data types
                         of particular cells, if a cell starts with "=", it is determined to be a formula and set as
                         that in the data type field for that cell.  This causes issues when opening the file in excel.
                         Microsoft Excel recommends prefacing the string with a single quote character, however,
                         this only seems to be within Excel itself.  You can specify the data type of the cell in
                         openpyxl, but not in the write-only mode that is being used here.  In order to work around
                         this, we check if the first character of a string or bytearray is a "=" character and preface
                         that string with a space.  There may be better ways to handle this such as not using the
                         write-only mode.
                         Note:  Additionally to the "=" character, the "-" character has similar issues in excel.
                                However, openpyxl explicitly checks on the "=" character being the first character
                                and setting that cell to a formula and does not handle the use case of a cell starting
                                with the "-" character, so this use case is ignored.
               2.)  If the value does not fall in one of the above use cases, we leave it as is and write it to the
                    xlsx without any modifications.

        Note:  If the value is None, we leave it as None.  We used to update the None value with the string "NULL"
               since issues could be seen when carving cells where the value is None not because it was NULL originally
               in the database, but because it was unable to be parsed out when it may have actually had a value (when
               it was truncated).  Distinction is needed between these two use cases.

        Note:  It was noticed that blob objects are typically detected as isinstance of str here and strings are
               bytearray objects.  This needs to be investigated why exactly blob objects are coming out as str
               objects.

        Note:  Comparisons should be done on how other applications work with different database text encodings in
               reference to their output.

        Note:  The decoding of the value in the database text encoding should only specify replace on a carved entry.

        :param sheet:
        :param file_type:
        :param database_text_encoding:
        :param page_type:
        :param cells:
        :param operation:

        :return:

        """

        for cell in cells:
            cell_record_column_values = []
            for record_column in cell.payload.record_columns:
                serial_type = record_column.serial_type
                text_affinity = True if serial_type >= 13 and serial_type % 2 == 1 else False
                value = record_column.value
                if isinstance(value, (bytearray, str)):
                    if len(value) == 0 and isinstance(value, bytearray):
                        value = None
                    else:
                        value = value.decode(database_text_encoding, "replace") if text_affinity else str(value)
                        try:
                            value.encode(UTF_8)
                        except UnicodeDecodeError:
                            value = value.decode(UTF_8, "replace")
                        value = ILLEGAL_XML_CHARACTER_PATTERN.sub(" ", value)
                        if value.startswith("="):
                            value = ' ' + value
                cell_record_column_values.append(value)

            row = [file_type, cell.version_number, cell.page_version_number, cell.source, cell.page_number,
                   cell.location, operation, cell.file_offset]
            if page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:
                row.append(cell.row_id)
            row.extend(cell_record_column_values)

            sheet.append(row)
