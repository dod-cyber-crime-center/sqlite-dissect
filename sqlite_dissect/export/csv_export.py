import os
from csv import QUOTE_ALL
from csv import writer
from logging import DEBUG
from logging import getLogger
from os.path import basename
from os.path import normpath
from os.path import sep
from re import sub
from sqlite_dissect.constants import ILLEGAL_XML_CHARACTER_PATTERN
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.constants import UTF_8
from sqlite_dissect.exception import ExportError
from sqlite_dissect.file.database.utilities import aggregate_leaf_cells

"""

csv_export.py

This script holds the objects used for exporting results of the SQLite carving framework to csv files.

This script holds the following object(s):
VersionCsvExporter(object)
CommitCsvExporter(object)

"""


class VersionCsvExporter(object):

    @staticmethod
    def write_version(csv_file_name, export_directory, version, master_schema_entry_carved_records=None):

        logger = getLogger(LOGGER_NAME)

        if not master_schema_entry_carved_records:
            master_schema_entry_carved_records = {}

        for master_schema_entry in version.master_schema.master_schema_entries:

            """

            Here we only care about the master schema entries that have a root page number since ones that either
            do not have a root page number or have a root page number of 0 do not have correlating b-trees in the
            SQLite file and are instead either trigger types, view types, or special cases of table types such as
            virtual tables.

            """

            if master_schema_entry.root_page_number:

                fixed_file_name = basename(normpath(csv_file_name))
                fixed_master_schema_name = sub(" ", "_", master_schema_entry.name)
                csv_file_name = export_directory + sep + fixed_file_name + "-" + fixed_master_schema_name + ".csv"

                logger.info("Writing CSV file: {}.".format(csv_file_name))

                with open(csv_file_name, "wb") as csv_file_handle:

                    csv_writer = writer(csv_file_handle, delimiter=',', quotechar="\"", quoting=QUOTE_ALL)

                    b_tree_root_page = version.get_b_tree_root_page(master_schema_entry.root_page_number)

                    """

                    Retrieve the carved records for this particular master schema entry.

                    """

                    carved_cells = []
                    if master_schema_entry.name in master_schema_entry_carved_records:
                        carved_cells = master_schema_entry_carved_records[master_schema_entry.name]

                    """

                    Below we have to account for how the pages are stored.

                    For the table master schema entry row type:
                        1.) If the table is not a "without rowid" table, it will be stored on a table b-tree page with
                            row ids.
                        2.) If the table is a "without rowid" table, it will be stored on an index b-tree page with no
                            row ids.

                    For the index master schema entry row type:
                        1.) It will be stored on an index b-tree page with no row ids.

                    Different functions are created to write records for both table and index b-tree pages.  Keep in
                    mind that a table master schema row type may be stored on a index b-tree page depending if it is
                    specified as a "without rowid" table.  All index master schema row types are stored on index
                    b-tree pages.

                    """

                    if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.TABLE:

                        if not master_schema_entry.without_row_id:

                            VersionCsvExporter._write_b_tree_table_leaf_records(csv_writer, version,
                                                                                master_schema_entry,
                                                                                b_tree_root_page, carved_cells)

                        else:

                            VersionCsvExporter._write_b_tree_index_leaf_records(csv_writer, version,
                                                                                master_schema_entry,
                                                                                b_tree_root_page, carved_cells)

                    elif master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.INDEX:

                        VersionCsvExporter._write_b_tree_index_leaf_records(csv_writer, version, master_schema_entry,
                                                                            b_tree_root_page, carved_cells)

                    else:

                        log_message = "Invalid master schema entry row type: {} found for csv export on master " \
                                      "schema entry name: {} table name: {} sql: {}."
                        log_message = log_message.format(master_schema_entry.row_type, master_schema_entry.name,
                                                         master_schema_entry.table_name, master_schema_entry.sql)

                        logger.warn(log_message)
                        raise ExportError(log_message)

    @staticmethod
    def _write_b_tree_index_leaf_records(csv_writer, version, master_schema_entry, b_tree_root_page, carved_cells):

        """

        This function will write the list of cells sent in to the sheet specified including the metadata regarding
        to the file type, page type, and operation.

        Note:  The types of the data in the values can prove to be an issue here.  We want to write the value out as
               a string similarly as the text and csv outputs do for example even though it may contain invalid
               characters.  When data is sent into the openpyxl library to be written to the xml xlsx, if it is a
               string, it is encoded into the default encoding and then checked for xml illegal characters that may
               pose an issue when written to the xml.  In order to properly check the values and write them accordingly
               through the openpyxl library we address the following use cases for the value in order:
               1.)  If the value is None, we replace the value with the string "NULL".  This might be replaced by
                    leaving it None but issues can be seen when carving cells where the value is None not because it
                    was NULL originally in the database, but because it was unable to be parsed out when it may have
                    actually had a value (when it was truncated).  Distinction is needed between these two use cases.
               2.)  If the value is a bytearray (most likely originally a blob object) or a string value, we want to
                    write the value as a string.  However, in order to do this for blob objects or strings that may
                    have a few bad characters in them from carving, we need to do our due diligence and make sure
                    there are no bad unicode characters and no xml illegal characters that may cause issues with
                    writing to the xlsx.  In order to do this we do the following:
                    a.)  We first convert the value to string if the affinity was not text, otherwise we decode
                         the value in the database text encoding.  When we decode using the database text encoding,
                         we specify to "replace" characters it does not recognize in order to compensate for carved
                         rows.
                    b.)  We then test encoding it to UTF-8.
                         i.)   If the value successfully encodes as UTF-8 we set that as the value.
                         ii.)  If the value throws an exception encoding, we have illegal unicode characters in the
                               string that need to be addressed.  In order to escape these, we decode the string
                               as UTF-8 using the "replace" method to replace any illegal unicode characters
                               with '\ufffd' and set this back as the value after encoding again.
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
               3.)  If the value does not fall in one of the above use cases, we leave it as is and write it to the
                    xlsx without any modifications.

        Note:  It was noticed that blob objects are typically detected as isinstance of str here and strings are
               bytearray objects.  This needs to be investigated why exactly blob objects are coming out as str
               objects.

        Note:  Comparision should be done on how other applications work with different database text encodings in
               reference to their output.

        Note:  The decoding of the value in the database text encoding should only specify replace on a carved entry.

        :param csv_writer:
        :param version:
        :param master_schema_entry:
        :param b_tree_root_page:
        :param carved_cells:

        :return:

        """

        logger = getLogger(LOGGER_NAME)

        number_of_cells, cells = aggregate_leaf_cells(b_tree_root_page)

        if logger.isEnabledFor(DEBUG):
            master_schema_entry_string = "The {} b-tree page with {} row type and name: {} with sql: {} " \
                                         "has {} in-tact rows:"
            master_schema_entry_string = master_schema_entry_string.format(b_tree_root_page.page_type,
                                                                           master_schema_entry.row_type,
                                                                           master_schema_entry.name,
                                                                           master_schema_entry.sql, number_of_cells)
            logger.debug(master_schema_entry_string)

        """

        Note:  The index master schema entries are currently not fully parsed and therefore we do not have column
               definitions in order to derive the column names from.

        """

        column_headers = []
        column_headers.extend(["File Source", "Version", "Page Version", "Cell Source", "Page Number", "Location",
                               "Carved", "Status", "File Offset"])
        logger.debug("Column Headers: {}".format(" , ".join(column_headers)))

        csv_writer.writerow(column_headers)

        for cell in cells.values():

            cell_record_column_values = []

            for record_column in cell.payload.record_columns:
                serial_type = record_column.serial_type
                text_affinity = True if serial_type >= 13 and serial_type % 2 == 1 else False
                value = record_column.value
                if value is None:
                    pass
                elif isinstance(value, (bytearray, str)):
                    value = value.decode(version.database_text_encoding, "replace") if text_affinity else str(value)
                    try:
                        value.encode(UTF_8)
                    except UnicodeDecodeError:
                        value = value.decode(UTF_8, "replace")
                    value = ILLEGAL_XML_CHARACTER_PATTERN.sub(" ", value)
                    if value.startswith("="):
                        value = ' ' + value
                cell_record_column_values.append(value)

            row = [version.file_type, cell.version_number, cell.page_version_number, cell.source, cell.page_number,
                   cell.location, False, "Complete", cell.file_offset]
            row.extend(cell_record_column_values)
            csv_writer.writerow(row)

        if logger.isEnabledFor(DEBUG):
            for cell in cells.values():
                cell_record_column_values = [str(record_column.value) if record_column.value else "NULL"
                                             for record_column in cell.payload.record_columns]
                log_message = "File source: {} version: {} page version: {} cell source: {} page: {} located: {} " \
                              "carved: {} status: {} at file offset: {}: "
                log_message = log_message.format(version.file_type, cell.version_number, cell.page_version_number,
                                                 cell.source, cell.page_number, cell.location, False,
                                                 "Complete", cell.file_offset)
                log_message += "(" + ", ".join(cell_record_column_values) + ")"
                logger.debug(log_message)

        VersionCsvExporter._write_b_tree_table_master_schema_carved_records(csv_writer, version, carved_cells, False)

    @staticmethod
    def _write_b_tree_table_leaf_records(csv_writer, version, master_schema_entry, b_tree_root_page, carved_cells):

        """

        This function will write the list of cells sent in to the sheet specified including the metadata regarding
        to the file type, page type, and operation.

        Note:  The types of the data in the values can prove to be an issue here.  We want to write the value out as
               a string similarly as the text and csv outputs do for example even though it may contain invalid
               characters.  When data is sent into the openpyxl library to be written to the xml xlsx, if it is a
               string, it is encoded into the default encoding and then checked for xml illegal characters that may
               pose an issue when written to the xml.  In order to properly check the values and write them accordingly
               through the openpyxl library we address the following use cases for the value in order:
               1.)  If the value is None, we replace the value with the string "NULL".  This might be replaced by
                    leaving it None but issues can be seen when carving cells where the value is None not because it
                    was NULL originally in the database, but because it was unable to be parsed out when it may have
                    actually had a value (when it was truncated).  Distinction is needed between these two use cases.
               2.)  If the value is a bytearray (most likely originally a blob object) or a string value, we want to
                    write the value as a string.  However, in order to do this for blob objects or strings that may
                    have a few bad characters in them from carving, we need to do our due diligence and make sure
                    there are no bad unicode characters and no xml illegal characters that may cause issues with
                    writing to the xlsx.  In order to do this we do the following:
                    a.)  We first convert the value to string if the affinity was not text, otherwise we decode
                         the value in the database text encoding.  When we decode using the database text encoding,
                         we specify to "replace" characters it does not recognize in order to compensate for carved
                         rows.
                    b.)  We then test encoding it to UTF-8.
                         i.)   If the value successfully encodes as UTF-8 we set that as the value.
                         ii.)  If the value throws an exception encoding, we have illegal unicode characters in the
                               string that need to be addressed.  In order to escape these, we decode the string
                               as UTF-8 using the "replace" method to replace any illegal unicode characters
                               with '\ufffd' and set this back as the value after encoding again.
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
               3.)  If the value does not fall in one of the above use cases, we leave it as is and write it to the
                    xlsx without any modifications.

        Note:  It was noticed that blob objects are typically detected as isinstance of str here and strings are
               bytearray objects.  This needs to be investigated why exactly blob objects are coming out as str
               objects.

        Note:  Comparision should be done on how other applications work with different database text encodings in
               reference to their output.

        Note:  The decoding of the value in the database text encoding should only specify replace on a carved entry.

        :param csv_writer:
        :param version:
        :param master_schema_entry:
        :param b_tree_root_page:
        :param carved_cells:

        :return:

        """

        logger = getLogger(LOGGER_NAME)

        number_of_cells, cells = aggregate_leaf_cells(b_tree_root_page)

        if logger.isEnabledFor(DEBUG):
            master_schema_entry_string = "The {} b-tree page with {} row type and name: {} with sql: {} " \
                                         "has {} in-tact rows:"
            master_schema_entry_string = master_schema_entry_string.format(b_tree_root_page.page_type,
                                                                           master_schema_entry.row_type,
                                                                           master_schema_entry.name,
                                                                           master_schema_entry.sql, number_of_cells)
            logger.debug(master_schema_entry_string)

        column_headers = []
        column_headers.extend(["File Source", "Version", "Page Version", "Cell Source", "Page Number", "Location",
                               "Carved", "Status", "File Offset", "Row ID"])
        column_headers.extend([column_definition.column_name
                               for column_definition in master_schema_entry.column_definitions])

        logger.debug("Column Headers: {}".format(" , ".join(column_headers)))

        csv_writer.writerow(column_headers)

        sorted_cells = sorted(cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)

        for cell in sorted_cells:

            cell_record_column_values = []

            for record_column in cell.payload.record_columns:
                serial_type = record_column.serial_type
                text_affinity = True if serial_type >= 13 and serial_type % 2 == 1 else False
                value = record_column.value
                if value is None:
                    pass
                elif isinstance(value, (bytearray, str)):
                    value = value.decode(version.database_text_encoding, "replace") if text_affinity else str(value)
                    try:
                        value = value.encode(UTF_8)
                    except UnicodeDecodeError:
                        value = value.decode(UTF_8, "replace").encode(UTF_8)
                    value = ILLEGAL_XML_CHARACTER_PATTERN.sub(" ", value)
                    if value.startswith("="):
                        value = ' ' + value
                    value = str(value)
                cell_record_column_values.append(value)

            row = [version.file_type, cell.version_number, cell.page_version_number, cell.source, cell.page_number,
                   cell.location, False, "Complete", cell.file_offset, cell.row_id]
            row.extend(cell_record_column_values)
            csv_writer.writerow(row)

        if logger.isEnabledFor(DEBUG):
            for cell in sorted_cells:
                cell_record_column_values = [str(record_column.value) if record_column.value else "NULL"
                                             for record_column in cell.payload.record_columns]
                log_message = "File source: {} version: {} page version: {} cell source: {} page: {} location: {} " \
                              "carved: {} status: {} at file offset: {} for row id: {}: "
                log_message = log_message.format(version.file_type, cell.version_number, cell.page_version_number,
                                                 cell.source, cell.page_number, cell.location, False, "Complete",
                                                 cell.file_offset, cell.row_id)
                log_message += "(" + ", ".join(cell_record_column_values) + ")"
                logger.debug(log_message)

        VersionCsvExporter._write_b_tree_table_master_schema_carved_records(csv_writer, version, carved_cells, True)

    @staticmethod
    def _write_b_tree_table_master_schema_carved_records(csv_writer, version, carved_cells, has_row_ids):

        logger = getLogger(LOGGER_NAME)

        for carved_cell in carved_cells:

            cell_record_column_values = []

            for record_column in carved_cell.payload.record_columns:
                serial_type = record_column.serial_type
                text_affinity = True if serial_type >= 13 and serial_type % 2 == 1 else False
                value = record_column.value
                if value is None:
                    pass
                elif isinstance(value, (bytearray, str)):
                    value = value.decode(version.database_text_encoding, "replace") if text_affinity else str(value)
                    try:
                        value = value.encode(UTF_8)
                    except UnicodeDecodeError:
                        value = value.decode(UTF_8, "replace").encode(UTF_8)
                    value = ILLEGAL_XML_CHARACTER_PATTERN.sub(" ", value)
                    if value.startswith("="):
                        value = ' ' + value
                    value = str(value)
                cell_record_column_values.append(value)

            row = [version.file_type, carved_cell.version_number, carved_cell.page_version_number,
                   carved_cell.source, carved_cell.page_number, carved_cell.location, True, "Unknown",
                   carved_cell.file_offset]
            if has_row_ids:
                row.append("")
            row.extend(cell_record_column_values)
            csv_writer.writerow(row)

        if logger.isEnabledFor(DEBUG):
            for carved_cell in carved_cells:
                cell_record_column_values = [str(record_column.value) if record_column.value else "NULL"
                                             for record_column in carved_cell.payload.record_columns]
                log_message = "File source: {} version: {} version number: {} cell source: {} page: {} location: {} " \
                              "carved: {} status: {} at file offset: {}"
                log_message = log_message.format(version.file_type, carved_cell.version_number,
                                                 carved_cell.page_version_number, carved_cell.source,
                                                 carved_cell.page_number, carved_cell.location, True,
                                                 "Unknown", carved_cell.file_offset)
                if has_row_ids:
                    log_message += " for row id: {}:".format("")
                log_message += "(" + ", ".join(cell_record_column_values) + ")"
                logger.debug(log_message)


class CommitCsvExporter(object):

    def __init__(self, export_directory, file_name_prefix=""):
        self._export_directory = export_directory
        self._file_name_prefix = file_name_prefix
        self._csv_file_names = {}

    @property
    def csv_file_names(self):
        return self._csv_file_names

    def write_commit(self, master_schema_entry, commit):
        """
        Note:  This function only writes the commit record if the commit record was updated.
        :param master_schema_entry:
        :param commit:
        """

        if not commit.updated:
            return

        logger = getLogger(LOGGER_NAME)

        mode = "ab"
        csv_file_name = self._csv_file_names[commit.name] if commit.name in self._csv_file_names else None
        write_headers = False

        if not csv_file_name:
            mode = "wb"
            commit_name = sub(" ", "_", commit.name)
            csv_file_name = os.path.join(self._export_directory, (self._file_name_prefix + "-" + commit_name + ".csv"))
            self._csv_file_names[commit.name] = csv_file_name
            write_headers = True

        with open(csv_file_name, mode) as csv_file_handle:

            csv_writer = writer(csv_file_handle, delimiter=',', quotechar="\"", quoting=QUOTE_ALL)

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

                csv_writer.writerow(column_headers)

                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, commit.added_cells.values(), "Added")
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, commit.updated_cells.values(), "Updated")
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, commit.deleted_cells.values(), "Deleted")
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, commit.carved_cells.values(), "Carved")

            elif commit.page_type == PAGE_TYPE.B_TREE_TABLE_LEAF or commit.page_type == PAGE_TYPE.B_TREE_TABLE_INTERIOR:

                if write_headers:
                    column_headers.append("Row ID")
                    column_headers.extend([column_definition.column_name
                                           for column_definition in master_schema_entry.column_definitions])
                    csv_writer.writerow(column_headers)

                # Sort the added, updated, and deleted cells by the row id
                sorted_added_cells = sorted(commit.added_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, sorted_added_cells, "Added")
                sorted_updated_cells = sorted(commit.updated_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, sorted_updated_cells, "Updated")
                sorted_deleted_cells = sorted(commit.deleted_cells.values(), key=lambda b_tree_cell: b_tree_cell.row_id)
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, sorted_deleted_cells, "Deleted")

                # We will not sort the carved cells since row ids are not deterministic even if parsed
                CommitCsvExporter._write_cells(csv_writer, commit.file_type, commit.database_text_encoding,
                                               commit.page_type, commit.carved_cells.values(), "Carved")

            else:

                log_message = "Invalid commit page type: {} found for csv export on master " \
                              "schema entry name: {} while writing to csv file name: {}."
                log_message = log_message.format(commit.page_type, commit.name, csv_file_name)
                logger.warn(log_message)
                raise ExportError(log_message)

    @staticmethod
    def _write_cells(csv_writer, file_type, database_text_encoding, page_type, cells, operation):

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
                         i.)   If the value successfully encodes as UTF-8 we set that as the value.
                         ii.)  If the value throws an exception encoding, we have illegal unicode characters in the
                               string that need to be addressed.  In order to escape these, we decode the string
                               as UTF-8 using the "replace" method to replace any illegal unicode characters
                               with '\ufffd' and set this back as the value after encoding again.
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

        Note:  Comparision should be done on how other applications work with different database text encodings in
               reference to their output.

        Note:  The decoding of the value in the database text encoding should only specify replace on a carved entry.

        :param csv_writer:
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
                if value is None:
                    pass
                elif isinstance(value, (bytearray, str)):
                    value = value.decode(database_text_encoding, "replace") if text_affinity else str(value)
                    try:
                        value = value.encode(UTF_8)
                    except UnicodeDecodeError:
                        value = value.decode(UTF_8, "replace").encode(UTF_8)
                    value = ILLEGAL_XML_CHARACTER_PATTERN.sub(" ", value)
                    if value.startswith("="):
                        value = ' ' + value
                    value = str(value)
                cell_record_column_values.append(value)

            row = [file_type, cell.version_number, cell.page_version_number, cell.source, cell.page_number,
                   cell.location, operation, cell.file_offset]
            if page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:
                row.append(cell.row_id)
            row.extend(cell_record_column_values)
            csv_writer.writerow(row)
