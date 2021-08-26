from abc import ABCMeta
from abc import abstractmethod
from binascii import hexlify
from collections import namedtuple
from logging import getLogger
from re import match
from re import sub
from warnings import warn
from sqlite_dissect.constants import CREATE_TABLE_CLAUSE
from sqlite_dissect.constants import CREATE_VIRTUAL_TABLE_CLAUSE
from sqlite_dissect.constants import CREATE_INDEX_CLAUSE
from sqlite_dissect.constants import CREATE_UNIQUE_INDEX_CLAUSE
from sqlite_dissect.constants import INDEX_ON_COMMAND
from sqlite_dissect.constants import INDEX_WHERE_CLAUSE
from sqlite_dissect.constants import INTERNAL_SCHEMA_OBJECT_INDEX_PREFIX
from sqlite_dissect.constants import INTERNAL_SCHEMA_OBJECT_PREFIX
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_PAGE_HEX_ID
from sqlite_dissect.constants import MASTER_SCHEMA_COLUMN
from sqlite_dissect.constants import MASTER_SCHEMA_NUMBER_OF_COLUMNS
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import ORDINARY_TABLE_AS_CLAUSE
from sqlite_dissect.constants import SQLITE_MASTER_SCHEMA_ROOT_PAGE
from sqlite_dissect.constants import TABLE_CONSTRAINT_PREFACES
from sqlite_dissect.constants import VIRTUAL_TABLE_USING_CLAUSE
from sqlite_dissect.exception import MasterSchemaParsingError
from sqlite_dissect.exception import MasterSchemaRowParsingError
from sqlite_dissect.file.database.header import InteriorPageHeader
from sqlite_dissect.file.database.page import TableInteriorPage
from sqlite_dissect.file.database.page import TableLeafCell
from sqlite_dissect.file.database.page import TableLeafPage
from sqlite_dissect.file.database.utilities import get_pages_from_b_tree_page
from sqlite_dissect.file.schema.column import ColumnDefinition
from sqlite_dissect.file.schema.utilities import parse_comment_from_sql_segment
from sqlite_dissect.file.schema.table import TableConstraint
from sqlite_dissect.file.schema.utilities import get_index_of_closing_parenthesis
from sqlite_dissect.utilities import get_md5_hash

"""

master.py

This script holds the main objects used for parsing the master schema and master schema entries (ie. rows).

This script holds the following object(s):
MasterSchema(object)
MasterSchemaRow(object)
TableRow(MasterSchemaRow)
OrdinaryTableRow(TableRow)
VirtualTableRow(TableRow)
IndexRow(MasterSchemaRow)
ViewRow(MasterSchemaRow)
TriggerRow(MasterSchemaRow)

"""


class MasterSchema(object):

    MasterSchemaEntryData = namedtuple("MasterSchemaEntryData",
                                       "record_columns row_type sql b_tree_table_leaf_page_number cell")

    def __init__(self, version_interface, root_page):

        logger = getLogger(LOGGER_NAME)

        if root_page.number != SQLITE_MASTER_SCHEMA_ROOT_PAGE:
            log_message = "The root page number: {} is not the expected sqlite master schema root page number: {}."
            log_message = log_message.format(root_page.number, SQLITE_MASTER_SCHEMA_ROOT_PAGE)
            logger.error(log_message)
            raise ValueError(log_message)

        if root_page.hex_type != MASTER_PAGE_HEX_ID:
            log_message = "The root page hex type: {} is not the expected master page hex: {}."
            log_message = log_message.format(hexlify(root_page.hex_type), hexlify(MASTER_PAGE_HEX_ID))
            logger.error(log_message)
            raise ValueError(log_message)

        self._version_interface = version_interface

        self.version_number = self._version_interface.version_number
        self.page_version_number = self._version_interface.get_page_version(root_page.number)
        self.root_page = root_page
        self.master_schema_entries = []

        """

        The master schema entry data attribute below is a dictionary with up to four keys in it representing each of
        the four types of master schema entries: index, table, trigger, and view pointing to an array of row data
        where each entry is a MasterSchemaEntryData object describing an entry of that type.

        """

        database_text_encoding = self._version_interface.database_text_encoding

        if isinstance(self.root_page, TableInteriorPage):

            master_schema_entry_data = MasterSchema._parse_table_interior(self.root_page, database_text_encoding)

        elif isinstance(self.root_page, TableLeafPage):

            master_schema_entry_data = MasterSchema._parse_table_leaf(self.root_page, database_text_encoding)

        else:

            """

            Note:  This case should never occur since we checked above that the root page needs to start with the
                   master page hex id and a ValueError would have already been thrown if this was not true.  This
                   check is still done just in case.

            """

            log_message = "The root page is not a table page but is a: {}.".format(type(self.root_page))
            logger.error(log_message)
            raise ValueError(log_message)

        if not master_schema_entry_data:

            """

            There is the use case that no master schema entry data was found (ie. empty/no defined schema).

            Double check this use case by making sure the root page b-tree header has:
            1.) The number of cells on the page set to zero.
            2.) The cell content offset is equal to the page size (meaning there is no page content).
            3.) The b-tree table page is not an interior page (referring that it would have subpages with information).

            """

            b_tree_root_page_header = self.root_page.header

            if b_tree_root_page_header.number_of_cells_on_page != 0:
                log_message = "The b-tree root page header has a cell count of: {} where the master schema entry " \
                              "data was not set in version: {}."
                log_message = log_message.format(b_tree_root_page_header.number_of_cells_on_page, self.version_number)
                logger.error(log_message)
                raise MasterSchemaParsingError(log_message)

            if b_tree_root_page_header.cell_content_offset != self._version_interface.page_size:
                log_message = "The b-tree root page cell content offset is: {} when it should match the page " \
                              "size: {} where the master schema entry data was not set in version: {}."
                log_message = log_message.format(b_tree_root_page_header.cell_content_offset,
                                                 self._version_interface.page_size, self.version_number)
                logger.error(log_message)
                raise MasterSchemaParsingError(log_message)

            if isinstance(b_tree_root_page_header, InteriorPageHeader):
                log_message = "The b-tree root page is an interior table page where the master schema entry data " \
                              "was not set in version: {}."
                log_message = log_message.format(self.version_number)
                logger.error(log_message)
                raise MasterSchemaParsingError(log_message)

        """

        Next we create dictionaries for both tables and views.

        Table names are unique across both tables and views, however:
        1.) indexes can only be created on tables (not virtual tables or views)
        2.) views are built off of the tables
        3.) triggers can be built off of either tables and/or views but it is helpful to know which

        Therefore, we work with two dictionaries instead of one general table dictionary in the form of:
        dictionary[TABLE_NAME] = [MasterSchemaRow] where MasterSchemaRow will be either a TableRow or IndexRow
        depending on the respective dictionary.

        Note:  Virtual tables will be checked in a different manner to ensure no indexes have been created from it
               for validation purposes.

        """

        master_schema_tables = {}
        master_schema_views = {}

        if master_schema_entry_data:

            # Make sure the database text encoding is set.
            if not self._version_interface.database_text_encoding:
                log_message = "Master schema entries were found, however no database text encoding as been set yet " \
                              "as expected in version: {}."
                log_message = log_message.format(self.version_number)
                logger.error(log_message)
                raise MasterSchemaParsingError(log_message)

            """

            Due to the way each type is built off of each other, we create the entries in the following order:
            1.) Tables
            2.) Indexes
            3.) Views
            4.) Triggers

            Since information from tables in creating indexes is helpful (especially in generating signatures), tables
            are created first and then sent into the IndexRow class.  The specific table that belongs to the index being
            created is then pulled out and check in the IndexRow constructor.  This table is not pulled out ahead of
            time and sent in by itself since we don't have a good way to get to the index table name until the IndexRow
            is created itself.

            Next, all tables are sent into the ViewRow since a view can be made of multiple tables.

            Last, all tables and views are sent into the TriggerRow since a trigger can be across multiple tables
            and views.  Triggers can be defined on views.  Although INSERT, UPDATE, DELETE operations will not work
            on views, triggers will cause associated triggers to fire.

            """

            # Account for table master schema rows
            if MASTER_SCHEMA_ROW_TYPE.TABLE in master_schema_entry_data:
                for row_type_data in master_schema_entry_data[MASTER_SCHEMA_ROW_TYPE.TABLE]:

                    """

                    For tables, we have the choice of two types of tables.  The ordinary table and a virtual table.
                    There are two classes for these: OrdinaryTableRow and VirtualTableRow.  Both of these classes
                    extend the TableRow class but need to be specified differently since they both are parsed
                    differently.  We figure out what type of table we have by checking the beginning of the command.
                    If the command starts with CREATE_TABLE_COMMAND then the table is a create [ordinary] table
                    command and if it starts with CREATE_VIRTUAL_TABLE_COMMAND then the table is a virtual table.

                    Note:  Due to the way the rules work (documented in the table row classes themselves), the
                           create command at the beginning is always a set static command.  All capitals with single
                           spaces until the table name.  Therefore, we can be assured that these checks will work.

                    """

                    if row_type_data.sql.startswith(CREATE_TABLE_CLAUSE):
                        table_row = OrdinaryTableRow(self._version_interface,
                                                     row_type_data.b_tree_table_leaf_page_number,
                                                     row_type_data.cell, row_type_data.record_columns)
                    elif row_type_data.sql.startswith(CREATE_VIRTUAL_TABLE_CLAUSE):
                        table_row = VirtualTableRow(self._version_interface,
                                                    row_type_data.b_tree_table_leaf_page_number,
                                                    row_type_data.cell, row_type_data.record_columns)
                    else:
                        log_message = "Master schema table row with table name: {} has invalid sql: {}."
                        log_message = log_message.format(row_type_data.sql)
                        logger.error(log_message)
                        raise MasterSchemaParsingError(log_message)

                    if not table_row:
                        log_message = "Master schema table row was not set."
                        logger.error(log_message)
                        raise MasterSchemaParsingError(log_message)

                    self.master_schema_entries.append(table_row)
                    if table_row.table_name in master_schema_tables:
                        log_message = "Master schema table row with table name: {} was already specified in table rows."
                        log_message = log_message.format(table_row.table_name)
                        logger.error(log_message)
                        raise MasterSchemaParsingError(log_message)
                    master_schema_tables[table_row.table_name] = table_row

            # Account for index master schema rows
            if MASTER_SCHEMA_ROW_TYPE.INDEX in master_schema_entry_data:
                for row_type_data in master_schema_entry_data[MASTER_SCHEMA_ROW_TYPE.INDEX]:
                    index_row = IndexRow(self._version_interface, row_type_data.b_tree_table_leaf_page_number,
                                         row_type_data.cell, row_type_data.record_columns, master_schema_tables)
                    self.master_schema_entries.append(index_row)

            # Account for view master schema rows
            if MASTER_SCHEMA_ROW_TYPE.VIEW in master_schema_entry_data:
                for row_type_data in master_schema_entry_data[MASTER_SCHEMA_ROW_TYPE.VIEW]:
                    view_row = ViewRow(self._version_interface,
                                       row_type_data.b_tree_table_leaf_page_number,
                                       row_type_data.cell,
                                       row_type_data.record_columns,
                                       master_schema_tables)
                    self.master_schema_entries.append(view_row)
                    if view_row.table_name in master_schema_tables:
                        log_message = "Master schema view row with table name: {} was already specified in table rows."
                        log_message = log_message.format(view_row.table_name)
                        logger.error(log_message)
                        raise MasterSchemaParsingError(log_message)
                    if view_row.table_name in master_schema_views:
                        log_message = "Master schema view row with table name: {} was already specified in view rows."
                        log_message = log_message.format(view_row.table_name)
                        logger.error(log_message)
                        raise MasterSchemaParsingError(log_message)
                    master_schema_views[view_row.table_name] = view_row

            # Account for trigger master schema rows
            if MASTER_SCHEMA_ROW_TYPE.TRIGGER in master_schema_entry_data:
                for row_type_data in master_schema_entry_data[MASTER_SCHEMA_ROW_TYPE.TRIGGER]:
                    trigger_row = TriggerRow(self._version_interface, row_type_data.b_tree_table_leaf_page_number,
                                             row_type_data.cell, row_type_data.record_columns, master_schema_tables,
                                             master_schema_views)
                    self.master_schema_entries.append(trigger_row)

        self.master_schema_pages = get_pages_from_b_tree_page(self.root_page)
        self.master_schema_page_numbers = [master_schema_page.number for master_schema_page in self.master_schema_pages]

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_master_schema_root_page=True,
                  print_master_schema_entries=True, print_b_tree_root_pages=True):
        string = padding + "Version Number: {}\n" \
                 + padding + "Page Version Number: {}\n" \
                 + padding + "Master Schema Page Numbers: {}\n" \
                 + padding + "Master Schema Entries Length: {}\n" \
                 + padding + "Master Schema B-Tree Root Page Numbers: {}"
        string = string.format(self.version_number,
                               self.page_version_number,
                               self.master_schema_page_numbers,
                               len(self.master_schema_entries),
                               self.master_schema_b_tree_root_page_numbers)
        if print_master_schema_root_page:
            string += "\n" + padding + "Master Schema Root Page:\n{}"
            string = string.format(self.root_page.stringify(padding + "\t"))
        if print_master_schema_entries:
            for master_schema_entry in self.master_schema_entries:
                string += "\n" + padding + "Master Schema Entry:\n{}"
                string = string.format(master_schema_entry.stringify(padding + "\t"), print_b_tree_root_pages)
        return string

    @property
    def master_schema_b_tree_root_page_numbers(self):

        """

        This property will return a list of all of the root page numbers obtained from all master schema entries but
        only in the following cases:
        1.) The entry has a root page number and is not None.
        2.) The root page number is not 0.

        Therefore, if the entries were called manually and inspected, master schema entries that are not in
        this returned page number list may either be 0 or none.

        Note: Originally there was a method to retrieve root b-tree pages directly from the master schema.  This was
              changed by just having the master schema report the root page numbers and then have the client retrieve
              them as needed from the version interface itself.  In regards to pulling out the root pages the following
              note was made that still applies:

              Additional investigation needs to be done here to see and confirm exactly where the root page can be
              0 or None.  Right now we know of that according to the documentation "rows that define views,
              triggers, and virtual tables, the rootpage column is 0 or NULL".  We have seen that:
              1.) None seems to be used for triggers and views.
              2.) The root page number 0 seems to be used for virtual tables.

              Again additional investigation needs to be done here but these should be documented and checked.  It
              may be better to check this in the subclasses themselves instead of here (or both).

        :return: list  A list of int data types representing the root page numbers from the master schema entries.

        """

        return [entry.root_page_number for entry in self.master_schema_entries if entry.root_page_number]

    @staticmethod
    def _create_master_schema_entry_data_named_tuple(b_tree_table_leaf_page_number, cell, database_text_encoding):

        logger = getLogger(LOGGER_NAME)

        record_columns = dict(map(lambda x: [x.index, x], cell.payload.record_columns))

        if MASTER_SCHEMA_COLUMN.TYPE not in record_columns:
            log_message = "No type column found in record columns for cell index: {}.".format(cell.index)
            logger.error(log_message)
            raise MasterSchemaParsingError(log_message)

        if not record_columns[MASTER_SCHEMA_COLUMN.TYPE].value:
            log_message = "No type value set in type record column index: {} for cell index: {}."
            log_message = log_message.format(record_columns[MASTER_SCHEMA_COLUMN.TYPE].index, cell.index)
            logger.error(log_message)
            raise MasterSchemaParsingError(log_message)

        row_type = record_columns[MASTER_SCHEMA_COLUMN.TYPE].value.decode(database_text_encoding)

        if MASTER_SCHEMA_COLUMN.SQL not in record_columns:
            log_message = "No sql column found in record columns for cell index: {}.".format(cell.index)
            logger.error(log_message)
            raise MasterSchemaParsingError(log_message)

        """

        Note:  The value in the SQL record column may be None if there is a index internal schema object found.

        """

        sql_value = record_columns[MASTER_SCHEMA_COLUMN.SQL].value
        sql = sql_value.decode(database_text_encoding) if sql_value else None

        return MasterSchema.MasterSchemaEntryData(record_columns, row_type, sql, b_tree_table_leaf_page_number, cell)

    @staticmethod
    def _parse_table_interior(b_tree_table_interior_page, database_text_encoding):

        logger = getLogger(LOGGER_NAME)

        pages = [b_tree_table_interior_page.right_most_page]
        for b_tree_table_interior_cell in b_tree_table_interior_page.cells:
            pages.append(b_tree_table_interior_cell.left_child_page)

        """

        The master schema entry data attribute below is a dictionary with up to four keys in it representing each of
        the four types of master schema entries: index, table, trigger, and view pointing to an array of row data
        where each entry is a MasterSchemaEntryData object describing an entry of that type.

        """

        master_schema_entry_data = {}

        for page in pages:

            if isinstance(page, TableInteriorPage):
                returned_master_schema_entry_data = MasterSchema._parse_table_interior(page, database_text_encoding)
            elif isinstance(page, TableLeafPage):
                returned_master_schema_entry_data = MasterSchema._parse_table_leaf(page, database_text_encoding)
            else:
                log_message = "Invalid page type found: {} when expecting TableInteriorPage or TableLeafPage."
                log_message = log_message.format(type(page))
                logger.error(log_message)
                raise MasterSchemaParsingError(log_message)

            if not returned_master_schema_entry_data:
                log_message = "Returned master schema entry data was not set."
                logger.error(log_message)
                raise MasterSchemaParsingError(log_message)

            for row_type, row_type_data in returned_master_schema_entry_data.iteritems():
                if row_type in master_schema_entry_data:
                    master_schema_entry_data[row_type].extend(row_type_data)
                else:
                    master_schema_entry_data[row_type] = row_type_data

        return master_schema_entry_data

    @staticmethod
    def _parse_table_leaf(b_tree_table_leaf_page, database_text_encoding):

        logger = getLogger(LOGGER_NAME)

        """

        All leaf pages should have at least one cell entry in them unless they are the root page.  If the leaf page
        is the root page, it can have 0 cells indicating no schema.

        """

        if len(b_tree_table_leaf_page.cells) == 0 and b_tree_table_leaf_page.number != SQLITE_MASTER_SCHEMA_ROOT_PAGE:
            log_message = "Length of cells on leaf page is 0 and page number is: {}."
            log_message = log_message.format(b_tree_table_leaf_page.number)
            logger.error(log_message)
            raise MasterSchemaParsingError(log_message)

        """

        The master schema entry data attribute below is a dictionary with up to four keys in it representing each of
        the four types of master schema entries: index, table, trigger, and view pointing to an array of row data
        where each entry is a MasterSchemaEntryData object describing an entry of that type.

        """

        master_schema_entry_data = {}

        for cell in b_tree_table_leaf_page.cells:
            entry_data = MasterSchema._create_master_schema_entry_data_named_tuple(b_tree_table_leaf_page.number, cell,
                                                                                   database_text_encoding)
            if entry_data.row_type not in master_schema_entry_data:
                master_schema_entry_data[entry_data.row_type] = [entry_data]
            else:
                master_schema_entry_data[entry_data.row_type].append(entry_data)

        return master_schema_entry_data


class MasterSchemaRow(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, version_interface, b_tree_table_leaf_page_number, b_tree_table_leaf_cell, record_columns):

        logger = getLogger(LOGGER_NAME)

        if not isinstance(b_tree_table_leaf_cell, TableLeafCell):
            log_message = "Invalid cell type found: {} when expecting TableLeafCell."
            log_message = log_message.format(type(b_tree_table_leaf_cell))
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        self._version_interface = version_interface

        self.b_tree_table_leaf_page_number = b_tree_table_leaf_page_number
        self.version_number = self._version_interface.version_number
        self.page_version_number = self._version_interface.get_page_version(self.b_tree_table_leaf_page_number)

        self.row_id = b_tree_table_leaf_cell.row_id
        self.row_md5_hex_digest = b_tree_table_leaf_cell.md5_hex_digest
        self.record_md5_hex_digest = b_tree_table_leaf_cell.payload.md5_hex_digest
        self.record_columns = record_columns

        if len(self.record_columns) != MASTER_SCHEMA_NUMBER_OF_COLUMNS:
            log_message = "Invalid number of columns: {} when expected {} for row id: {} of row type: {} on page: {}."
            log_message = log_message.format(len(self.record_columns), MASTER_SCHEMA_NUMBER_OF_COLUMNS,
                                             self.row_id, self.row_type, self.b_tree_table_leaf_page_number)
            logger.error(log_message)
            MasterSchemaRowParsingError(log_message)

        if not self.record_columns[MASTER_SCHEMA_COLUMN.TYPE].value:
            log_message = "No master schema column row type value found for row id: {} of row type: {} on page: {}."
            log_message = log_message.format(self.row_id, self.row_type, self.b_tree_table_leaf_page_number)
            logger.error(log_message)
            MasterSchemaRowParsingError(log_message)

        if not self.record_columns[MASTER_SCHEMA_COLUMN.NAME].value:
            log_message = "No master schema column name value found for row id: {} of row type: {} on page: {}."
            log_message = log_message.format(self.row_id, self.row_type, self.b_tree_table_leaf_page_number)
            logger.error(log_message)
            MasterSchemaRowParsingError(log_message)

        if not self.record_columns[MASTER_SCHEMA_COLUMN.TABLE_NAME].value:
            log_message = "No master schema column table name value found for row id: {} of row type: {} on page: {}."
            log_message = log_message.format(self.row_id, self.row_type, self.b_tree_table_leaf_page_number)
            logger.error(log_message)
            MasterSchemaRowParsingError(log_message)

        # Get the database text encoding
        database_text_encoding = version_interface.database_text_encoding

        # The fields are read out as strings for better incorporation with calling classes when hashing since
        # if this is not done they are bytearray types and will be unhashable possibly throwing an exception.
        self.row_type = self.record_columns[MASTER_SCHEMA_COLUMN.TYPE].value.decode(database_text_encoding)
        self.name = self.record_columns[MASTER_SCHEMA_COLUMN.NAME].value.decode(database_text_encoding)
        self.table_name = self.record_columns[MASTER_SCHEMA_COLUMN.TABLE_NAME].value.decode(database_text_encoding)
        self.root_page_number = self.record_columns[MASTER_SCHEMA_COLUMN.ROOT_PAGE].value

        sql_value = self.record_columns[MASTER_SCHEMA_COLUMN.SQL].value
        self.sql = sql_value.decode(database_text_encoding) if sql_value else None

        self.sql_has_comments = False

        self.comments = []

        if self.sql:

            """

            Below describes the documentation and assumptions that have been made while parsing the schema.
            It is important to keep in mind that these may change in the future or might be different for
            older SQLite files.  Most of the files being test with are in the range of SQLite version 3.6 to 3.9.

            For the SQLITE_MASTER_TABLE_TYPE the table type could be a normal table or virtual table.
            The two SQL commands this would account for would be CREATE TABLE and CREATE VIRTUAL TABLE.

            According to the SQLite File Format Documentation, the following modifications are done to the
            SQL commands before storing them into the SQLite master table SQL column:
            1.) The CREATE, TABLE, VIEW, TRIGGER, and INDEX keywords at the beginning of the statement are
            converted to all upper case letters.
            2.) The TEMP or TEMPORARY keyword is removed if it occurs after the initial CREATE keyword.
            3.) Any database name qualifier that occurs prior to the name of the object being created is removed.
            4.) Leading spaces are removed.
            5.) All spaces following the first two keywords are converted into a single space.

            To note, number 5 above does not work as exactly worded.  The spaces are removed throughout all of
            main keywords to the table name.  After the table name, all spaces and capitalization are kept as
            entered.

            Due to this we don't have to check for the TEMP, TEMPORARY, or database name qualifier such as
            main.[DB NAME], temp.[DB NAME], etc.  These qualifiers only place the table into the corresponding
            opened database (schema name) and then removes this portion of the statement.  As a side note,
            temporary database files are stored in the temp directory of the user along with any additional files
            such as a rollback journal or WAL file.

            Also, virtual tables were not incorporated until SQLite version 3.8.2 and therefore will not appear
            in earlier version of SQLite.

            The statement "IF NOT EXISTS" is also removed but not documented in the above for table creation.
            Therefore, we do not need to check for this use case.

            """

            # Check if comments exist
            if self.sql.find("--") != -1 or self.sql.find("/*"):
                self.sql_has_comments = True

        """

        Below we make a unique identifier for this master schema entry.  This is build from all of the fields in the
        master schema entry except for the root page.

        Note:  All fields will have a value except for the SQL.  This could be None but "None" will just be used in
               the creation of the identifier.

        """

        master_schema_entry_identifier_string = "{}{}{}{}".format(self.row_id, self.row_type, self.name,
                                                                  self.table_name, self.sql)
        self.md5_hash_identifier = get_md5_hash(master_schema_entry_identifier_string)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_record_columns=True):
        string = padding + "Version Number: {}\n" \
                 + padding + "Page Version Number: {}\n" \
                 + padding + "B-Tree Table Leaf Page Number: {}\n" \
                 + padding + "Row ID: {}\n" \
                 + padding + "Row MD5 Hex Digest: {}\n" \
                 + padding + "Record MD5 Hex Digest: {}\n" \
                 + padding + "Row Type: {}\n" \
                 + padding + "Name: {}\n" \
                 + padding + "Table Name: {}\n" \
                 + padding + "Root Page Number: {}\n" \
                 + padding + "SQL: {}\n" \
                 + padding + "SQL Has Comments: {}\n" \
                 + padding + "MD5 Hash Identifier: {}"
        string = string.format(self.version_number,
                               self.page_version_number,
                               self.b_tree_table_leaf_page_number,
                               self.row_id,
                               self.row_md5_hex_digest,
                               self.record_md5_hex_digest,
                               self.row_type,
                               self.name,
                               self.table_name,
                               self.root_page_number,
                               self.sql,
                               self.sql_has_comments,
                               self.md5_hash_identifier)
        for comment in self.comments:
            string += "\n" + padding + "Comment: {}".format(comment)
        if print_record_columns:
            for index, record_column in self.record_columns.iteritems():
                string += "\n" \
                          + padding + "Record Column {}:\n{}:".format(index, record_column.stringify(padding + "\t"))
        return string

    @staticmethod
    def _get_master_schema_row_name_and_remaining_sql(row_type, name, sql, remaining_sql_command):

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        """

        This method can only be called on table or index types.

        """

        if row_type not in [MASTER_SCHEMA_ROW_TYPE.TABLE, MASTER_SCHEMA_ROW_TYPE.INDEX]:
            log_message = "Invalid row type: {} defined when parsing master schema row name: {} from sql: {} when " \
                          "type {} or {} was expected."
            log_message = log_message.format(row_type, name, sql,
                                             MASTER_SCHEMA_ROW_TYPE.TABLE, MASTER_SCHEMA_ROW_TYPE.INDEX)
            logger.error(log_message)
            raise ValueError(log_message)

        """

        Since the table or index name can be in brackets, backticks, single quotes, or double quotes, we check to
        make sure the table or index name is not in single or double quotes.  If it is, our job is fairly simple,
        otherwise we parse it normally.

        Note:  Characters like the '.' character are not allowed since it implies a schema.  However, if it is in
               brackets, backticks, or quotes (single or double), it is allowed.

        Note:  There may be comments following the table name preceding the column definitions, ie. "(...)", portion
               of the SQL.  If the table name has brackets, backticks, or quotes (single or double) around it,
               then this use case is handled in the way the table name is pulled out.  However, if there are not
               brackets, backticks, or quotes around the table name, the table name and remaining SQL have to be
               accounted for differently in the case that there are comments.

        Note:  SQLite allows backticks for compatibility with MySQL and allows brackets for compatibility with
               Microsoft databases.

        """

        if remaining_sql_command[0] == "[":

            # The table name or index name is surrounded by brackets
            match_object = match("^\[(.*?)\]", remaining_sql_command)

            if not match_object:
                log_message = "No bracket match found for {} name in sql for {} row name: {} and sql: {}."
                log_message = log_message.format(row_type, row_type, name, sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the parsed name and strip the brackets
            parsed_name = remaining_sql_command[match_object.start():match_object.end()].strip("[]")

            # Set the remaining sql
            remaining_sql_command = remaining_sql_command[match_object.end():]

            # Return the parsed name and remaining sql command
            return parsed_name, remaining_sql_command

        elif remaining_sql_command[0] == "`":

            # The table name or index name is surrounded by backticks
            match_object = match("^`(.*?)`", remaining_sql_command)

            if not match_object:
                log_message = "No backtick match found for {} name in sql for {} row name: {} and sql: {}."
                log_message = log_message.format(row_type, row_type, name, sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the parsed name and strip the backticks
            parsed_name = remaining_sql_command[match_object.start():match_object.end()].strip("`")

            # Set the remaining sql
            remaining_sql_command = remaining_sql_command[match_object.end():]

            # Return the parsed name and remaining sql command
            return parsed_name, remaining_sql_command

        elif remaining_sql_command[0] == "\'":

            # The table name or index name is surrounded by single quotes
            match_object = match("^\'(.*?)\'", remaining_sql_command)

            if not match_object:
                log_message = "No single quote match found for {} name in sql for {} row name: {} and sql: {}."
                log_message = log_message.format(row_type, row_type, name, sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the parsed name and strip the single quotes
            parsed_name = remaining_sql_command[match_object.start():match_object.end()].strip("\'")

            # Set the remaining sql
            remaining_sql_command = remaining_sql_command[match_object.end():]

            # Return the parsed name and remaining sql command
            return parsed_name, remaining_sql_command

        elif remaining_sql_command[0] == "\"":

            # The table name or index name is surrounded by double quotes
            match_object = match("^\"(.*?)\"", remaining_sql_command)

            if not match_object:
                log_message = "No double quote match found for {} name in sql for {} row name: {} and sql: {}."
                log_message = log_message.format(row_type, row_type, name, sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Set the parsed name and strip the double quotes
            parsed_name = remaining_sql_command[match_object.start():match_object.end()].strip("\"")

            # Set the remaining sql
            remaining_sql_command = remaining_sql_command[match_object.end():]

            # Return the parsed name and remaining sql command
            return parsed_name, remaining_sql_command

        else:

            # Iterate through the characters in the remaining sql command
            for index, character in enumerate(remaining_sql_command):

                """

                This works for both table and index since with indexes:
                1.) Indexes: Following the index name there has to be a newline, space or a comment indicator.
                             There is no use case for it to be anything else such as the opening parenthesis.
                2.) Tables:  Following the table name, there may or may not be a space between the table name and
                             opening parenthesis.  There may also be a comment (with or without a space) directly
                             after the table name.  Here will only care in the case it is a comment indicator directly
                             after the table name without a space.  We also check for newlines.

                Note:  This may be a bit more time consuming for virtual table module names since at this point you
                       could just parse out the name by finding the next " " character index as the ending index for
                       the name.

                Note:  A single "-" character is not allowed here as it is within the column definitions such as
                       default negative integer values, etc.

                """

                # See if the character is a single space or an opening parenthesis, or comment indicator
                if character == '\n' or character == ' ' or character == '(' or character == '-' or character == '/':

                    # Check to make sure the full comment indicators were found for "--" and "/*"
                    if (character == '-' and remaining_sql_command[index + 1] != '-') or \
                            (character == '/' and remaining_sql_command[index + 1] != '*'):

                        log_message = "Comment indicator '{}' found followed by an invalid secondary comment " \
                                      "indicator: {} found in {} name in sql for {} row name: {} and sql: {}."
                        log_message = log_message.format(character, remaining_sql_command[index + 1],
                                                         row_type, row_type, name, sql)
                        logger.error(log_message)
                        raise MasterSchemaRowParsingError(log_message)

                    # Set the table name or index name
                    parsed_name = remaining_sql_command[:index]

                    # Set the remaining sql
                    remaining_sql_command_start_offset = remaining_sql_command.index(parsed_name) + len(parsed_name)
                    remaining_sql_command = remaining_sql_command[remaining_sql_command_start_offset:]

                    # Return the parsed name and remaining sql command
                    return parsed_name, remaining_sql_command

                # See if the character is a "." since this would apply a schema name which we know shouldn't exist.
                elif character == '.':
                    log_message = "Invalid \'.\' character found in {} name in sql for " \
                                  "{} row name: {} and sql: {}."
                    log_message = log_message.format(row_type, row_type, name, sql)
                    logger.error(log_message)
                    raise MasterSchemaRowParsingError(log_message)

            """

            Note:  The index method could throw an exception if the table name or index name is not found but this
                   use case is ignored here since we just retrieved it from the remaining SQL command itself.

            """

            log_message = "No {} name found in sql for {} row name: {} and sql: {}."
            log_message = log_message.format(row_type, row_type, name, sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)


class TableRow(MasterSchemaRow):

    def __init__(self, version, b_tree_table_leaf_page_number, b_tree_table_leaf_cell, record_columns):

        # Call the superclass to initialize this object
        super(TableRow, self).__init__(version, b_tree_table_leaf_page_number, b_tree_table_leaf_cell, record_columns)

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        # Make sure this is the table row type after initialized by it's superclass
        if self.row_type != MASTER_SCHEMA_ROW_TYPE.TABLE:
            log_message = "Invalid row type: {} when expecting: {} with name: {}."
            log_message = log_message.format(self.row_type, MASTER_SCHEMA_ROW_TYPE.TABLE, self.name)
            logger.error(log_message)
            raise ValueError(log_message)

        """

        The SQL is always specified for tables (as well as triggers and views).  The majority of indexes also have
        the SQL specified.  However, "internal indexes" created by "unique" or "primary key" constraints on ordinary
        tables do not have SQL.

        """

        # The sql statement must exist for table rows
        if not self.sql:
            log_message = "SQL does not exist for table row with name: {}."
            log_message = log_message.format(self.name)
            logger.error(log_message)
            raise ValueError(log_message)

    def stringify(self, padding="", print_record_columns=True):
        return super(TableRow, self).stringify(padding, print_record_columns)

    @staticmethod
    def _get_module_name_and_remaining_sql(name, sql, remaining_sql_command):
        return MasterSchemaRow._get_master_schema_row_name_and_remaining_sql(MASTER_SCHEMA_ROW_TYPE.TABLE, name, sql,
                                                                             remaining_sql_command)


class OrdinaryTableRow(TableRow):

    def __init__(self, version, b_tree_table_leaf_page_number, b_tree_table_leaf_cell, record_columns):

        # Call the superclass to initialize this object
        super(OrdinaryTableRow, self).__init__(version, b_tree_table_leaf_page_number,
                                               b_tree_table_leaf_cell, record_columns)

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        # Make sure this is a create table statement
        if not self.sql.startswith(CREATE_TABLE_CLAUSE):
            log_message = "Invalid sql for create ordinary table statement: {} with name: {}."
            log_message = log_message.format(self.sql, self.name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Declare the column definitions and table constraints
        self.column_definitions = []
        self.table_constraints = []

        """

        Note:  The "without rowid" option can not be used in virtual tables.

        Note:  Virtual tables do not have any "internal schema objects".

        """

        self.without_row_id = False
        self.internal_schema_object = False

        # Retrieve the sql command to this table and replace all multiple spaces with a single space
        sql_command = sub("[\t\r\f\v ][\t\r\f\v ]+", " ", self.sql)

        # Set the create command offset to point to the end of the "create table" statement
        create_command_offset = len(CREATE_TABLE_CLAUSE)

        """

        We take off the "create table" beginning portion of the command here leaving the table name followed by
        the column definitions and table constraints with an optional "without rowid" at the end.

        Note:  The schema names are never included in the statements themselves since they just redirect which file
               the data will be stored in.  Schemas act more as file handles to open SQLite files in the driver.

        """

        # Left strip the "create table" command from the beginning of the create table statement removing any whitespace
        remaining_sql_command = str(sql_command[create_command_offset:]).lstrip()

        """

        We now parse through the remaining SQL command to find the table name.  Once we find the table name and set it,
        we remove the table name from the remaining SQL command.

        Note:  The table and/or column names may be in single or double quotes.  For example, quotes need to be used
               if a table name has spaces.  This is only seen in the SQL statement.  These quotes are removed in the
               name and table name fields.

        Note:  It was observed that there may be or may not be a space between the table name and opening parenthesis.

        Note:  There may also be a comment directly following the table name (with or without a space character) before
               the column definitions.  The SQL function checks for this use
               case but does not remove the comment from the returned string.  Therefore, it needs to be checked here
               for comments.

        Note:  The above was noticed with one of the sequence tables automatically created by SQLite in some use cases
               was parsed.  The following tables are examples of this in the documentation:
               1.) CREATE TABLE sqlite_sequence(name,seq);
               2.) CREATE TABLE sqlite_stat1(tbl,idx,stat);
               3.) CREATE TABLE sqlite_stat2(tbl,idx,sampleno,sample)
               4.) CREATE TABLE sqlite_stat3(tbl,idx,nEq,nLt,nDLt,sample)
               5.) CREATE TABLE sqlite_stat4(tbl,idx,nEq,nLt,nDLt,sample);

               These use cases are "internal schema objects" and any master schema objects with the name beginning
               with "sqlite_" these types of objects.  The prefix "sqlite_" used in the name of SQLite master schema
               rows is reserved for use by SQLite.

        Note:  There is no current use case of having "internal schema objects" for virtual tables and therefore
               no virtual table name will start with "sqlite_".

        """

        # Retrieve the table name and remaining sql after the table name is removed
        table_name, remaining_sql_command = \
            MasterSchemaRow._get_master_schema_row_name_and_remaining_sql(self.row_type, self.name, self.sql,
                                                                          remaining_sql_command)

        # Left strip the remaining sql command
        remaining_sql_command = remaining_sql_command.lstrip()

        # Make sure the table name was set which may not have if for some reason the remaining sql command
        # did not contain a single space character which would not be an acceptable create table statement
        if not table_name:
            log_message = "The table name was not set while parsing sql for table row name: {} and sql: {}."
            log_message = log_message.format(self.name, self.sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Check the table name is equal to the name as specified in the sqlite documentation
        if table_name.lower() != self.name.lower():
            log_message = "For table master schema row: {}, the derived table name: {} from the sql: {} " \
                          "does not match the name: {},"
            log_message = log_message.format(self.row_id, table_name, self.sql, self.name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Check the table name is equal to the table name as specified in the sqlite documentation
        if table_name.lower() != self.table_name.lower():
            log_message = "For table master schema row: {}, the derived table name: {} from the sql: {} " \
                          "does not match the table name: {},"
            log_message = log_message.format(self.row_id, table_name, self.sql, self.table_name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        """

        Check the table name to see if it is a internal schema object starting with "sqlite_".  More investigation is
        needed for these objects if there are any different use cases that may apply to them.  It appears that these
        can be parsed just as normal tables.  Therefore, we only throw an info message to the logging framework and
        continue on.

        """

        if self.table_name.startswith(INTERNAL_SCHEMA_OBJECT_PREFIX):
            self.internal_schema_object = True

            log_message = "Master schema ordinary table row found as internal schema object with name: {}, " \
                          "table name: {} and sql: {} and may have use cases that still need to be addressed."
            log_message = log_message.format(self.name, self.table_name, self.sql)
            logger.info(log_message)

        """

        The remaining SQL command must now either start with an opening parenthesis "(", a comment indicator, or "AS".
        Comment indicators would be either the "--" or "/*" character sequences.

        Note:  At this moment the "as [select-stmt]" is not addressed and if detected, a NotImplementedError
               will be thrown.

        Note:  Comments are parsed differently for each row.  In the case of a normal table row comments can be
               anywhere in the create table statement following the name.  Therefore the beginning statement:
               "CREATE TABLE [NAME]" cannot include any comments, but comments can directly follow the name,
               with or without a space.  It was also noted that comments will not appear after the ending ")"
               parenthesis after the column definitions unless "WITHOUT ROWID" is specified in which case they
               will occur even after the "WITHOUT ROWID" SQL.

        """

        # Check for comments after the table name, before the column definitions
        while remaining_sql_command.startswith(("--", "/*")):
            comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
            self.comments.append(comment.rstrip())
            remaining_sql_command = remaining_sql_command.lstrip()

        # See if the opening parenthesis is not the first character
        if remaining_sql_command.find("(") != 0:

            # Check if this remaining sql statement starts with "AS"
            if remaining_sql_command[:len(ORDINARY_TABLE_AS_CLAUSE)].upper() == ORDINARY_TABLE_AS_CLAUSE:
                log_message = "Create table statement has an \"AS\" clause for master schema table row with " \
                              "name: {} and sql: {} and is not implemented."
                log_message = log_message.format(self.name, self.sql)
                logger.error(log_message)
                raise NotImplementedError(log_message)

            # If the remaining sql statement does not hit the above two use cases then this is an erroneous statement
            else:
                log_message = "Create table statement has an unknown clause for master schema table row with " \
                              "name: {} and sql: {}."
                log_message = log_message.format(self.name, self.sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

        """

        Due to the checks above and the fact that the "AS" use case is not handled yet, we can be assured that
        this create statement remaining SQL command is now in the form of: "(...) ...".

        Next we will parse out the column definitions and table constraints between the "(" and ").  After this is done,
        we will investigate the trailing portion of the create statement past the closing parenthesis if it exists.

        Note: If the "AS" statement was used instead of the opening parenthesis here, the create table statement
              would be needed to be parsed differently and not in the form of: "(...) ...".  Due to this, there
              is not the same concept of a trailing portion of the create statement past the closing parenthesis.
              Instead the remaining statement following the "AS" would be a select statement and need to be parsed
              as such.

        """

        # The first thing is to get the closing parenthesis index to the column definitions and table constraints
        closing_parenthesis_index = get_index_of_closing_parenthesis(remaining_sql_command)

        # Declare the definitions to be the "(...)" section of the "(...) ..." explained above
        definitions = remaining_sql_command[:closing_parenthesis_index + 1]

        # Double check the definitions has a beginning opening parenthesis and ends with a closing parenthesis
        if definitions.find("(") != 0 or definitions.rfind(")") != len(definitions) - 1:
            log_message = "The definitions are not surrounded by parenthesis as expected for table row with name: {}" \
                          "and sql: {} with definitions: {}."
            log_message = log_message.format(self.name, self.sql, definitions)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Remove the beginning and ending parenthesis and left strip the string in case single whitespace characters
        # appear directly after the opening parenthesis and set it back to the definitions.  The characters before
        # the ending parenthesis are allowed since there could be a "\n" character corresponding to a "--" comment.
        definitions = definitions[1:len(definitions) - 1].lstrip()

        """

        At this point the column definitions, column constraints, and table constraints should be in the format of:
        ( column-name [[type-name] [column constraint]] [, ...,] [, table constraint] [, ...,] )
        where the brackets [] represent optional declaration and [, ...,] represents repeats of the previous argument.

        A definition can be a column definition or table constraint:
        1.) A column definition is in the form of: column-name [[type-name] [column constraint]]
            column-name [type-name] [COLUMN-CONSTRAINT ....]
        2.) A table constraint is in the form of: [table-constraint]
            [TABLE-CONSTRAINT ...]

        In order to parse the column definitions and table constraints we need to break them up in their respective
        segments.  Since parentheses and commas exist in their respective segments, we cannot simply do a split on
        a comma to divide up the sections.  In order to break up the sections correctly, we iterate through the
        definitions string looking for the commas but if we find an opening parenthesis, skip to the closing
        parenthesis ignoring commas if they exist as well as other characters in that portion of the string.  Also,
        if we find a quote character such as " or ', we need to skip to the following " or ' character.

        According to the documentation it appears that commas separate each segment defining the column definitions
        and table constraints and only appear within a pair of opening/closing parenthesis within the segment
        otherwise.  Therefore we do not make an assumption here, but raise an exception.

        As we move along parsing the individual segments, we check the beginning of each new section (minus leading
        whitespace that is removed) if it begins with one of the table constraint prefaces.  If it does, we know
        that is the end of the column definitions and the start of the table constraints.  From the first (if any)
        segment matches one of the table constraint prefaces, than that and any following definitions should all be
        table constraints and no more column definitions should show up.  If any of the following from the first table
        constraint here does not begin with a table constraint preface, than an exception will be thrown.

        To note, if the first definition found is a table constraint, than an exception will be thrown as well.  Also,
        at least one column definition must be present in the definitions in order to be a proper create statement.
        According to the documentation, this appears true and therefore if this use case is detected, an exception is
        thrown.

        Note:  When a table is created it must have at least one column.

        Note:  The above documentation does not account for comments.  Comments may be found anywhere within the
               definitions.  However, if quotes are used to define a default value, data type, etc. the comment is
               ignored.

               Example:  CREATE TABLE example (text_field "TEXT -- I am a text field")
                         In the above example, the data type is "TEXT -- I am a text field" which resolves to a TEXT
                         storage class and from SQLite's perspective, there is no comment.

               Note:  The above also gives merit to the following use case:

                      Example:  CREATE TABLE example (text_field "TEXT -- maintenance information")
                                In the above example, the storage class IS NOT TEXT.  It is INTEGER since "int" appears
                                in the string and is checked for first by SQLite when checking the storage class.

        Note:  If a value, or other field has a "," in it, it also gets ignored in the same manner if inside single or
               double quotes.  As an example usage, this was first noticed in the DEFAULT clause of a column definition
               which contained "," characters in the default text string.

        """

        # Make sure the definitions is not an empty string
        if not definitions:
            log_message = "No definitions parsed for the table row name: {} and sql: {}."
            log_message = log_message.format(self.name, self.sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Define a index for the column definitions and table constraints
        definition_index = 0

        # Define an index for the parsing the definitions and the beginning definition index
        character_index = 0
        beginning_definition_index = 0

        # Define a boolean for when the table constraints
        table_constraints_found = False

        # Initialize comments
        column_definition_comments = []

        # Iterate through all of the characters in the definitions
        while character_index < len(definitions):

            # Get the current indexed character
            character = definitions[character_index]

            """

            Check to make sure we are not encountering a comment.

            Note:  A single "-" is allowed since it can be before a negative default value for example in the create
                   statement.

            """

            if character == "-":

                # Check to make sure the full comment indicator was found for "--"
                if definitions[character_index + 1] == "-":
                    character_index = definitions.index("\n", character_index)

            elif character == "/":

                # Check to make sure the full comment indicator was found for "/*"
                if definitions[character_index + 1] != "*":
                    log_message = "Comment indicator '{}' found followed by an invalid secondary comment " \
                                  "indicator: {} found in {}."
                    log_message = log_message.format(character, definitions[character_index + 1], definitions)
                    logger.error(log_message)
                    raise MasterSchemaParsingError(log_message)

                character_index = definitions.index("*/", character_index) + 1

            """

            Below, we account for column definition comments that may have commas or parenthesis in them in order to
            make sure a particular portion of a comment doesn't cause the column definition to be parsed incorrectly.

            This is also done with backticks, single, and double quotes.

            Note:  SQLite allows backticks for compatibility with MySQL and allows brackets for compatibility with
                   Microsoft databases.

            """

            # Check if the character is an opening bracket, `, and skip to the closing single quote if so
            if character == "[":

                try:

                    # Set the character index to the closing bracket to this opening one
                    character_index = definitions.index("]", character_index + 1)

                except ValueError:

                    log_message = "No ending \"]\" character found in the definitions: {} starting from index: {} " \
                                  "while parsing the remaining sql: {} for the table row name: {}."
                    log_message = log_message.format(definitions, character_index + 1, remaining_sql_command, self.name)
                    logger.error(log_message)
                    raise

            # Check if the character is an opening backtick, `, and skip to the closing single quote if so
            if character == "`":

                try:

                    # Set the character index to the closing backtick to this opening one
                    character_index = definitions.index("`", character_index + 1)

                except ValueError:

                    log_message = "No ending \"`\" character found in the definitions: {} starting from index: {} " \
                                  "while parsing the remaining sql: {} for the table row name: {}."
                    log_message = log_message.format(definitions, character_index + 1, remaining_sql_command, self.name)
                    logger.error(log_message)
                    raise

            # Check if the character is an opening single quote, ', and skip to the closing single quote if so
            if character == "'":

                try:

                    # Set the character index to the closing single quote to this opening one
                    character_index = definitions.index("'", character_index + 1)

                except ValueError:

                    log_message = "No ending \"'\" character found in the definitions: {} starting from index: {} " \
                                  "while parsing the remaining sql: {} for the table row name: {}."
                    log_message = log_message.format(definitions, character_index + 1, remaining_sql_command, self.name)
                    logger.error(log_message)
                    raise

            # Check if the character is an opening double quote, ", and skip to the closing double quote if so
            if character == "\"":

                try:

                    # Set the character index to the closing double quote to this opening one
                    character_index = definitions.index("\"", character_index + 1)

                except ValueError:

                    log_message = "No ending \"\"\" character found in the definitions: {} starting from index: {} " \
                                  "while parsing the remaining sql: {} for the table row name: {}."
                    log_message = log_message.format(definitions, character_index + 1, remaining_sql_command, self.name)
                    logger.error(log_message)
                    raise

            # Check if the character is an opening parenthesis and skip to the closing parenthesis if so
            if character == "(":

                # Set the character index to the closing parenthesis to this opening one and increment the index
                character_index = get_index_of_closing_parenthesis(definitions, character_index)

            # If we find a closing parenthesis character than something went wrong and an exception is thrown
            elif character == ")":
                log_message = "An error occurred while parsing the remaining sql: {} for the table row name: {}."
                log_message = log_message.format(remaining_sql_command, self.name)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            """

            Above we update the index in the case that we find a opening parenthesis to the closing parenthesis index.
            Below we check if the character is a comma or at the end of the definition string in order to make the next
            and/or possibly final definition.

            1.) If the character is a comma then we know we reached that end of the portion of the definition in the
                definition string and there are more that follow.
            2.) If the character index + 1 == len(definitions) which means the character index is pointing to the last
                element in the array and on the while loop will break on the next iteration.  In this case we make the
                remaining segment into a definition.

            """

            # Check if we find a comma character and if so we know we reached the end of the current definition or
            # if the character index is either at the end of the definitions string.
            if character == "," or character_index + 1 == len(definitions):

                # Initialize a variable to add to the character index if comments are found after the comma
                ending_comments_length = 0

                # If the character index is one length than the length of the definitions (at the end of the
                # definitions string) then we want to increment it one in order to pick up the last character.
                # This is due to the array for strings being exclusive to the last index specified.
                if character_index + 1 == len(definitions):
                    character_index += 1

                # Check if there are comments if there was a comma
                else:

                    """

                    For column definitions and table constraints, we will only parse out the comments and send them
                    into the constructor if they start out the definition or directly follow a ",".  Any other comments,
                    will not be parsed here and will instead be sent into the column definition or table constraint
                    class for parsing.  This was decided to be the best way to associate comments based on location in
                    the create table statement based on location.

                    Example 1:  CREATE TABLE example_1 ( -- field for text
                                                         text_field)
                                Here the comment will be parsed out and sent in to the column definition constructor.

                    Example 2:  CREATE TABLE example_2 ( text_field, /* text field */ integer_field,
                                                        /* integer field */ )
                                Here the "/* text field */" comment will be sent in as a comment to the text_field
                                column definition.  The same will be true for the "/* integer field */" comment for the
                                integer_field.

                    Example 3:  CREATE TABLE example_3 ( text_field
                                                         -- field for text)
                                Here the comment will be included in the column definition string and not parsed as a
                                separate comment since there is no "," character even though it's on the next line.

                    Example 4:  CREATE TABLE example_4 ( text_field,
                                                         -- field for text
                                                         integer_field
                                                         -- field for integer)
                                Here, both comments will be sent in the column definition string for the integer_field
                                and not parsed separate since the first comment is after the "," and the second comment
                                is before (although no following fields are specified here) the next ",".  Even though
                                it can be seen that this may not be correct, the pattern above does not follow a
                                consistent pattern and is against what was considered the best way to parse schema
                                comments.

                    Example 5:  CREATE TABLE example_5 (text_field, -- field for text
                                                        /* this is a field for text */
                                                        integer_field -- field for integer)
                                Here, the "-- field for text" comment on the first line will be parsed and sent into the
                                column definition for the text_field.  However the "/* this is a field for text */"
                                comment will be parsed and sent into the second column definition.  The final comment
                                "-- field for integer" will be sent in along with the integer_field as part of the
                                column definition string.

                    In summation, comments right in the beginning or directly following a "," in the definitions will be
                    parsed separate and sent in through the constructor of the corresponding column definition or table
                    constraint.  Otherwise, the comment will be send in as part of the definition string to the
                    appropriate class and leave that class up to parse the inner comments to that definition.

                    Note:  The reason why comments preceding the definition had to be parsed was to pull out extra
                           content from the beginning of the column definition or table constraint in order to be able
                           to detect if it was a table constraint or not.

                    Note:  This means in the above form of parsing comments that there can be many "/* ... */" comments
                           as long as a "\n" does not appear following the ",".  This means that as soon as there is a
                           "-- ... \n" comment, the parsing will end.  This also means that there will always be at most
                           one "-- ... \n" comment and the end of the statement following the ",".

                    """

                    # Get the remaining definition past the comma
                    remaining_definition = definitions[character_index + 1:]
                    left_stripped_character_length = len(remaining_definition)
                    remaining_definition = sub("^[\t\r\f\v ]+", "", remaining_definition)
                    left_stripped_character_length -= len(remaining_definition)

                    # See if any comments in the form "/* ... */" exist and remove them if so (there may be 0 ... *)
                    while remaining_definition.startswith("/*"):
                        comment, remaining_definition = parse_comment_from_sql_segment(remaining_definition)
                        left_stripped_character_length += len(remaining_definition)
                        remaining_definition = remaining_definition.lstrip(" ")
                        left_stripped_character_length -= len(remaining_definition)
                        ending_comments_length += len(comment) + left_stripped_character_length
                        column_definition_comments.append(comment)

                    # See if any comments in the form "-- ... \n" exist and remove them if so (there may be 0 ... 1)
                    if remaining_definition.startswith("--"):
                        comment, remaining_definition = parse_comment_from_sql_segment(remaining_definition)
                        left_stripped_character_length += len(remaining_definition)
                        remaining_definition = remaining_definition.lstrip(" ")
                        left_stripped_character_length -= len(remaining_definition)
                        ending_comments_length += len(comment) + left_stripped_character_length
                        column_definition_comments.append(comment)

                # Initialize a current definition index to validate against later
                current_definition_index = definition_index

                # Get the definition string and strip the beginning characters since we do not need any
                # default whitespace characters there, but may need them at the end (for example in the case
                # of a "--" comment that ends in "\n".
                definition = definitions[beginning_definition_index:character_index].lstrip()

                # Check for comments after the beginning of the definition
                while definition.startswith(("--", "/*")):
                    comment, remaining_definition = parse_comment_from_sql_segment(definition)
                    column_definition_comments.append(comment.rstrip())
                    definition = remaining_definition.lstrip()

                # Iterate through the table constraint prefaces and make sure none of them start off the definition
                for table_constraint_preface in TABLE_CONSTRAINT_PREFACES:

                    # Make sure the length of the definition is at least as long as the table constraint preface
                    if len(definition) >= len(table_constraint_preface):

                        """

                        Note: Even though the column and table constraint share some of the same prefaces for
                              their constraints, this check is safe since the column definitions will never
                              start out directly with a column constraint preface name that could be confused with
                              a table constraint preface name.

                        Note: When the check is done on the definition, we check the next character is not one of the
                              allowed characters in a column name to make sure the constraint preface is not the
                              beginning of a longer column name where it is not actually a constraint preface
                              (example: primaryEmail).  The "\w" regular expression when no LOCALE and UNICODE flags
                              are set will be equivalent to the set: [a-zA-Z0-9_].

                        """

                        # Check to see if the definition starts with the table constraint preface
                        if definition[:len(table_constraint_preface)].upper() == table_constraint_preface:

                            if not (len(table_constraint_preface) + 1 <= len(definition)
                                    and match("\w", definition[len(table_constraint_preface)])):

                                # We have found a table constraint here and make sure this is not the first definition
                                if definition_index == 0:

                                    # The first definition is a table constraint which should not occur
                                    log_message = "First definition found: {} in table row with name: {} and sql: {} " \
                                                  "is a table constraint."
                                    log_message = log_message.format(definition[:len(table_constraint_preface)],
                                                                     self.name, self.sql)
                                    logger.error(log_message)
                                    raise MasterSchemaRowParsingError(log_message)

                                # The definition is a table constraint and not the first definition
                                else:

                                    """

                                    Note: Since we are here we assume the first column definition has already been made
                                          because at least one of them had to be parsed successfully before reaching
                                          this portion of the code.  Therefore no additional checks need to be done
                                          for checking at least one column definition existing.

                                    """

                                    # Create the table constraint
                                    self.table_constraints.append(TableConstraint(definition_index, definition,
                                                                                  column_definition_comments))

                                    # Set the table constraints found variable to true now
                                    table_constraints_found = True

                                    # Reinitialize the comments
                                    column_definition_comments = []

                                    # Increment the definition index
                                    definition_index += 1

                """

                After each parsing of the definition we check if that was a table constraint.  If it was we make sure
                that the first table constraint and all ones following it are.  If this iteration is not a table
                constraint, that means no table constraints should have been found yet and it is a normal column
                definition.

                """

                # Check if table constraint has not been found yet (previously or on this iteration)
                if not table_constraints_found:

                    """

                    This definition is a column definition.

                    Make sure the index was not incremented since no table constraint was made.

                    """

                    # Make sure the definition index has not changed
                    if current_definition_index != definition_index:
                        log_message = "The definition index: {} was updated indicating a table constraint was " \
                                      "made when it should be: {} for a column definition in table row with " \
                                      "name: {} and sql: {}."
                        log_message = log_message.format(definition_index, current_definition_index,
                                                         self.name, self.sql)
                        logger.error(log_message)
                        raise MasterSchemaRowParsingError(log_message)

                    # Create the column definition
                    self.column_definitions.append(ColumnDefinition(definition_index, definition,
                                                                    column_definition_comments))

                    # Reinitialize the comments to the next segments columns
                    column_definition_comments = []

                    # Increment the definition index
                    definition_index += 1

                # Make sure the table constraint was made
                else:

                    """

                    This definition is a table constraint.

                    Make sure the index was incremented since the table constraint was made.

                    """

                    # Check that the definition index was incremented meaning a table constraint was made
                    if current_definition_index + 1 != definition_index:
                        log_message = "The definition index: {} was not updated indicating a column definition was " \
                                      "made when it should be: {} for a table constraint in table row with " \
                                      "name: {} and sql: {}."
                        log_message = log_message.format(definition_index, current_definition_index + 1,
                                                         self.name, self.sql)
                        logger.error(log_message)
                        raise MasterSchemaRowParsingError(log_message)

                # Update the beginning definition and character indexes
                character_index += ending_comments_length + 1
                beginning_definition_index = character_index

            # The character is just a normal character
            else:

                # Increment the character index
                character_index += 1

        """

        Lastly, if there is remaining SQL, we check to make sure it is the "without rowid" statement.  If it is not,
        then an exception will be thrown since that is the only use case allowed here according to the SQLite
        documentation.

        """

        # Last get the remaining sql command to check for the "without rowid" use case
        remaining_sql_command = remaining_sql_command[closing_parenthesis_index + 1:].lstrip()

        # See if the remaining sql command has any content left
        if len(remaining_sql_command) != 0:

            """

            Note:  Below we check for comments before, in between and after the "without rowid" statement.  We only
                   check for comments assuming we have the "without rowid" specified.  This is due to the fact that
                   if the "without rowid" is not specified, any comments following the end of the column definitions
                   are ignored in the create table statement by SQLite.  Only when "without rowid" is specified, are
                   comments recognized.

            """

            # Check for comments after the end of the column definitions before the "without rowid"
            while remaining_sql_command.startswith(("--", "/*")):
                comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
                self.comments.append(comment.rstrip())
                remaining_sql_command = remaining_sql_command.lstrip()

            # If there is content left, check if it is the "without rowid" string by seeing if it starts with "without"
            if remaining_sql_command.upper().startswith("WITHOUT"):

                remaining_sql_command = remaining_sql_command[len("WITHOUT"):].lstrip()

                # Check for comments after the end of the column definitions before the "without rowid"
                while remaining_sql_command.startswith(("--", "/*")):
                    comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
                    self.comments.append(comment.rstrip())
                    remaining_sql_command = remaining_sql_command.lstrip()

                if remaining_sql_command.upper().startswith("ROWID"):

                    remaining_sql_command = remaining_sql_command[len("ROWID"):].lstrip()

                    # Set the without row id variable to true
                    self.without_row_id = True

                    # Check for comments at the end
                    while remaining_sql_command.startswith(("--", "/*")):
                        comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
                        self.comments.append(comment.rstrip())
                        remaining_sql_command = remaining_sql_command.lstrip()

                    # Make sure we are at the end
                    if len(remaining_sql_command) != 0:
                        log_message = "Invalid sql ending: {} found when nothing more expected in " \
                                      "table row with name: {} and sql: {}."
                        log_message = log_message.format(remaining_sql_command, self.name, self.sql)
                        logger.error(log_message)
                        raise MasterSchemaRowParsingError(log_message)

                else:
                    log_message = "Invalid sql ending: {} found after \"WITHOUT\" when \"ROWID\" expected in " \
                                  "table row with name: {} and sql: {}."
                    log_message = log_message.format(remaining_sql_command, self.name, self.sql)
                    logger.error(log_message)
                    raise MasterSchemaRowParsingError(log_message)

            # The remaining string is not the "without rowid" string which, according to sqlite documentation,
            # should not occur
            else:
                log_message = "Invalid sql ending: {} found in table row with name: {} and sql: {}."
                log_message = log_message.format(remaining_sql_command, self.name, self.sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

        """

        Until the "without rowid" is fully implemented, we will throw a warning here.  Tables without a row id have
        all of their data stored in index b-tree pages rather than table b-tree pages.  Also, the ordering of the
        columns are switched around depending on what field(s) the primary key is comprised of and where those fields
        are in the column definitions.

        """

        if self.without_row_id:
            log_message = "A table specified without a row id was found in table row with name: {} and sql: {}.  " \
                          "This use case is not fully implemented."
            log_message = log_message.format(self.name, self.sql)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

    def stringify(self, padding="", print_record_columns=True,
                  print_column_definitions=True, print_table_constraints=True):
        string = "\n" \
                 + padding + "Without Row ID: {}\n" \
                 + padding + "Internal Schema Object: {}\n" \
                 + padding + "Column Definitions Length: {}\n" \
                 + padding + "Table Constraints Length: {}"
        string = string.format(self.without_row_id,
                               self.internal_schema_object,
                               len(self.column_definitions),
                               len(self.table_constraints))
        string = super(OrdinaryTableRow, self).stringify(padding, print_record_columns) + string
        if print_column_definitions:
            for column_definition in self.column_definitions:
                string += "\n" \
                          + padding + "Column Definition:\n{}".format(column_definition.stringify(padding + "\t"))
        if print_table_constraints:
            for table_constraint in self.table_constraints:
                string += "\n" \
                          + padding + "Table Constraint:\n{}".format(table_constraint.stringify(padding + "\t"))
        return string


class VirtualTableRow(TableRow):

    def __init__(self, version, b_tree_table_leaf_page_number, b_tree_table_leaf_cell, record_columns):

        # Call the superclass to initialize this object
        super(VirtualTableRow, self).__init__(version, b_tree_table_leaf_page_number,
                                              b_tree_table_leaf_cell, record_columns)

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        # Make sure this is a create virtual table statement
        if not self.sql.startswith(CREATE_VIRTUAL_TABLE_CLAUSE):
            log_message = "Invalid sql for create virtual table statement: {} with name: {}."
            log_message = log_message.format(self.sql, self.name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        """

        Note:  The "without rowid" option can not be used in virtual tables.

        Note:  Virtual tables do not have any "internal schema objects".

        """

        # Retrieve the sql command to this table and replace all multiple spaces with a single space
        sql_command = sub("[\t\r\f\v ][\t\r\f\v ]+", " ", self.sql)

        # Set the create command offset to point to the end of the "create virtual table" statement
        create_command_offset = len(CREATE_VIRTUAL_TABLE_CLAUSE)

        """

        We take off the "create virtual table" beginning portion of the command here leaving the table name followed by
        the "using" statement and then the module arguments.

        Note:  The schema names are never included in the statements themselves since they just redirect which file
               the data will be stored in.  Schemas act more as file handles to open sqlite files in the driver.

        """

        # Left strip the "create table" command from the beginning of the create table statement removing any whitespace
        remaining_sql_command = str(sql_command[create_command_offset:]).lstrip()

        """

        We now parse through the remaining SQL command to find the table name.  Once we find the table name and set it,
        we remove the table name from the remaining SQL command.

        Note:  The table and/or column names may be in single or double quotes.  For example, quotes need to be used
               if a table name has spaces.  This is only seen in the SQL statement.  These quotes are removed in the
               name and table name fields.

        Note:  It was observed that there may be or may not be a space between the table name and opening parenthesis.

        Note:  The above was noticed with one of the sequence tables automatically created by SQLite in some use cases
               was parsed.  The following tables are examples of this in the documentation:
               1.) CREATE TABLE sqlite_sequence(name,seq);
               2.) CREATE TABLE sqlite_stat1(tbl,idx,stat);
               3.) CREATE TABLE sqlite_stat2(tbl,idx,sampleno,sample)
               4.) CREATE TABLE sqlite_stat3(tbl,idx,nEq,nLt,nDLt,sample)
               5.) CREATE TABLE sqlite_stat4(tbl,idx,nEq,nLt,nDLt,sample);

               These use cases are "internal schema objects" and any master schema objects with the name beginning
               with "sqlite_" these types of objects.  The prefix "sqlite_" used in the name of SQLite master schema
               rows is reserved for use by SQLite.

        Note:  There is no current use case of having "internal schema objects" for virtual tables and therefore
               no virtual table name will start with "sqlite_".

        """

        # Retrieve the table name and remaining sql after the table name is removed
        table_name, remaining_sql_command = \
            MasterSchemaRow._get_master_schema_row_name_and_remaining_sql(self.row_type, self.name, self.sql,
                                                                          remaining_sql_command)

        # Left strip the remaining sql command
        remaining_sql_command = remaining_sql_command.lstrip()

        # Check the table name is equal to the name as specified in the sqlite documentation
        if table_name.lower() != self.name.lower():
            log_message = "For virtual table master schema row: {}, the derived table name: {} from the sql: {} " \
                          "does not match the name: {},"
            log_message = log_message.format(self.row_id, table_name, self.sql, self.name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Check the table name is equal to the table name as specified in the sqlite documentation
        if table_name.lower() != self.table_name.lower():
            log_message = "For virtual table master schema row: {}, the derived table name: {} from the sql: {} " \
                          "does not match the table name: {},"
            log_message = log_message.format(self.row_id, table_name, self.sql, self.table_name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        """

        Check the virtual table name to see if it is a internal schema object starting with "sqlite_".  Since this 
        is not expected for a virtual table, a error will be raised if detected.

        """

        if self.table_name.startswith(INTERNAL_SCHEMA_OBJECT_PREFIX):
            log_message = "Master schema virtual table row found as internal schema object with name: {}, " \
                          "table name: {} and sql: {} which should not occur."
            log_message = log_message.format(self.name, self.table_name, self.sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        """

        The remaining SQL command must now either start with "using" which may be mixed-case or an opening
        parenthesis "(", or a comment indicator.  Comment indicators would be either the "--" or "/*" character
        sequences.

        """

        # Check for comments after the virtual table name, before the using clause
        while remaining_sql_command.startswith(("--", "/*")):
            comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
            self.comments.append(comment.rstrip())
            remaining_sql_command = remaining_sql_command.lstrip()

        # Declare the module arguments
        self.module_arguments = []

        # Check if this remaining sql statement starts with "AS"
        if remaining_sql_command[:len(VIRTUAL_TABLE_USING_CLAUSE)].upper() != VIRTUAL_TABLE_USING_CLAUSE:
            log_message = "Create virtual table statement does not have a \"USING\" clause for master schema " \
                          "table row with name: {} and sql: {}."
            log_message = log_message.format(self.name, self.sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Remove the using prefix and left strip any whitespace
        remaining_sql_command = remaining_sql_command[len(VIRTUAL_TABLE_USING_CLAUSE):].lstrip()

        # Check for comments after the using clause, before the module name
        while remaining_sql_command.startswith(("--", "/*")):
            comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
            self.comments.append(comment.rstrip())
            remaining_sql_command = remaining_sql_command.lstrip()

        # Declare the module arguments
        self.module_arguments = []

        # Retrieve the module name and remaining sql after the module name is removed
        self.module_name, remaining_sql_command = TableRow._get_module_name_and_remaining_sql(self.name, self.sql,
                                                                                              remaining_sql_command)

        # Left strip the remaining  sql command
        remaining_sql_command = remaining_sql_command.lstrip()

        # Check for comments after the module name, before the module arguments
        while remaining_sql_command.startswith(("--", "/*")):
            comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
            self.comments.append(comment.rstrip())
            remaining_sql_command = remaining_sql_command.lstrip()

        # Declare the module arguments
        self.module_arguments = []

        """

        At this point the remaining portion of the SQL command should be in the form of "( module-argument, ... )".

        """

        # The first thing is to get the closing parenthesis index to the module arguments
        closing_parenthesis_index = get_index_of_closing_parenthesis(remaining_sql_command)

        # Declare the arguments to be the "(...)" section
        arguments = remaining_sql_command[:closing_parenthesis_index + 1]

        # Double check the module arguments has a beginning opening parenthesis and ends with a closing parenthesis
        if arguments.find("(") != 0 or arguments.rfind(")") != len(arguments) - 1:
            log_message = "The arguments are not surrounded by parenthesis as expected for table row with name: {}" \
                          "and sql: {} with arguments: {}."
            log_message = log_message.format(self.name, self.sql, arguments)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Remove the beginning and ending parenthesis and left strip the string in case single whitespace characters
        # appear directly after the opening parenthesis and set it back to the definitions.  The characters before
        # the ending parenthesis are allowed since there could be a "\n" character corresponding to a "--" comment.

        """

        The next step here is to parse and strip the module arguments and continue parsing:
        The next step here is the strip the arguments of the parenthesis and continue parsing the module arguments:
        arguments = arguments[1:len(arguments) - 1].lstrip()
        ...

        Support for virtual table modules and module arguments is not yet implemented.

        """

        """
        
        At this point we have the SQL down to the remaining module arguments.  Since the module arguments are different
        depending on the module, many use cases will need to be investigated and addressed.  For now a warning is
        thrown that a virtual table was found.

        """

        log_message = "Virtual table name: {} was found with module name: {} and sql: {}.  Virtual table modules are " \
                      "not fully implemented."
        log_message = log_message.format(self.name, self.module_name, self.sql)
        logger.warn(log_message)
        warn(log_message, RuntimeWarning)

        """

        The last thing to do is make sure there is nothing remaining in the SQL after the closing parenthesis of the
        module arguments.

        Note:  Similarly, like the create table statement, any comments placed after the module name (when there are no
               module arguments), or the module arguments, are ignored by SQLite.

        """

        # Last get the remaining sql command to check for the "without rowid" use case
        remaining_sql_command = remaining_sql_command[closing_parenthesis_index + 1:].lstrip()

        # See if the remaining sql command has any content left
        if len(remaining_sql_command) != 0:
            log_message = "Additional content found in virtual table sql after module arguments in table row" \
                          "with name: {} found with module name: {} and sql: {}."
            log_message = log_message.format(self.name, self.module_name, self.sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

    def stringify(self, padding="", print_record_columns=True, print_module_arguments=True):
        string = "\n" \
                 + padding + "Module Name: {}\n" \
                 + padding + "Module Arguments Length: {}"
        string = string.format(self.module_name,
                               len(self.module_arguments))
        string = super(VirtualTableRow, self).stringify(padding, print_record_columns) + string
        if print_module_arguments:
            for module_argument in self.module_arguments:
                string += "\n" \
                          + padding + "Module Argument:\n{}".format(module_argument.stringify(padding + "\t"))
        return string


class IndexRow(MasterSchemaRow):

    def __init__(self, version_interface, b_tree_table_leaf_page_number,
                 b_tree_table_leaf_cell, record_columns, tables):

        super(IndexRow, self).__init__(version_interface, b_tree_table_leaf_page_number,
                                       b_tree_table_leaf_cell, record_columns)

        # Initialize the logger
        logger = getLogger(LOGGER_NAME)

        # Make sure this is the index row type after initialized by it's superclass
        if self.row_type != MASTER_SCHEMA_ROW_TYPE.INDEX:
            log_message = "Invalid row type: {} when expecting: {} with name: {}."
            log_message = log_message.format(self.row_type, MASTER_SCHEMA_ROW_TYPE.INDEX, self.name)
            logger.error(log_message)
            raise ValueError(log_message)

        """

        Three boolean fields are declared below:

        1.) internal_schema_object:
        An internal schema object is if the index is created by SQLite implicitly through the create table statement
        such as a primary key or unique constraint.

        2.) unique
        If the index is not an internal schema object then it is either a regular index or a unique index.  The unique
        index only enforces that duplicates are not allowed.

        Note:  NULL values are considered unique to each other in SQLite, therefore there may be multiple NULL values
               in any index including unique indexes.

        3.) partial_index:
        An index where the WHERE clause is found is a partial index.  In ordinary indexes, there is exactly one entry
        in the index for every row in the table but in partial indexes only some subset of the rows in the table have
        corresponding index entries.  For example where a value is not null resulting in a index where only non-null
        values have the index over them.

        """

        self.internal_schema_object = False
        self.unique = False
        self.partial_index = False

        # Check if this index is an internal schema object
        if self.name.startswith(INTERNAL_SCHEMA_OBJECT_PREFIX):
            self.internal_schema_object = True

        """

        Note:  Currently the only internal schema objects for indexes begin with "sqlite_autoindex_" according
               to SQLite documentation from version 3.9.2.  Therefore, if any index starts with "sqlite_" but
               without the following "autoindex_" portion, an error will be raised.

        """

        if self.internal_schema_object and not self.name.startswith(INTERNAL_SCHEMA_OBJECT_INDEX_PREFIX):
            log_message = "Internal schema object detected but invalid prefix for index row with name: {}."
            log_message = log_message.format(self.name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        """

        If this index is an internal schema object index, then it will have no SQL.

        """

        if self.internal_schema_object and self.sql:
            log_message = "Internal schema object detected for index row with name: {} but found sql: {}."
            log_message = log_message.format(self.name, self.sql)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        if not self.internal_schema_object and not self.sql:
            log_message = "Index row with name: {} found with no sql and is not an internal schema object."
            log_message = log_message.format(self.name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        # Make sure the table name this index refers to is in the tables and retrieve that table row.
        if self.table_name not in tables:
            log_message = "Index row with name: {} and table name: {} has not correlating table in the tables."
            log_message = log_message.format(self.name, self.table_name)
            logger.error(log_message)
            raise MasterSchemaRowParsingError(log_message)

        table_row = tables[self.table_name]

        if table_row.without_row_id:
            log_message = "Index row with name: {} and table name: {} was found to rely on a table without a row id."
            log_message = log_message.format(self.name, self.table_name)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        """

        Since internal schema object do not have SQL, we need to handle internal schema object differently.
        Internal schema objects need to have their names parsed rather than SQL.  For index internal schema objects,
        the name are of the form "sqlite_autoindex_TABLE_N" where table is the table name they refer to (this should
        also match the table name) and N is the index of the primary or unique constraint as defined in the schema.

        Note:  The INTEGER PRIMARY KEY does not get an index.  For older versions of SQLite it would get a
               "sqlite_sequence" table created for it if it did not already exist, but this is no longer done unless
               the AUTOINCREMENT clause is added which is not recommended per the SQLite documentation.  However,
               it has been noticed that there are cases where there may be a INTEGER PRIMARY KEY UNIQUE clause on a
               column which would cause a unique index internal schema object to be made.  This could be confusing
               since the naming nomenclature would be the same for either primary key or unique and may at first
               appear to be a created primary key index internal schema object.

        Note:  Index internal schema objects are created as side affects to create table statements.  A index internal
               schema object can not be created outside the create table statement.

        """

        if self.internal_schema_object:

            """

            Note:  An index internal schema object will not be a partial index but may be unique depending on the
                   clause that created it from the create table statement.

            """

            """

            Until the index internal schema objects are fully implemented, we will throw a warning here.  The index
            internal schema objects are only made on primary key or unique constraints created in the table according
            to current documentation as of SQLite 3.9.2.  These names are in teh form of "sqlite_autoindex_TABLE_N"
            where TABLE is the table name the auto index belongs to (which should also be mirrored in the table name)
            and N is the counter for where it appears in the create statement.

            """

            log_message = "A index internal schema object found in index row with name: {} " \
                          "and sql: {}.  This is not fully implemented and may cause issues with index pages."
            log_message = log_message.format(self.name, self.sql)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        else:

            # Retrieve the sql command to this table and replace all multiple spaces with a single space
            sql_command = sub("[\t\r\f\v ][\t\r\f\v ]+", " ", self.sql)

            """

            At the beginning of the create index statement there can be two use cases to account for:
            1.) CREATE INDEX [INDEX_NAME] ...
            2.) CREATE UNIQUE INDEX [INDEX_NAME] ...

            The spacing and capitalization will always match one of the two the create [...] index statements above due
            to the way SQLite works with the SQL.  (Also, see documentation in the MasterSchemaRow class.)

            The unique only means that the index is unique and there may not be more than one index in this set that
            is equivalent.  Keep in mind NULL values considered unique to each other in SQLite.  This use case does
            not concern us since we are merely parsing the data, creating signatures, carving data, etc.  We are not
            adding to the index here and therefore this is nothing more than informative for us.  However, it may be
            helpful to keep in mind in the future for trying to rebuild carved entries in some way.

            """

            if sql_command.startswith(CREATE_INDEX_CLAUSE):

                # Set the create command offset to point to the end of the "create index" statement
                create_command_offset = len(CREATE_INDEX_CLAUSE)

            elif sql_command.startswith(CREATE_UNIQUE_INDEX_CLAUSE):

                self.unique = True

                # Set the create command offset to point to the end of the "create unique index" statement
                create_command_offset = len(CREATE_UNIQUE_INDEX_CLAUSE)

            else:
                log_message = "Invalid sql for create index statement: {} with name: {}."
                log_message = log_message.format(self.sql, self.name)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            if not create_command_offset:
                log_message = "The create command offset was not set while parsing sql for index row name: {} " \
                              "and sql: {}."
                log_message = log_message.format(self.name, self.sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            """

            We take off the "create [unique] index" beginning portion of the command here leaving the index name next.
            At this point we have the create index command in the following structure:

            [INDEX_NAME] ON [TABLE_NAME] ( [INDEXED_COLUMN], ... ) [WHERE [EXPR]]

            Note:  An INDEXED_COLUMN (specified above) can be either a column-name or expr that may be followed by
                   either a COLLATE command or ASC/DESC command (or both).

            Note:  Capitalization of commands does not matter and checks on exact string commands need to take into
                   account case insensitivity.

            Note:  Following the index name, comments may appear from that point after in the index SQL.

            """

            # Strip off the "create [unique] index" command from the beginning of the create index statement
            remaining_sql_command = str(sql_command[create_command_offset + 1:])

            # Get the index name and remaining sql
            index_name, remaining_sql_command = \
                MasterSchemaRow._get_master_schema_row_name_and_remaining_sql(self.row_type, self.name,
                                                                              self.sql, remaining_sql_command)

            # Left strip the remaining sql command
            remaining_sql_command = remaining_sql_command.lstrip()

            # Check if this remaining sql statement starts with "ON"
            if remaining_sql_command[:len(INDEX_ON_COMMAND)].upper() != INDEX_ON_COMMAND:
                log_message = "Create index statement does not have a \"ON\" clause for master schema " \
                              "index row with name: {} and sql: {}."
                log_message = log_message.format(self.name, self.sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Remove the using prefix and strip any whitespace from the beginning
            remaining_sql_command = remaining_sql_command[len(INDEX_ON_COMMAND):].lstrip()

            # Get the table name and remaining sql
            table_name, remaining_sql_command = \
                MasterSchemaRow._get_master_schema_row_name_and_remaining_sql(self.row_type, self.name,
                                                                              self.sql, remaining_sql_command)

            # Left strip the remaining sql command
            remaining_sql_command = remaining_sql_command.lstrip()

            # Check the index name is equal to the name as specified in the sqlite documentation
            if index_name.lower() != self.name.lower():
                log_message = "For index master schema row: {}, the index name: {} does not match the derived index" \
                              "name: {} from the sql: {}."
                log_message = log_message.format(self.row_id, self.name, index_name, self.sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Check the table name is equal to the index table name as specified in the sqlite documentation
            if table_name.lower() != self.table_name.lower():
                log_message = "For index master schema row: {}, the table name: {} does not match the derived table " \
                              "name: {} from the sql: {}."
                log_message = log_message.format(self.row_id, self.table_name, table_name, self.sql)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            """

            Note:  Since we already checked above that the table name was in the table master schema entries sent in,
                   we do not check again here.

            """

            """

            The remaining SQL command must now either start with an opening parenthesis "(", or a comment indicator.
            Comment indicators would be either the "--" or "/*" character sequences.

            """

            # Check for comments after the index name, before the indexed columns
            while remaining_sql_command.startswith(("--", "/*")):
                comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
                self.comments.append(comment.rstrip())
                remaining_sql_command = remaining_sql_command.lstrip()

            # The first thing to be done is get the closing parenthesis index to the indexed columns
            closing_parenthesis_index = get_index_of_closing_parenthesis(remaining_sql_command)

            # Declare the indexed columns to be the "( [INDEXED_COLUMN], ... )" explained above
            indexed_columns = remaining_sql_command[:closing_parenthesis_index + 1]

            # Double check the indexed columns has a beginning opening parenthesis and ends with a closing parenthesis.
            if indexed_columns.find("(") != 0 or indexed_columns.rfind(")") != len(indexed_columns) - 1:
                log_message = "The indexed columns are not surrounded by parenthesis as expected for index row with" \
                              "name: {} and sql: {} with definitions: {}."
                log_message = log_message.format(self.name, self.sql, indexed_columns)
                logger.error(log_message)
                raise MasterSchemaRowParsingError(log_message)

            # Remove the beginning and ending parenthesis and left strip the string in case single whitespace characters
            # appear directly after the opening parenthesis and set it back to the index columns.  The characters before
            # the ending parenthesis are allowed since there could be a "\n" character corresponding to a "--" comment.

            """

            The next step here is to parse and left strip the indexed columns and continue parsing:
            indexed_columns = indexed_columns[1:len(indexed_columns) - 1].lstrip()
            ...

            Support for indexed columns has not been implemented yet.

            """

            """

            Lastly, if there is remaining SQL, we check to make sure it is a "WHERE" statement.  If it is not,
            then an exception will be thrown since that is the only use case allowed here according to the SQLite
            documentation.

            """

            # Last get the remaining sql command to check for the "where" use case
            remaining_sql_command = remaining_sql_command[closing_parenthesis_index + 1:].lstrip()

            """

            The create index statements work differently than the create table statements in respect to comments
            and the clauses after the column definitions/indexed columns.  In a create table statement, any comments
            after the end of the column definitions is ignored by SQLite unless the "without rowid" clause is
            stated which then recognizes comments before, in between, and after the clause.

            For create index statements, comments are not ignored by SQLite no matter if the "where" clause
            is specified after the indexed columns or not.  Therefore, if the remaining SQL command has any
            more content, it may either be a comment, a "where" clause, or both.

            """

            # Check for comments after the end of the index columns
            while remaining_sql_command.startswith(("--", "/*")):
                comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
                self.comments.append(comment.rstrip())
                remaining_sql_command = remaining_sql_command.lstrip()

            # See if the remaining sql command has any content left
            if len(remaining_sql_command) != 0:

                """

                Since we removed any previous comments above, if we still have content at this point, we know that the
                only allowed use case in this scenario is to have the "where" statement next in the SQL.

                Note:  The "where" clause may be mixed-case.

                """

                # Check if this remaining sql statement starts with "WHERE"
                if remaining_sql_command[:len(INDEX_WHERE_CLAUSE)].upper() != INDEX_WHERE_CLAUSE:
                    log_message = "Create virtual table statement does not have a \"WHERE\" clause for master schema " \
                                  "index row with name: {} and sql: {} when expected."
                    log_message = log_message.format(self.name, self.sql)
                    logger.error(log_message)
                    raise MasterSchemaRowParsingError(log_message)

                # Set the partial index flag and the where expression
                self.partial_index = True

                # Check for comments after the where clause
                while remaining_sql_command.startswith(("--", "/*")):
                    comment, remaining_sql_command = parse_comment_from_sql_segment(remaining_sql_command)
                    self.comments.append(comment.rstrip())
                    remaining_sql_command = remaining_sql_command.lstrip()

                """

                The next step here is to parse the "WHERE" clause:
                remaining_sql_command = remaining_sql_command[len(INDEX_WHERE_CLAUSE):].lstrip()
                ...

                Support for partial indexes has not been implemented yet.

                """

            """

            Until the partial index is fully implemented, we will throw a warning here.  Partial indexes are only
            made on a subset of rows depending on the "WHERE" clause which would need to be parsed to be exact.

            """

            if self.partial_index:
                log_message = "A index specified as a partial index was found in index row with name: {} " \
                              "and sql: {}.  This use case is not fully implemented."
                log_message = log_message.format(self.name, self.sql)
                logger.warn(log_message)
                warn(log_message, RuntimeWarning)

    def stringify(self, padding="", print_record_columns=True):
        string = "\n" \
                 + padding + "Internal Schema Object: {}\n" \
                 + padding + "Unique: {}\n" \
                 + padding + "Partial Index: {}"
        string = string.format(self.internal_schema_object,
                               self.unique,
                               self.partial_index)
        string = super(IndexRow, self).stringify(padding, print_record_columns) + string
        return string


class ViewRow(MasterSchemaRow):

    def __init__(self, version_interface, b_tree_table_leaf_page_number,
                 b_tree_table_leaf_cell, record_columns, tables):

        super(ViewRow, self).__init__(version_interface, b_tree_table_leaf_page_number,
                                      b_tree_table_leaf_cell, record_columns)

        logger = getLogger(LOGGER_NAME)

        if self.row_type != MASTER_SCHEMA_ROW_TYPE.VIEW:
            log_message = "Invalid row type: {} when expecting: {} with name: {}."
            log_message = log_message.format(self.row_type, MASTER_SCHEMA_ROW_TYPE.VIEW, self.name)
            logger.error(log_message)
            raise ValueError(log_message)


class TriggerRow(MasterSchemaRow):

    def __init__(self, version_interface, b_tree_table_leaf_page_number,
                 b_tree_table_leaf_cell, record_columns, tables, views):

        super(TriggerRow, self).__init__(version_interface, b_tree_table_leaf_page_number,
                                         b_tree_table_leaf_cell, record_columns)

        logger = getLogger(LOGGER_NAME)

        if self.row_type != MASTER_SCHEMA_ROW_TYPE.TRIGGER:
            log_message = "Invalid row type: {} when expecting: {} with name: {}."
            log_message = log_message.format(self.row_type, MASTER_SCHEMA_ROW_TYPE.TRIGGER, self.name)
            logger.error(log_message)
            raise ValueError(log_message)
