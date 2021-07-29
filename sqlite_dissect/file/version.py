from abc import ABCMeta
from abc import abstractmethod
from binascii import hexlify
from logging import getLogger
from re import sub
from sqlite_dissect.constants import INDEX_INTERIOR_PAGE_HEX_ID
from sqlite_dissect.constants import INDEX_LEAF_PAGE_HEX_ID
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_PAGE_HEX_ID
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.constants import PAGE_TYPE_LENGTH
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from sqlite_dissect.constants import SQLITE_MASTER_SCHEMA_ROOT_PAGE
from sqlite_dissect.constants import TABLE_INTERIOR_PAGE_HEX_ID
from sqlite_dissect.constants import TABLE_LEAF_PAGE_HEX_ID
from sqlite_dissect.exception import VersionParsingError
from sqlite_dissect.file.database.header import DatabaseHeader
from sqlite_dissect.file.database.page import IndexInteriorPage
from sqlite_dissect.file.database.page import IndexLeafPage
from sqlite_dissect.file.database.page import TableInteriorPage
from sqlite_dissect.file.database.page import TableLeafPage
from sqlite_dissect.file.database.utilities import get_pages_from_b_tree_page
from sqlite_dissect.file.schema.master import MasterSchema

"""

version.py

This script holds the superclass objects used for parsing the database and write ahead log.

This script holds the following object(s):
Version(object)

"""


class Version(object):

    __metaclass__ = ABCMeta

    def __init__(self, file_handle, version_number, store_in_memory, strict_format_checking):

        self._logger = getLogger(LOGGER_NAME)

        self.file_handle = file_handle
        self.version_number = version_number
        self.store_in_memory = store_in_memory
        self.strict_format_checking = strict_format_checking
        self.page_size = self.file_handle.header.page_size

        self._database_header = None
        self.database_size_in_pages = None

        self._root_page = None

        self.first_freelist_trunk_page = None
        self.freelist_page_numbers = None
        self.pointer_map_pages = None
        self.pointer_map_page_numbers = None

        self._master_schema = None

        self.updated_page_numbers = None
        self.page_version_index = None

        """

        The _pages variable is only for the use case that the pages are requested to be stored in memory.

        """

        self._pages = None

        """

        The following variables are to track across the versions (database and wal commit records) what portions of the
        file are changed.

        For the Database:

        The database header, root b-tree page, and master schema will be set to True since these objects are always
        considered modified in the database file.  As a side note, the master schema could not have any entries if there
        was no schema but is still considered modified.

        The freelist pages modified flag will be set to True if freelist pages exist, otherwise False since there are no
        freelist pages.

        The pointer map pages modified flag will be set to True if the largest b-tree root page is set in the header
        indicating that auto-vacuuming is turned on.  Otherwise, if this field is 0, auto-vacuuming is turned off and
        the pointer map pages modified flag will be set to False.  As a side note, if this is set to False, then it
        will continue to be False throughout all following versions since the auto-vacuuming must be set before the
        schema creation and cannot be turned off if enabled, or turned on if not enabled initially.  (Switching between
        full (0) and incremental (1) auto-vacuuming modes is allowed.)

        The updated b-tree page numbers array are all the schema root page numbers including all pages of the b-tree.
        These will represent all of the b-tree and overflow pages (excluding the master schema related pages) updated.
        All of the b-tree pages for the database will be included in this array.

        For the WriteAheadLogCommitRecord:

        The database header will be set to True if the database header was updated.  This should always occur if any
        change was made to the root page (although the root page may be in the commit record with no changes).

        The root b-tree page modified flag will be set if the content on the root b-tree portion (not including the
        database header) is modified.  This will also result in teh master schema modified flag being set to True.
        However, the inverse is not true as described next.

        The master schema modified flag will be set if the master schema is updated.  This includes any of the master
        schema pages being updated in the b-tree.  If the master schema updated pages did not include the sqlite master
        schema root page (1). then the master schema modified flag will still be set to True, but the root b-tree page
        modified flag will be False.

        The freelist pages modified and pointer map pages flags will be set to True when the freelist pages are updated
        in any way.

        The updated b-tree page numbers array are all the schema root page numbers including all pages of the b-tree.
        These will represent all of the b-tree and overflow pages (excluding the master schema related pages) updated.
        Only the b-tree pages for the wal commit record that were updated will be included in this array.

        Note:  The database header modified flag and the root b-tree page modified tags refer to different areas of the
               sqlite root page.  The database header may be modified without the root b-tree page being modified.
               However, if the root b-tree page is modified, then the header should always be modified since the header
               contains a change counter that is incremented whenever changes to the database are done.

        Note:  The following variables below specify if the root b-tree page was modified and if the master
               schema was modified.  Although the master schema is on the database root b-tree page, the
               master schema changes may not be directly on the root page itself.  Therefore, the master schema
               may be modified without modifying the root page but if the root b-tree page is modified, then the
               master schema modified flag will always be set.

        """

        self.database_header_modified = False
        self.root_b_tree_page_modified = False
        self.master_schema_modified = False
        self.freelist_pages_modified = False
        self.pointer_map_pages_modified = False

        self.updated_b_tree_page_numbers = None

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_pages=True, print_schema=True):
        string = padding + "File Type: {}\n" \
                 + padding + "Version Number: {}\n" \
                 + padding + "Store in Memory: {}\n" \
                 + padding + "Strict Format Checking: {}\n" \
                 + padding + "Page Size: {}\n" \
                 + padding + "File Handle:\n{}\n" \
                 + padding + "Database Header:\n{}\n" \
                 + padding + "Database Size in Pages: {}\n" \
                 + padding + "Freelist Page Numbers: {}\n" \
                 + padding + "Pointer Map Page Numbers: {}\n" \
                 + padding + "Updated Page Numbers: {}\n" \
                 + padding + "Page Version Index: {}\n" \
                 + padding + "Database Header Modified: {}\n" \
                 + padding + "Root B-Tree Page Modified: {}\n" \
                 + padding + "Master Schema Modified: {}\n" \
                 + padding + "Freelist Pages Modified: {}\n" \
                 + padding + "Pointer Map Pages Modified: {}\n" \
                 + padding + "Updated B-Tree Page Numbers: {}"
        string = string.format(self.file_type,
                               self.version_number,
                               self.store_in_memory,
                               self.strict_format_checking,
                               self.page_size,
                               self.file_handle.stringify(padding + "\t"),
                               self.database_header.stringify(padding + "\t"),
                               self.database_size_in_pages,
                               self.freelist_page_numbers,
                               self.pointer_map_page_numbers,
                               self.updated_page_numbers,
                               self.page_version_index,
                               self.database_header_modified,
                               self.root_b_tree_page_modified,
                               self.master_schema_modified,
                               self.freelist_pages_modified,
                               self.pointer_map_pages_modified,
                               self.updated_b_tree_page_numbers)
        if print_pages:
            for page in self.pages.itervalues():
                string += "\n" + padding + "Page:\n{}".format(page.stringify(padding + "\t"))
        if print_schema:
            string += "\n" \
                      + padding + "Master Schema:\n{}".format(self.master_schema.stringify(padding + "\t", print_pages))
        return string

    @property
    def file_type(self):
        return self.file_handle.file_type

    @property
    def database_text_encoding(self):
        return self.file_handle.database_text_encoding

    @database_text_encoding.setter
    def database_text_encoding(self, database_text_encoding):
        self.file_handle.database_text_encoding = database_text_encoding

    @property
    def database_header(self):
        if not self._database_header:
            return DatabaseHeader(self.get_page_data(SQLITE_MASTER_SCHEMA_ROOT_PAGE)[:SQLITE_DATABASE_HEADER_LENGTH])
        return self._database_header

    @property
    def root_page(self):
        if not self._root_page:
            return self.get_b_tree_root_page(SQLITE_MASTER_SCHEMA_ROOT_PAGE)
        return self._root_page

    @property
    def master_schema(self):
        if not self._master_schema:
            return MasterSchema(self, self.root_page)
        return self._master_schema

    @property
    def pages(self):

        # Return the pages if they are being stored in memory and already parsed
        if self._pages:
            return self._pages

        pages = {}

        # Populate the freelist pages into the pages dictionary
        freelist_trunk_page = self.first_freelist_trunk_page
        while freelist_trunk_page:
            pages[freelist_trunk_page.number] = freelist_trunk_page
            for freelist_leaf_page in freelist_trunk_page.freelist_leaf_pages:
                pages[freelist_leaf_page.number] = freelist_leaf_page
            freelist_trunk_page = freelist_trunk_page.next_freelist_trunk_page

        # Populate the pointer map pages into the pages dictionary
        for pointer_map_page in self.pointer_map_pages:
            pages[pointer_map_page.number] = pointer_map_page

        """

        Since the WAL commit record may not have the master schema parsed and needs to parse it, we store the master
        schema to a variable so it is only parsed once, if need be.

        """

        master_schema = self.master_schema

        # Populate the master schema page into the pages dictionary including the root page
        for master_schema_page in master_schema.master_schema_pages:
            pages[master_schema_page.number] = master_schema_page

        # Populate the b-trees from the master schema including the root page
        for b_tree_root_page_number in master_schema.master_schema_b_tree_root_page_numbers:
            b_tree_root_page = self.get_b_tree_root_page(b_tree_root_page_number)
            for b_tree_page in get_pages_from_b_tree_page(b_tree_root_page):
                pages[b_tree_page.number] = b_tree_page

        # Set the number of pages that were found
        number_of_pages = len(pages)

        if number_of_pages != self.database_size_in_pages:
            log_message = "The number of pages: {} did not match the database size in pages: {} for version: {}."
            log_message = log_message.format(number_of_pages, self.database_size_in_pages, self.version_number)
            self._logger.error(log_message)
            raise VersionParsingError(log_message)

        for page_number in [page_index + 1 for page_index in range(self.database_size_in_pages)]:
            if page_number not in pages:
                log_message = "Page number: {} was not found in the pages: {} for version: {}."
                log_message = log_message.format(page_number, pages.keys(), self.version_number)
                self._logger.error(log_message)
                raise VersionParsingError(log_message)

        return pages

    @abstractmethod
    def get_page_data(self, page_number, offset=0, number_of_bytes=None):
        log_message = "The abstract method get_page_data was called directly and is not implemented."
        self._logger.error(log_message)
        raise NotImplementedError(log_message)

    @abstractmethod
    def get_page_offset(self, page_number):
        log_message = "The abstract method get_page_offset was called directly and is not implemented."
        self._logger.error(log_message)
        raise NotImplementedError(log_message)

    def get_b_tree_root_page(self, b_tree_page_number):

        """



        Note:  There is no real way of efficiently checking if this page is a root page or not and doesn't really
               matter for the purpose of this library.  Therefore, any b-tree page requested is considered a root
               page in relation to it's position to the b-tree that it is a part of for the purposes of this function.

        :param b_tree_page_number:

        :return:

        """

        # Return the page if it is already being in memory and already parsed
        if self._pages:

            b_tree_root_page = self._pages[b_tree_page_number]

            # Make sure the page is a b-tree page
            if b_tree_root_page.page_type not in [PAGE_TYPE.B_TREE_TABLE_INTERIOR, PAGE_TYPE.B_TREE_TABLE_LEAF,
                                                  PAGE_TYPE.B_TREE_INDEX_INTERIOR, PAGE_TYPE.B_TREE_INDEX_LEAF]:
                log_message = "The b-tree page number: {} is not a b-tree page but instead has a type of: {}."
                log_message = log_message.format(b_tree_page_number, b_tree_root_page.page_type)
                self._logger.error(log_message)
                raise ValueError(log_message)

            # Return the b-tree page
            return b_tree_root_page

        page_hex_type = self.get_page_data(b_tree_page_number, 0, PAGE_TYPE_LENGTH)

        if page_hex_type == MASTER_PAGE_HEX_ID:

            # Make sure this is the sqlite master schema root page
            if b_tree_page_number != SQLITE_MASTER_SCHEMA_ROOT_PAGE:
                log_message = "The b-tree page number: {} contains the master page hex but is not page number: {}."
                log_message = log_message.format(b_tree_page_number)
                self._logger.error(log_message)
                raise VersionParsingError(log_message)

            page_hex_type = self.get_page_data(b_tree_page_number, SQLITE_DATABASE_HEADER_LENGTH, PAGE_TYPE_LENGTH)

            # If this is the sqlite master schema root page then this page has to be a table interior or leaf page
            if page_hex_type not in [TABLE_INTERIOR_PAGE_HEX_ID, TABLE_LEAF_PAGE_HEX_ID]:
                log_message = "The b-tree page number: {} contains the master page hex but has hex type: {} which " \
                              "is not the expected table interior or table leaf page hex."
                log_message = log_message.format(b_tree_page_number, hexlify(page_hex_type))
                self._logger.error(log_message)
                raise VersionParsingError(log_message)

        # Check if it was a b-tree table interior
        if page_hex_type == TABLE_INTERIOR_PAGE_HEX_ID:

            # Create the table interior page
            return TableInteriorPage(self, b_tree_page_number)

        # Check if it was a b-tree table leaf
        elif page_hex_type == TABLE_LEAF_PAGE_HEX_ID:

            # Create the table leaf page
            return TableLeafPage(self, b_tree_page_number)

        # Check if it was a b-tree index interior
        elif page_hex_type == INDEX_INTERIOR_PAGE_HEX_ID:

            # Create the table interior page
            return IndexInteriorPage(self, b_tree_page_number)

        # Check if it was a b-tree index leaf
        elif page_hex_type == INDEX_LEAF_PAGE_HEX_ID:

            # Create the table leaf page
            return IndexLeafPage(self, b_tree_page_number)

        # Throw an exception since the type of the b-tree page was not a b-tree hex type
        else:

            log_message = "The b-tree page number: {} did not refer to a b-tree page but rather a page of hex type: {}."
            log_message = log_message.format(hexlify(page_hex_type))
            self._logger.error(log_message)
            raise ValueError(log_message)

    def get_page_version(self, page_number):

        try:

            return self.page_version_index[page_number]

        except KeyError:

            log_message = "The page number: {} was not found in the page version index: {} for version: {}."
            log_message = log_message.format(page_number, self.page_version_index, self.version_number)
            self._logger.error(log_message)
            raise
