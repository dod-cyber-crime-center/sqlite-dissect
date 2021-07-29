from abc import ABCMeta
from binascii import hexlify
from logging import getLogger
from re import sub
from struct import unpack
from warnings import warn
from sqlite_dissect.constants import CELL_LOCATION
from sqlite_dissect.constants import CELL_MODULE
from sqlite_dissect.constants import CELL_POINTER_BYTE_LENGTH
from sqlite_dissect.constants import CELL_SOURCE
from sqlite_dissect.constants import FIRST_OVERFLOW_PAGE_INDEX
from sqlite_dissect.constants import FIRST_OVERFLOW_PAGE_NUMBER_LENGTH
from sqlite_dissect.constants import FIRST_OVERFLOW_PARENT_PAGE_NUMBER
from sqlite_dissect.constants import FREEBLOCK_BYTE_LENGTH
from sqlite_dissect.constants import FREELIST_HEADER_LENGTH
from sqlite_dissect.constants import FREELIST_LEAF_PAGE_NUMBER_LENGTH
from sqlite_dissect.constants import FREELIST_NEXT_TRUNK_PAGE_LENGTH
from sqlite_dissect.constants import INDEX_INTERIOR_CELL_CLASS
from sqlite_dissect.constants import INDEX_INTERIOR_PAGE_HEX_ID
from sqlite_dissect.constants import INDEX_LEAF_CELL_CLASS
from sqlite_dissect.constants import INDEX_LEAF_PAGE_HEX_ID
from sqlite_dissect.constants import INTERIOR_PAGE_HEADER_CLASS
from sqlite_dissect.constants import LEAF_PAGE_HEADER_CLASS
from sqlite_dissect.constants import LEFT_CHILD_POINTER_BYTE_LENGTH
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_PAGE_HEX_ID
from sqlite_dissect.constants import NEXT_FREEBLOCK_OFFSET_LENGTH
from sqlite_dissect.constants import OVERFLOW_HEADER_LENGTH
from sqlite_dissect.constants import PAGE_FRAGMENT_LIMIT
from sqlite_dissect.constants import PAGE_HEADER_MODULE
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.constants import PAGE_TYPE_LENGTH
from sqlite_dissect.constants import POINTER_MAP_B_TREE_NON_ROOT_PAGE_TYPE
from sqlite_dissect.constants import POINTER_MAP_B_TREE_ROOT_PAGE_TYPE
from sqlite_dissect.constants import POINTER_MAP_ENTRY_LENGTH
from sqlite_dissect.constants import POINTER_MAP_FREELIST_PAGE_TYPE
from sqlite_dissect.constants import POINTER_MAP_OVERFLOW_FIRST_PAGE_TYPE
from sqlite_dissect.constants import POINTER_MAP_OVERFLOW_FOLLOWING_PAGE_TYPE
from sqlite_dissect.constants import POINTER_MAP_PAGE_TYPES
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from sqlite_dissect.constants import SQLITE_MASTER_SCHEMA_ROOT_PAGE
from sqlite_dissect.constants import TABLE_INTERIOR_CELL_CLASS
from sqlite_dissect.constants import TABLE_INTERIOR_PAGE_HEX_ID
from sqlite_dissect.constants import TABLE_LEAF_CELL_CLASS
from sqlite_dissect.constants import TABLE_LEAF_PAGE_HEX_ID
from sqlite_dissect.constants import ZERO_BYTE
from sqlite_dissect.exception import BTreePageParsingError
from sqlite_dissect.exception import CellParsingError
from sqlite_dissect.exception import PageParsingError
from sqlite_dissect.file.database.payload import decode_varint
from sqlite_dissect.file.database.payload import Record
from sqlite_dissect.utilities import calculate_expected_overflow
from sqlite_dissect.utilities import get_class_instance
from sqlite_dissect.utilities import get_md5_hash

"""

page.py

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

Note:  In some places, like with unallocated data on the page, it was decided to not store this data in memory
       and pull it from the file on demand and/or calculate information from it if needed on demand.  This was done
       to prevent the memory used by this program becoming bloated with unneeded data.

Assumptions:
1.) OverflowPage: All overflow pages are replaced in a chain on modification.  This assumes that whenever a cell is
                  modified, that even if the content of the overflow portion does not change, the whole cell including
                  overflow need to be replaced due to the way the cells are stored in SQLite.

"""


class Page(object):

    __metaclass__ = ABCMeta

    def __init__(self, version_interface, number):

        self._logger = getLogger(LOGGER_NAME)

        self._version_interface = version_interface
        self.version_number = self._version_interface.version_number
        self.page_version_number = self._version_interface.get_page_version(number)
        self.number = number
        self.page_type = None
        self.offset = self._version_interface.get_page_offset(self.number)
        self.size = self._version_interface.page_size
        self.md5_hex_digest = None
        self.unallocated_space_start_offset = None
        self.unallocated_space_end_offset = None

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Version Number: {}\n" \
                 + padding + "Page Version Number: {}\n" \
                 + padding + "Number: {}\n" \
                 + padding + "Page Type: {}\n" \
                 + padding + "Offset: {}\n" \
                 + padding + "Size: {}\n" \
                 + padding + "MD5 Hex Digest: {}\n" \
                 + padding + "Unallocated Space Start Offset: {}\n" \
                 + padding + "Unallocated Space End Offset: {}\n" \
                 + padding + "Unallocated Space Size: {}\n" \
                 + padding + "Unallocated Content MD5 Hex Digest: {}\n" \
                 + padding + "Unallocated Content (Hex): {}"
        return string.format(self.version_number,
                             self.page_version_number,
                             self.number,
                             self.page_type,
                             self.offset,
                             self.size,
                             self.md5_hex_digest,
                             self.unallocated_space_start_offset,
                             self.unallocated_space_end_offset,
                             self.unallocated_space_length,
                             self.unallocated_space_md5_hex_digest,
                             hexlify(self.unallocated_space))

    @property
    def unallocated_space(self):

        """

        This property returns the unallocated space inside this page.

        :return: bytearray The byte array for unallocated space.

        """

        if self.unallocated_space_length == 0:
            return bytearray()
        else:
            return self._version_interface.get_page_data(self.number, self.unallocated_space_start_offset,
                                                         self.unallocated_space_length)

    @property
    def unallocated_space_md5_hex_digest(self):

        """

        This method will compute the md5 hash of the unallocated space of this page and return it.  This is
        calculated when called instead of before hand since this is a superclass and does not know where the
        unallocated space starts and ends at time of creation.  Although this could be computed and stored the first
        time it is called, it was decided to always compute when called.

        :return: string The hexadecimal md5 hash string.

        """

        return get_md5_hash(self.unallocated_space)

    @property
    def unallocated_space_length(self):

        """

        This property will compute the unallocated space length of this page and return it.  This is calculated
        when called instead of before hand since this is a superclass and does not know the unallocated space
        start and end offsets at time of creation.

        :return: int The unallocated space length.

        """

        # Return the length of the unallocated space on this page
        return self.unallocated_space_end_offset - self.unallocated_space_start_offset


class OverflowPage(Page):

    def __init__(self, version_interface, number, parent_cell_page_number, parent_overflow_page_number,
                 index, payload_remaining):

        super(OverflowPage, self).__init__(version_interface, number)

        self.page_type = PAGE_TYPE.OVERFLOW

        if payload_remaining <= 0:
            log_message = "No payload remaining when overflow page initialized for version number: {} page number: {}."
            log_message = log_message.format(self.version_number, self.number)
            self._logger.error(log_message)
            raise PageParsingError(log_message)

        page = self._version_interface.get_page_data(self.number)

        self.parent_cell_page_number = parent_cell_page_number
        self.parent_overflow_page_number = parent_overflow_page_number
        self.index = index
        self.next_overflow_page_number = unpack(b">I", page[:OVERFLOW_HEADER_LENGTH])[0]

        self.unallocated_space_start_offset = self.size
        self.unallocated_space_end_offset = self.size
        self.md5_hex_digest = get_md5_hash(page)

        if payload_remaining <= self.size - OVERFLOW_HEADER_LENGTH:

            # This was found to be the last overflow page in the chain.  Make sure there are no other overflow pages.
            if self.next_overflow_page_number:
                log_message = "Additional overflow page number: {} found for version number: {} " \
                              "page version number: {} page number: {} when no more overflow pages were expected."
                log_message = log_message.format(self.next_overflow_page_number, self.version_number,
                                                 self.page_version_number, self.number)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            self.unallocated_space_start_offset = payload_remaining + OVERFLOW_HEADER_LENGTH

        if self.next_overflow_page_number:

            """

            Here we make the assumption that all overflow pages have to be replaced when any overflow page in a chain
            is updated.  In other words, when a overflow chain is changed in a version, all overflow pages in that chain
            belong to that version.  This is due to the face that all overflow pages in a chain pertain to a cell that
            was modified and therefore all overflow pages belonging to that record need to be reinserted even if the
            same as before.

            Here we check the version of the overflow page that this one points to.  If the versions of the two pages
            are different we throw an exception.

            Since overflow pages are in a chain, this check is done on each creation of the next overflow page for the
            following overflow page if it exists.

            """

            next_overflow_page_version = self._version_interface.get_page_version(self.next_overflow_page_number)
            if self.page_version_number != next_overflow_page_version:
                log_message = "The version of the current overflow page: {} on version: {} on page: {} has points to " \
                              "a next overflow page version: {} for page: {} that has a different version."
                log_message = log_message.format(self.page_version_number, self.version_number, self.number,
                                                 next_overflow_page_version, self.next_overflow_page_number)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Parent Cell Page Number: {}\n" \
                 + padding + "Parent Overflow Page Number: {}\n" \
                 + padding + "Index: {}\n" \
                 + padding + "Next Overflow Page Number: {}\n" \
                 + padding + "Content Length: {}\n" \
                 + padding + "Content (Hex): {}"
        string = string.format(self.parent_cell_page_number,
                               self.parent_overflow_page_number,
                               self.index,
                               self.next_overflow_page_number,
                               self.content_length,
                               hexlify(self.content))
        return super(OverflowPage, self).stringify(padding) + string

    @property
    def content(self):
        return self._version_interface.get_page_data(self.number, OVERFLOW_HEADER_LENGTH, self.content_length)

    @property
    def content_length(self):
        return self.unallocated_space_start_offset - OVERFLOW_HEADER_LENGTH


class FreelistTrunkPage(Page):

    def __init__(self, version_interface, number, parent_freelist_trunk_page_number, index):

        super(FreelistTrunkPage, self).__init__(version_interface, number)

        self.page_type = PAGE_TYPE.FREELIST_TRUNK

        self.parent_freelist_trunk_page_number = parent_freelist_trunk_page_number
        self.index = index

        page = self._version_interface.get_page_data(self.number)

        self.next_freelist_trunk_page_number = unpack(b">I", page[:FREELIST_NEXT_TRUNK_PAGE_LENGTH])[0]
        self.number_of_leaf_page_pointers = unpack(b">I", page[FREELIST_NEXT_TRUNK_PAGE_LENGTH:
                                                               FREELIST_HEADER_LENGTH])[0]
        self.freelist_leaf_page_numbers = []
        self.freelist_leaf_pages = []
        for index in range(self.number_of_leaf_page_pointers):
            start_offset = index * FREELIST_LEAF_PAGE_NUMBER_LENGTH + FREELIST_HEADER_LENGTH
            end_offset = start_offset + FREELIST_LEAF_PAGE_NUMBER_LENGTH
            freelist_leaf_page_number = unpack(b">I", page[start_offset:end_offset])[0]

            """

            Note: Freelist leaf pages can be in previous commit records to the commit record this current freelist trunk
                  page is in or commit records up to the main commit record version if applicable.

            """

            freelist_leaf_page = FreelistLeafPage(self._version_interface, freelist_leaf_page_number,
                                                  self.number, index)

            self.freelist_leaf_page_numbers.append(freelist_leaf_page_number)
            self.freelist_leaf_pages.append(freelist_leaf_page)

        if len(self.freelist_leaf_page_numbers) != self.number_of_leaf_page_pointers:
            log_message = "In freelist trunk page: {} with page version: {} in version: {} found a different amount " \
                          "of freelist leaf page numbers: {} than freelist leaf page pointers: {} found on the page."
            log_message = log_message.format(self.number, self.page_version_number, self.version_number,
                                             len(self.freelist_leaf_page_numbers), self.number_of_leaf_page_pointers)
            self._logger.error(log_message)
            raise PageParsingError(log_message)

        freelist_leaf_page_numbers_size = self.number_of_leaf_page_pointers * FREELIST_LEAF_PAGE_NUMBER_LENGTH
        self.unallocated_space_start_offset = FREELIST_HEADER_LENGTH + freelist_leaf_page_numbers_size
        self.unallocated_space_end_offset = self.size

        self.md5_hex_digest = get_md5_hash(page)

        self.next_freelist_trunk_page = None
        if self.next_freelist_trunk_page_number:

            """

            Here we make the assumption that a freelist trunk page can be updated without updating following freelist
            trunk pages in the linked list.  Since this is an "allowed" assumption, a print statement will print a log
            info message that this happens and once we observe it, we can then declare it is no longer an assumption.

            """

            next_freelist_trunk_page_version_number = self._version_interface.get_page_version(
                                                                                self.next_freelist_trunk_page_number)
            if self.page_version_number > next_freelist_trunk_page_version_number:
                log_message = "Found a freelist trunk page: {} that has page version: {} in version: {} that points " \
                              "to an earlier freelist trunk page version: {}."
                log_message = log_message.format(self.number, self.page_version_number, self.version_number,
                                                 next_freelist_trunk_page_version_number)
                self._logger.info(log_message)

            self.next_freelist_trunk_page = FreelistTrunkPage(self._version_interface,
                                                              self.next_freelist_trunk_page_number,
                                                              self.number, self.index + 1)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Parent Freelist Trunk Page Number: {}\n" \
                 + padding + "Index: {}\n" \
                 + padding + "Next Freelist Trunk Page Number: {}\n" \
                 + padding + "Number of Leaf Page Pointers: {}\n" \
                 + padding + "Freelist Leaf Page Numbers: {}\n" \
                 + padding + "Freelist Leaf Pages length: {}"
        string = string.format(self.parent_freelist_trunk_page_number,
                               self.index,
                               self.next_freelist_trunk_page_number,
                               self.number_of_leaf_page_pointers,
                               self.freelist_leaf_page_numbers,
                               len(self.freelist_leaf_pages))
        for freelist_leaf_page in self.freelist_leaf_pages:
            string += "\n" + padding + "Freelist Leaf Page:\n{}".format(freelist_leaf_page.stringify(padding + "\t"))
        if self.next_freelist_trunk_page:
            string += "\n" + padding \
                      + "Next Freelist Trunk Page:\n{}".format(self.next_freelist_trunk_page.stringify(padding + "\t"))
        return super(FreelistTrunkPage, self).stringify(padding) + string


class FreelistLeafPage(Page):

    def __init__(self, version_interface, number, parent_freelist_trunk_page_number, index):

        super(FreelistLeafPage, self).__init__(version_interface, number)

        self.page_type = PAGE_TYPE.FREELIST_LEAF

        self.parent_freelist_trunk_page_number = parent_freelist_trunk_page_number
        self.index = index

        self.unallocated_space_start_offset = 0
        self.unallocated_space_end_offset = self.size

        page = self._version_interface.get_page_data(self.number)
        self.md5_hex_digest = get_md5_hash(page)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Parent Freelist Trunk Page Number: {}\n" \
                 + padding + "Index: {}"
        string = string.format(self.parent_freelist_trunk_page_number,
                               self.index)
        return super(FreelistLeafPage, self).stringify(padding) + string


class PointerMapPage(Page):

    def __init__(self, version_interface, number, number_of_entries):

        super(PointerMapPage, self).__init__(version_interface, number)

        self.page_type = PAGE_TYPE.POINTER_MAP

        page = self._version_interface.get_page_data(self.number)

        self.number_of_entries = number_of_entries

        self.unallocated_space_start_offset = self.number_of_entries * POINTER_MAP_ENTRY_LENGTH
        self.unallocated_space_end_offset = self.size

        self.md5_hex_digest = get_md5_hash(page)

        self.pointer_map_entries = []
        for index in range(self.number_of_entries):

            offset = index * POINTER_MAP_ENTRY_LENGTH

            if offset >= self.size:
                log_message = "For pointer map page: {} for page version: {} and version: {} the offset: {} " \
                              "was found to greater or equal to the page size: {} on index: {}."
                log_message = log_message.format(self.number, self.page_version_number, self.version_number,
                                                 offset, self.size, index)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            page_type = page[offset:offset + PAGE_TYPE_LENGTH]
            if page_type == ZERO_BYTE:
                log_message = "The page type was found to be empty for pointer map page: {} for page version: {} " \
                              "and version: {} on index: {} and offset: {}."
                log_message = log_message.format(self.number, self.page_version_number, self.version_number,
                                                 index, offset)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            elif offset + POINTER_MAP_ENTRY_LENGTH > self.size:
                log_message = "The offset {} and pointer map length: {} go beyond the page size: {} for pointer " \
                              "map page: {} for page version: {} and version: {} on index: {}."
                log_message = log_message.format(offset, POINTER_MAP_ENTRY_LENGTH, self.size, self.number,
                                                 self.page_version_number, self.version_number, index)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            elif page_type not in POINTER_MAP_PAGE_TYPES:
                log_message = "The page type was not recognized: {} as a valid pointer map page type for " \
                              "pointer map page: {} for page version: {} and version: {} on index: {} and offset: {}."
                log_message = log_message.format(hexlify(page_type), self.number, self.page_version_number,
                                                 self.version_number, index, offset)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            parent_page_number = unpack(b">I", page[offset + PAGE_TYPE_LENGTH:offset + POINTER_MAP_ENTRY_LENGTH])[0]

            if page_type in [POINTER_MAP_B_TREE_ROOT_PAGE_TYPE, POINTER_MAP_FREELIST_PAGE_TYPE] and parent_page_number:
                log_message = "The page type: {} has a parent page number: {} which is invalid for " \
                              "pointer map page: {} for page version: {} and version: {} on index: {} and offset: {}."
                log_message = log_message.format(hexlify(page_type), parent_page_number, self.number,
                                                 self.page_version_number, self.version_number, index, offset)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            elif page_type in [POINTER_MAP_OVERFLOW_FIRST_PAGE_TYPE, POINTER_MAP_OVERFLOW_FOLLOWING_PAGE_TYPE,
                               POINTER_MAP_B_TREE_NON_ROOT_PAGE_TYPE] and not parent_page_number:
                log_message = "The page type: {} does not have a parent page number which is invalid for " \
                              "pointer map page: {} for page version: {} and version: {} on index: {} and offset: {}."
                log_message = log_message.format(hexlify(page_type), self.number, self.page_version_number,
                                                 self.version_number, index, offset)
                self._logger.error(log_message)
                raise PageParsingError(log_message)

            pointer_map_entry_md5_hex_digest = get_md5_hash(page[offset:offset + POINTER_MAP_ENTRY_LENGTH])

            page_number = number + index + 1
            pointer_map_entry = PointerMapEntry(index, offset, page_number, page_type, parent_page_number,
                                                pointer_map_entry_md5_hex_digest)
            self.pointer_map_entries.append(pointer_map_entry)

        if len(self.pointer_map_entries) != self.number_of_entries:
            log_message = "In pointer map page: {} with page version: {} in version: {} found a different amount " \
                          "of pointer map entries: {} than expected number of entries: {} found on the page."
            log_message = log_message.format(self.number, self.page_version_number, self.version_number,
                                             len(self.pointer_map_entries), self.number_of_entries)
            self._logger.error(log_message)
            raise PageParsingError(log_message)

        remaining_space_offset = self.number_of_entries * POINTER_MAP_ENTRY_LENGTH
        if remaining_space_offset != self.unallocated_space_start_offset:
            log_message = "The remaining space offset: {} is not equal to the unallocated space start offset: {} " \
                          "for pointer map page: {} for page version: {} and version: {}."
            log_message = log_message.format(remaining_space_offset, self.unallocated_space_start_offset, self.number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise PageParsingError(log_message)

        """

        Originally here the remaining space was checked to see if it was all zeros, and if not an exception was thrown.
        This has since been removed since it was realized that this unallocated space can contain information resulting
        in non-zero unallocated space.

        It was realized that when a database increases in size and then decreases due to auto-vacuuming where freelist
        pages are truncated from the end of the database, the pointer information from those previous pages remain.

        This information may give an idea into what pages were removed and how they were previously structured.  This
        data should probably be parsed and investigated during the unallocated carving specific to pointer map pages.

        The patterns still need to match 5 bytes, first byte being the pointer map page type and the second 4 bytes
        being the page number (if existing).  This could give an idea of how big the database was previously but will
        only give the max size at any point in time since it does not appear that the pointer map pages are zero'd out
        at any point and are just overwritten if need be.

        There may still may be non-pointer map data included beyond the pointer map entries that does not fit the 5
        byte patterns.  For example page 2 where the first pointer map page was placed was previously a b-tree page
        before vacuuming was turned on.  However, there are other details where auto-vacuuming is only possible is
        turned on before table creation.  More research will have to be done here for exactly how everything here works.
        The page may also be zero'd out at a time such as this as well.

        """

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Number of Entries: {}\n" \
                 + padding + "Pointer Map Entries Size: {}"
        string = string.format(self.number_of_entries,
                               len(self.pointer_map_entries))
        for pointer_map_entry in self.pointer_map_entries:
            string += "\n" + padding + "Pointer Map Entry:\n{}".format(pointer_map_entry.stringify(padding + "\t"))
        return super(PointerMapPage, self).stringify(padding) + string


class PointerMapEntry(object):

    def __init__(self, index, offset, page_number, page_type, parent_page_number, md5_hex_digest):
        self.index = index
        self.offset = offset
        self.page_number = page_number
        self.page_type = page_type
        self.parent_page_number = parent_page_number
        self.md5_hex_digest = md5_hex_digest

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Index: {}\n" \
                 + padding + "Offset: {}\n" \
                 + padding + "Page Number: {}\n" \
                 + padding + "Page Type: {}\n" \
                 + padding + "Parent Page Number: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.index,
                             self.offset,
                             self.page_number,
                             self.page_type,
                             self.parent_page_number,
                             self.md5_hex_digest)


class BTreePage(Page):

    __metaclass__ = ABCMeta

    def __init__(self, version_interface, number, header_class_name, cell_class_name):

        super(BTreePage, self).__init__(version_interface, number)

        page = self._version_interface.get_page_data(self.number)

        self.page_type = None
        self.hex_type = page[0]

        if self.hex_type == MASTER_PAGE_HEX_ID:
            master_page_hex_type = page[SQLITE_DATABASE_HEADER_LENGTH]
            if master_page_hex_type == TABLE_INTERIOR_PAGE_HEX_ID:
                self.page_type = PAGE_TYPE.B_TREE_TABLE_INTERIOR
            elif master_page_hex_type == TABLE_LEAF_PAGE_HEX_ID:
                self.page_type = PAGE_TYPE.B_TREE_TABLE_LEAF
            else:
                log_message = "Page hex type for master page is: {} and not a table interior or table leaf page as " \
                              "expected in b-tree page: {} in page version: {} for version: {}."
                log_message = log_message.format(hexlify(master_page_hex_type), self.number,
                                                 self.page_version_number, self.version_number)
                self._logger.error(log_message)
                raise BTreePageParsingError(log_message)

        elif self.hex_type == TABLE_INTERIOR_PAGE_HEX_ID:
            self.page_type = PAGE_TYPE.B_TREE_TABLE_INTERIOR
        elif self.hex_type == TABLE_LEAF_PAGE_HEX_ID:
            self.page_type = PAGE_TYPE.B_TREE_TABLE_LEAF
        elif self.hex_type == INDEX_INTERIOR_PAGE_HEX_ID:
            self.page_type = PAGE_TYPE.B_TREE_INDEX_INTERIOR
        elif self.hex_type == INDEX_LEAF_PAGE_HEX_ID:
            self.page_type = PAGE_TYPE.B_TREE_INDEX_LEAF
        else:
            log_message = "Page hex type: {} is not a valid b-tree page type for b-tree page: {} in page version: {} " \
                          "for version: {}."
            log_message = log_message.format(hexlify(self.hex_type), self.number, self.page_version_number,
                                             self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        header_class = get_class_instance(header_class_name)
        cell_class = get_class_instance(cell_class_name)

        self.header = header_class(page)

        cell_pointer_array_offset = self.header.header_length
        if self.header.contains_sqlite_database_header:
            cell_pointer_array_offset += SQLITE_DATABASE_HEADER_LENGTH

            if self.number != SQLITE_MASTER_SCHEMA_ROOT_PAGE:
                log_message = "B-tree page found to contain the sqlite database header but is not the root page for " \
                              "b-tree page: {} in page version: {} for version: {}."
                log_message = log_message.format(self.number, self.page_version_number, self.version_number)
                self._logger.error(log_message)
                raise BTreePageParsingError(log_message)

        cell_pointer_array_length = self.header.number_of_cells_on_page * CELL_POINTER_BYTE_LENGTH
        self.unallocated_space_start_offset = cell_pointer_array_offset + cell_pointer_array_length
        self.unallocated_space_end_offset = self.header.cell_content_offset

        adjusted_header_length = self.header.header_length
        if self.header.contains_sqlite_database_header:
            adjusted_header_length += SQLITE_DATABASE_HEADER_LENGTH
        preface_size = adjusted_header_length + cell_pointer_array_length

        if preface_size != self.unallocated_space_start_offset:
            log_message = "The calculated preface size: {} is not equal to the unallocated space start offset: {} " \
                          "for b-tree page: {} in page version: {} for version: {}."
            log_message = log_message.format(preface_size, self.unallocated_space_start_offset, self.number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        if self.header.cell_content_offset != self.unallocated_space_end_offset:
            log_message = "The cell content offset in the header: {} is not equal to the unallocated space end " \
                          "offset: {} for b-tree page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.header.cell_content_offset, self.unallocated_space_end_offset,
                                             self.number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        self.cells = []
        self.calculated_cell_total_byte_size = 0
        for cell_index in range(self.header.number_of_cells_on_page):
            cell_start_offset = cell_pointer_array_offset + cell_index * CELL_POINTER_BYTE_LENGTH
            cell_end_offset = cell_start_offset + CELL_POINTER_BYTE_LENGTH
            cell_offset = unpack(b">H", page[cell_start_offset:cell_end_offset])[0]
            file_offset = self.offset + cell_offset
            cell_instance = cell_class(self._version_interface, self.page_version_number, file_offset, self.number,
                                       page, cell_index, cell_offset)
            self.cells.append(cell_instance)
            if type(cell_instance) != TableInteriorCell and cell_instance.has_overflow:
                overflow_adjusted_page_size = cell_instance.end_offset - cell_instance.start_offset
                self.calculated_cell_total_byte_size += overflow_adjusted_page_size
            else:
                self.calculated_cell_total_byte_size += cell_instance.byte_size

        if len(self.cells) != self.header.number_of_cells_on_page:
            log_message = "The number of cells parsed: {} does not equal the number of cells specified in the " \
                          "header: {} for b-tree page: {} in page version: {} for version: {}."
            log_message = log_message.format(len(self.cells), self.header.number_of_cells_on_page,
                                             self.number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        # Check if there are freeblocks specified in the header (0 if no freeblocks)
        self.freeblocks = []
        self.calculated_freeblock_total_byte_size = 0
        if self.header.first_freeblock_offset != 0:
            freeblock_index = 0
            next_freeblock_offset = self.header.first_freeblock_offset
            file_offset = self.offset + next_freeblock_offset
            while next_freeblock_offset:
                freeblock = Freeblock(self._version_interface, self.page_version_number, file_offset, self.number, page,
                                      freeblock_index, next_freeblock_offset)
                self.freeblocks.append(freeblock)
                next_freeblock_offset = freeblock.next_freeblock_offset
                self.calculated_freeblock_total_byte_size += freeblock.byte_size
                freeblock_index += 1

        # Find fragments
        self.fragments = []
        self.calculated_fragment_total_byte_size = 0
        fragment_index = 0
        aggregated_cells = sorted(self.cells + self.freeblocks, key=lambda b_tree_cell: b_tree_cell.start_offset)
        last_accounted_for_offset = self.unallocated_space_end_offset
        for cell in aggregated_cells:
            if last_accounted_for_offset >= self.size:
                log_message = "The last accounted for offset: {} while determining fragments is greater than or " \
                              "equal to the page size: {} for b-tree page: {} in page version: {} for version: {}."
                log_message = log_message.format(last_accounted_for_offset, self.size, self.number,
                                                 self.page_version_number, self.version_number)
                self._logger.error(log_message)
                raise BTreePageParsingError(log_message)

            if cell.start_offset != last_accounted_for_offset:
                file_offset = self.offset + last_accounted_for_offset
                fragment = Fragment(self._version_interface, self.page_version_number, file_offset, self.number, page,
                                    fragment_index, last_accounted_for_offset, cell.start_offset)
                self.fragments.append(fragment)
                self.calculated_fragment_total_byte_size += fragment.byte_size
                fragment_index += 1
            last_accounted_for_offset = cell.end_offset

        if self.header.number_of_fragmented_free_bytes > PAGE_FRAGMENT_LIMIT:
            log_message = "The number of fragmented free bytes: {} is greater than the page fragment limit: {} " \
                          "for b-tree page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.header.number_of_fragmented_free_bytes, PAGE_FRAGMENT_LIMIT,
                                             self.number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        if self.calculated_fragment_total_byte_size != self.header.number_of_fragmented_free_bytes:
            log_message = "The calculated fragment total byte size: {} does not equal the number of fragmented free " \
                          "bytes specified in the header: {} for b-tree page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.calculated_fragment_total_byte_size,
                                             self.header.number_of_fragmented_free_bytes,
                                             self.number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            if version_interface.strict_format_checking:
                raise BTreePageParsingError(log_message)
            else:
                warn(log_message, RuntimeWarning)

        # Account for all space within the page
        unallocated_space_size = self.unallocated_space_end_offset - self.unallocated_space_start_offset
        body_size = self.calculated_cell_total_byte_size
        body_size += self.calculated_freeblock_total_byte_size + self.calculated_fragment_total_byte_size

        accounted_for_space = preface_size + unallocated_space_size + body_size
        if accounted_for_space != self.size:
            log_message = "The calculated accounted for space: {} does not equal the page size: {} " \
                          "for b-tree page: {} in page version: {} for version: {}."
            log_message = log_message.format(accounted_for_space, self.size, self.number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            if version_interface.strict_format_checking:
                raise BTreePageParsingError(log_message)
            else:
                warn(log_message, RuntimeWarning)

        self.md5_hex_digest = get_md5_hash(page)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Hex Type (Hex): {}\n" \
                 + padding + "Header:\n{}\n"\
                 + padding + "Cells Length: {}\n" \
                 + padding + "Calculated Cell Total Byte Size: {}\n" \
                 + padding + "Freeblocks Length: {}\n" \
                 + padding + "Calculated Freeblock Total Byte Size: {}\n" \
                 + padding + "Fragments Length: {}\n" \
                 + padding + "Calculated Fragment Total Byte Size: {}"
        string = string.format(hexlify(self.hex_type),
                               self.header.stringify(padding + "\t"),
                               len(self.cells),
                               self.calculated_cell_total_byte_size,
                               len(self.freeblocks),
                               self.calculated_freeblock_total_byte_size,
                               len(self.fragments),
                               self.calculated_fragment_total_byte_size)
        for cell in self.cells:
            string += "\n" + padding + "Cell:\n{}".format(cell.stringify(padding + "\t"))
        for freeblock in self.freeblocks:
            string += "\n" + padding + "Freeblock:\n{}".format(freeblock.stringify(padding + "\t"))
        for fragment in self.fragments:
            string += "\n" + padding + "Fragment:\n{}".format(fragment.stringify(padding + "\t"))
        return super(BTreePage, self).stringify(padding) + string


class TableInteriorPage(BTreePage):

    def __init__(self, version_interface, number):
        header_class_name = "{}.{}".format(PAGE_HEADER_MODULE, INTERIOR_PAGE_HEADER_CLASS)
        cell_class_name = "{}.{}".format(CELL_MODULE, TABLE_INTERIOR_CELL_CLASS)
        super(TableInteriorPage, self).__init__(version_interface, number, header_class_name, cell_class_name)

        """

        Note: A table interior page can be updated without updating the right most pointer page in a version.

        """

        if not self.header.right_most_pointer:
            log_message = "The right most pointer is not set for b-tree table interior page: {} " \
                          "in page version: {} for version: {}."
            log_message = log_message.format(self.number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        right_most_pointer_page_hex_type = self._version_interface.get_page_data(self.header.right_most_pointer,
                                                                                 0, PAGE_TYPE_LENGTH)

        if right_most_pointer_page_hex_type == TABLE_INTERIOR_PAGE_HEX_ID:
            self.right_most_page = TableInteriorPage(self._version_interface, self.header.right_most_pointer)
        elif right_most_pointer_page_hex_type == TABLE_LEAF_PAGE_HEX_ID:
            self.right_most_page = TableLeafPage(self._version_interface, self.header.right_most_pointer)
        else:
            log_message = "The right most pointer does not point to a table interior or leaf page but instead has " \
                          "a hex type of: {} for b-tree table interior page: {} in page version: {} for version: {}."
            log_message = log_message.format(hexlify(right_most_pointer_page_hex_type), self.number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

    def stringify(self, padding=""):
        string = "\n" + padding + "Right Most Page:\n{}"
        string = string.format(self.right_most_page.stringify(padding + "\t") if self.right_most_page else None)
        return super(TableInteriorPage, self).stringify(padding) + string


class TableLeafPage(BTreePage):

    def __init__(self, version, number):
        header_class_name = "{}.{}".format(PAGE_HEADER_MODULE, LEAF_PAGE_HEADER_CLASS)
        cell_class_name = "{}.{}".format(CELL_MODULE, TABLE_LEAF_CELL_CLASS)
        super(TableLeafPage, self).__init__(version, number, header_class_name, cell_class_name)


class IndexInteriorPage(BTreePage):

    def __init__(self, version, number):

        header_class_name = "{}.{}".format(PAGE_HEADER_MODULE, INTERIOR_PAGE_HEADER_CLASS)
        cell_class_name = "{}.{}".format(CELL_MODULE, INDEX_INTERIOR_CELL_CLASS)
        super(IndexInteriorPage, self).__init__(version, number, header_class_name, cell_class_name)

        """

        Note: A index interior page can be updated without updating the right most pointer page in a version.

        """

        if not self.header.right_most_pointer:
            log_message = "The right most pointer is not set for b-tree index interior page: {} " \
                          "in page version: {} for version: {}."
            log_message = log_message.format(self.number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

        right_most_pointer_page_hex_type = self._version_interface.get_page_data(self.header.right_most_pointer,
                                                                                 0, PAGE_TYPE_LENGTH)

        if right_most_pointer_page_hex_type == INDEX_INTERIOR_PAGE_HEX_ID:
            self.right_most_page = IndexInteriorPage(self._version_interface, self.header.right_most_pointer)
        elif right_most_pointer_page_hex_type == INDEX_LEAF_PAGE_HEX_ID:
            self.right_most_page = IndexLeafPage(self._version_interface, self.header.right_most_pointer)
        else:
            log_message = "The right most pointer does not point to a index interior or leaf page but instead has " \
                          "a hex type of: {} for b-tree index interior page: {} in page version: {} for version: {}."
            log_message = log_message.format(hexlify(right_most_pointer_page_hex_type), self.number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise BTreePageParsingError(log_message)

    def stringify(self, padding=""):
        string = "\n" + padding + "Right Most Page:\n{}"
        string = string.format(self.right_most_page.stringify(padding + "\t") if self.right_most_page else None)
        return super(IndexInteriorPage, self).stringify(padding) + string


class IndexLeafPage(BTreePage):

    def __init__(self, version, number):
        header_class_name = "{}.{}".format(PAGE_HEADER_MODULE, LEAF_PAGE_HEADER_CLASS)
        cell_class_name = "{}.{}".format(CELL_MODULE, INDEX_LEAF_CELL_CLASS)
        super(IndexLeafPage, self).__init__(version, number, header_class_name, cell_class_name)


class BTreeCell(object):

    __metaclass__ = ABCMeta

    def __init__(self, version_interface, page_version_number, file_offset, page_number, index, offset,
                 source=CELL_SOURCE.B_TREE, location=None):

        self._logger = getLogger(LOGGER_NAME)

        self._version_interface = version_interface
        self._page_size = self._version_interface.page_size
        self.version_number = self._version_interface.version_number
        self.page_version_number = page_version_number
        self.file_offset = file_offset
        self.page_number = page_number
        self.index = index
        self.start_offset = offset
        self.location = location if location else CELL_LOCATION.ALLOCATED_SPACE
        self.source = source
        self.end_offset = None
        self.byte_size = None
        self.md5_hex_digest = None

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Version Number: {}\n" \
                 + padding + "Page Version Number: {}\n" \
                 + padding + "File Offset: {}\n" \
                 + padding + "Page Number: {}\n" \
                 + padding + "Source: {}\n" \
                 + padding + "Location: {}\n" \
                 + padding + "Index: {}\n" \
                 + padding + "Start Offset: {}\n" \
                 + padding + "End Offset: {}\n" \
                 + padding + "Byte Size: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.version_number,
                             self.page_version_number,
                             self.file_offset,
                             self.page_number,
                             self.source,
                             self.location,
                             self.index,
                             self.start_offset,
                             self.end_offset,
                             self.byte_size,
                             self.md5_hex_digest)


class TableInteriorCell(BTreeCell):

    """



    Note: B-Tree table interior cells never contain overflow.  Therefore they have no payload (ie. record).  This is
          the only type of b-tree page that does not have a payload.

    """

    def __init__(self, version_interface, page_version_number, file_offset, page_number, page, index, offset):

        super(TableInteriorCell, self).__init__(version_interface, page_version_number, file_offset,
                                                page_number, index, offset)
        left_child_pointer_end_offset = self.start_offset + LEFT_CHILD_POINTER_BYTE_LENGTH
        self.left_child_pointer = unpack(b">I", page[self.start_offset:left_child_pointer_end_offset])[0]
        self.row_id, self.row_id_varint_length = decode_varint(page, left_child_pointer_end_offset)

        self.byte_size = LEFT_CHILD_POINTER_BYTE_LENGTH + self.row_id_varint_length
        self.end_offset = self.start_offset + self.byte_size

        self.md5_hex_digest = get_md5_hash(page[self.start_offset:self.end_offset])

        """

        Note: A table interior cell can be updated without updating the left child page in a version.

        """

        if not self.left_child_pointer:
            log_message = "The left child pointer is not set for b-tree table interior cell index: {} " \
                          "at offset: {} for page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.index, self.start_offset, self.page_number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

        left_child_pointer_page_hex_type = self._version_interface.get_page_data(self.left_child_pointer,
                                                                                 0, PAGE_TYPE_LENGTH)

        if left_child_pointer_page_hex_type == TABLE_INTERIOR_PAGE_HEX_ID:
            self.left_child_page = TableInteriorPage(self._version_interface, self.left_child_pointer)
        elif left_child_pointer_page_hex_type == TABLE_LEAF_PAGE_HEX_ID:
            self.left_child_page = TableLeafPage(self._version_interface, self.left_child_pointer)
        else:
            log_message = "The left child pointer: {} does not point to a table interior or leaf page but instead " \
                          "has a hex type of: {} for b-tree table interior cell index: {} at offset: {} for page: {} " \
                          "in page version: {} for version: {}."
            log_message = log_message.format(self.left_child_pointer, hexlify(left_child_pointer_page_hex_type),
                                             self.index, self.start_offset, self.page_number, self.page_version_number,
                                             self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Left Child Pointer: {}\n" \
                 + padding + "Row ID: {}\n" \
                 + padding + "Row ID VARINT Length: {}"
        string = string.format(self.left_child_pointer,
                               self.row_id,
                               self.row_id_varint_length)
        string += "\n" + padding + "Left Child Page:\n{}"
        string = string.format(self.left_child_page.stringify(padding + "\t") if self.left_child_page else None)
        return super(TableInteriorCell, self).stringify(padding) + string


class TableLeafCell(BTreeCell):

    def __init__(self, version_interface, page_version_number, file_offset, page_number, page, index, offset):

        super(TableLeafCell, self).__init__(version_interface, page_version_number, file_offset,
                                            page_number, index, offset)

        self.payload_byte_size, self.payload_byte_size_varint_length = decode_varint(page, self.start_offset)
        row_id_offset = self.start_offset + self.payload_byte_size_varint_length
        self.row_id, self.row_id_varint_length = decode_varint(page, row_id_offset)
        self.payload_offset = self.start_offset + self.payload_byte_size_varint_length + self.row_id_varint_length

        self.has_overflow = False
        self.overflow_pages = None
        self.overflow_page_number_offset = None
        self.overflow_page_number = None
        self.overflow_page = None
        self.last_overflow_page_content_size = 0

        u = self._page_size
        p = self.payload_byte_size

        """

        Note:  According to the SQLite documentation (as of version 3.9.2) table leaf cell overflow is calculated
               by seeing if the payload size p is less than or equal to u - 35.  If it is then there is no overflow.
               If p is greater than u - 35, then there is overflow.  At this point m = (((u - 12) * 32) / 255) - 23.
               If p is greater than u - 35 then the number of bytes stored on the b-tree leaf page is the smaller of
               m + ((p - m) % (u - 4)) and u - 35.  The remaining bytes are then moved to overflow pages.

               The above was found to be wrong in the SQLite documentation.

               The documentation is incorrect that it is the smaller of m + ((p - m) % (u - 4)) and u - 35.  After
               a lot of testing and reviewing of the actual SQLite c code it was found out that the actual number of
               bytes stored on the b-tree leaf page is m + ((p - m) % (u - 4)) unless m + ((p - m) % (u - 4)) > u - 35
               in which case the bytes stored on the b-tree table leaf page is m itself.

               Therefore let b be the bytes on the b-tree table leaf page:
               u = page size
               p = payload byte size
               if p > u - 35
                    m = (((u - 12) * 32) / 255) - 23
                    b = m + ((p - m) % (u - 4))
                    if b > u - 35
                        b = m

               Additionally, the bytes stored on the b-tree table leaf page will always be greater to or equal to m
               once calculated.

        """

        self.bytes_on_first_page = p
        if p > u - 35:
            m = (((u - 12) * 32) / 255) - 23
            self.bytes_on_first_page = m + ((p - m) % (u - 4))
            if self.bytes_on_first_page > u - 35:
                self.bytes_on_first_page = m
            self.has_overflow = True
            self.overflow_page_number_offset = self.payload_offset + self.bytes_on_first_page
            overflow_page_number_end_offset = self.overflow_page_number_offset + FIRST_OVERFLOW_PAGE_NUMBER_LENGTH
            self.overflow_page_number = unpack(b">I", page[self.overflow_page_number_offset:
                                                           overflow_page_number_end_offset])[0]
            if self.bytes_on_first_page < m:
                log_message = "When calculating overflow, the bytes on the first page: {} calculated are less than " \
                              "m: {} for b-tree table leaf cell index: {} at offset: {} for page: {} in " \
                              "page version: {} for version: {}."
                log_message = log_message.format(self.bytes_on_first_page, m, self.index, self.start_offset,
                                                 self.page_number, self.page_version_number, self.version_number)
                self._logger.error(log_message)
                raise CellParsingError(log_message)

        self.byte_size = self.payload_byte_size_varint_length + self.row_id_varint_length + self.payload_byte_size
        self.byte_size += FIRST_OVERFLOW_PAGE_NUMBER_LENGTH if self.has_overflow else 0
        self.end_offset = self.start_offset + self.byte_size - self.payload_byte_size + self.bytes_on_first_page

        self.overflow_byte_size = self.payload_byte_size - self.bytes_on_first_page
        self.expected_number_of_overflow_pages, \
            self.expected_last_overflow_page_content_size = calculate_expected_overflow(self.overflow_byte_size, u)

        self.md5_hex_digest = get_md5_hash(page[self.start_offset:self.end_offset])

        if self.has_overflow:

            """

            The overflow pages are in a dictionary keyed off of their page number in the format:
            overflow_page[OVERFLOW_PAGE_NUMBER] = OVERFLOW_PAGE

            Originally, the overflow pages were nested objects, ie. each overflow page had the following overflow
            page within it, and so on.  However, this lead to recursion depth problems with larger cell content.
            It was changed to be a dictionary of pages here instead.

            Note:  Although overflow pages have to be replaced when any overflow page in a chain is updated, the
                   overflow here may not be updated due to a different cell in this page being updated.  Therefore,
                   we allow the first overflow page to be in a earlier version.  However, the overflow pages still
                   check that all overflow versions in respect to the first overflow page and beyond in the linked
                   list are all equal.

            """

            self.overflow_pages = {}
            payload_remaining = self.overflow_byte_size

            overflow_page = OverflowPage(self._version_interface, self.overflow_page_number, self.page_number,
                                         FIRST_OVERFLOW_PARENT_PAGE_NUMBER, FIRST_OVERFLOW_PAGE_INDEX,
                                         payload_remaining)

            self.overflow_pages[overflow_page.number] = overflow_page
            self.last_overflow_page_content_size = overflow_page.content_length

            while overflow_page.next_overflow_page_number:
                payload_remaining = payload_remaining - overflow_page.size + OVERFLOW_HEADER_LENGTH
                overflow_page = OverflowPage(self._version_interface, overflow_page.next_overflow_page_number,
                                             self.page_number, overflow_page.number, overflow_page.index + 1,
                                             payload_remaining)
                self.overflow_pages[overflow_page.number] = overflow_page
                self.last_overflow_page_content_size = overflow_page.content_length

        if self.expected_number_of_overflow_pages != self.number_of_overflow_pages:
            log_message = "The number of expected overflow pages: {} was not the actual number of overflow pages " \
                          "parsed: {} for b-tree table leaf cell index: {} at offset: {} for page: {} in " \
                          "page version: {} for version: {}."
            log_message = log_message.format(self.expected_number_of_overflow_pages, self.number_of_overflow_pages,
                                             self.index, self.start_offset, self.page_number, self.page_version_number,
                                             self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

        if self.expected_last_overflow_page_content_size != self.last_overflow_page_content_size:
            log_message = "The expected last overflow page content size: {} was not the actual last overflow page " \
                          "content size parsed: {} for b-tree table leaf cell index: {} at offset: {} for page: {} " \
                          "in page version: {} for version: {}."
            log_message = log_message.format(self.expected_last_overflow_page_content_size,
                                             self.last_overflow_page_content_size, self.index, self.start_offset,
                                             self.page_number, self.page_version_number, self.version_number)
            raise CellParsingError(log_message)

        self.payload = Record(page, self.payload_offset, self.payload_byte_size,
                              self.bytes_on_first_page, self.overflow)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Payload Byte Size: {}\n" \
                 + padding + "Payload Byte Size VARINT Length: {}\n" \
                 + padding + "Row ID: {}\n" \
                 + padding + "Row ID VARINT Length: {}\n" \
                 + padding + "Payload Offset: {}\n" \
                 + padding + "Bytes on First Page: {}\n" \
                 + padding + "Has Overflow: {}\n" \
                 + padding + "Overflow Byte Size: {}\n" \
                 + padding + "Expected Number of Overflow Pages: {}\n" \
                 + padding + "Expected Last Overflow Page Content Size: {}\n" \
                 + padding + "Number of Overflow Pages: {}\n" \
                 + padding + "Overflow Page Number Offset: {}\n" \
                 + padding + "Overflow Page Number: {}\n" \
                 + padding + "Last Overflow Page Content Size: {}\n" \
                 + padding + "Overflow (Hex): {}"
        string = string.format(self.payload_byte_size,
                               self.payload_byte_size_varint_length,
                               self.row_id,
                               self.row_id_varint_length,
                               self.payload_offset,
                               self.bytes_on_first_page,
                               self.has_overflow,
                               self.overflow_byte_size,
                               self.expected_number_of_overflow_pages,
                               self.expected_last_overflow_page_content_size,
                               self.number_of_overflow_pages,
                               self.overflow_page_number_offset,
                               self.overflow_page_number,
                               self.last_overflow_page_content_size,
                               hexlify(self.overflow))
        string += "\n" + padding + "Payload:\n{}".format(self.payload.stringify(padding + "\t"))
        if self.has_overflow:
            overflow_page = self.overflow_pages[self.overflow_page_number]
            string += "\n" + padding + "Overflow Page:\n{}".format(self.overflow_page.stringify(padding + "\t"))
            while overflow_page.next_overflow_page_number:
                overflow_page = self.overflow_pages[overflow_page.next_overflow_page_number]
                string += "\n" + padding + "Overflow Page:\n{}".format(self.overflow_page.stringify(padding + "\t"))
        return super(TableLeafCell, self).stringify(padding) + string

    @property
    def number_of_overflow_pages(self):
        return len(self.overflow_pages) if self.overflow_pages else 0

    @property
    def overflow(self):

        overflow = bytearray()

        if not self.has_overflow:

            return overflow

        else:

            overflow_page = self.overflow_pages[self.overflow_page_number]
            overflow += overflow_page.content
            while overflow_page.next_overflow_page_number:
                overflow_page = self.overflow_pages[overflow_page.next_overflow_page_number]
                overflow += overflow_page.content

            if len(overflow) != self.overflow_byte_size:
                log_message = "The expected overflow size: {} did not match the overflow size parsed: {} " \
                              "for b-tree table leaf cell index: {} at offset: {} for page: {} " \
                              "in page version: {} for version: {}."
                log_message = log_message.format(self.overflow_byte_size, len(overflow), self.index, self.start_offset,
                                                 self.page_number, self.page_version_number, self.version_number)
                raise CellParsingError(log_message)

            return overflow


class IndexInteriorCell(BTreeCell):

    def __init__(self, version_interface, page_version_number, file_offset, page_number, page, index, offset):

        super(IndexInteriorCell, self).__init__(version_interface, page_version_number, file_offset,
                                                page_number, index, offset)

        left_child_pointer_end_offset = self.start_offset + LEFT_CHILD_POINTER_BYTE_LENGTH
        self.left_child_pointer = unpack(b">I", page[self.start_offset:left_child_pointer_end_offset])[0]
        self.payload_byte_size, self.payload_byte_size_varint_length = decode_varint(page,
                                                                                     left_child_pointer_end_offset)
        self.payload_offset = left_child_pointer_end_offset + self.payload_byte_size_varint_length

        self.has_overflow = False
        self.overflow_pages = None
        self.overflow_page_number_offset = None
        self.overflow_page_number = None
        self.overflow_page = None
        self.last_overflow_page_content_size = 0

        u = self._page_size
        p = self.payload_byte_size
        x = (((u - 12) * 64) / 255) - 23

        """

        Note:  According to the SQLite documentation (as of version 3.9.2) index interior and leaf cell overflow is
               calculated by first calculating x as (((u - 12) * 64) / 255) - 23.  If the payload size p is less than
               or equal to x, then there is no overflow.  If p is greater than x, than m = (((u - 12) * 32) / 255) - 23.
               If p is greater than x then the number of bytes stored on the b-tree leaf page is the smaller of
               m + ((p - m) % (u - 4)) and x.  The remaining bytes are then moved to overflow pages.

               The above was found to be wrong in the SQLite documentation.

               The documentation is incorrect that it is the smaller of m + ((p - m) % (u - 4)) and x.  After
               a lot of testing and reviewing of the actual SQLite c code it was found out that the actual number of
               bytes stored on the b-tree leaf page is m + ((p - m) % (u - 4)) unless m + ((p - m) % (u - 4)) > x
               in which case the bytes stored on the b-tree index interior or index leaf page is m itself.

               Therefore let b be the bytes on the b-tree index interior or index leaf page:
               u = page size
               p = payload byte size
               x = (((u - 12) * 64) / 255) - 23
               if p > x
                    m = (((u - 12) * 32) / 255) - 23
                    b = m + ((p - m) % (u - 4))
                    if b > x
                        b = m

               Additionally, the bytes stored on the b-tree index interior or index leaf page will always be greater
               to or equal to m once calculated.

        """

        self.bytes_on_first_page = p
        if p > x:
            m = (((u - 12) * 32) / 255) - 23
            self.bytes_on_first_page = m + ((p - m) % (u - 4))
            if self.bytes_on_first_page > x:
                self.bytes_on_first_page = m
            self.has_overflow = True
            self.overflow_page_number_offset = self.payload_offset + self.bytes_on_first_page
            overflow_page_number_end_offset = self.overflow_page_number_offset + FIRST_OVERFLOW_PAGE_NUMBER_LENGTH
            self.overflow_page_number = unpack(b">I", page[self.overflow_page_number_offset:
                                                           overflow_page_number_end_offset])[0]
            if self.bytes_on_first_page < m:
                log_message = "When calculating overflow, the bytes on the first page: {} calculated are less than " \
                              "m: {} for b-tree index interior cell index: {} at offset: {} for page: {} in " \
                              "page version: {} for version: {}."
                log_message = log_message.format(self.bytes_on_first_page, m, self.index, self.start_offset,
                                                 self.page_number, self.page_version_number, self.version_number)
                self._logger.error(log_message)
                raise CellParsingError(log_message)

        self.byte_size = LEFT_CHILD_POINTER_BYTE_LENGTH
        self.byte_size += self.payload_byte_size_varint_length + self.payload_byte_size
        self.byte_size += FIRST_OVERFLOW_PAGE_NUMBER_LENGTH if self.has_overflow else 0
        self.end_offset = self.start_offset + self.byte_size - self.payload_byte_size + self.bytes_on_first_page

        self.overflow_byte_size = self.payload_byte_size - self.bytes_on_first_page
        self.expected_number_of_overflow_pages, \
            self.expected_last_overflow_page_content_size = calculate_expected_overflow(self.overflow_byte_size, u)

        self.md5_hex_digest = get_md5_hash(page[self.start_offset:self.end_offset])

        if self.has_overflow:

            """

            The overflow pages are in a dictionary keyed off of their page number in the format:
            overflow_page[OVERFLOW_PAGE_NUMBER] = OVERFLOW_PAGE

            Originally, the overflow pages were nested objects, ie. each overflow page had the following overflow
            page within it, and so on.  However, this lead to recursion depth problems with larger cell content.
            It was changed to be a dictionary of pages here instead.

            Note:  Although overflow pages have to be replaced when any overflow page in a chain is updated, the
                   overflow here may not be updated due to a different cell in this page being updated.  Therefore,
                   we allow the first overflow page to be in a earlier version.  However, the overflow pages still
                   check that all overflow versions in respect to the first overflow page and beyond in the linked
                   list are all equal.

            """

            self.overflow_pages = {}
            payload_remaining = self.overflow_byte_size

            overflow_page = OverflowPage(self._version_interface, self.overflow_page_number, self.page_number,
                                         FIRST_OVERFLOW_PARENT_PAGE_NUMBER, FIRST_OVERFLOW_PAGE_INDEX,
                                         payload_remaining)

            self.overflow_pages[overflow_page.number] = overflow_page
            self.last_overflow_page_content_size = overflow_page.content_length

            while overflow_page.next_overflow_page_number:
                payload_remaining = payload_remaining - overflow_page.size + OVERFLOW_HEADER_LENGTH
                overflow_page = OverflowPage(self._version_interface, overflow_page.next_overflow_page_number,
                                             self.page_number, overflow_page.number, overflow_page.index + 1,
                                             payload_remaining)
                self.overflow_pages[overflow_page.number] = overflow_page
                self.last_overflow_page_content_size = overflow_page.content_length

        if self.expected_number_of_overflow_pages != self.number_of_overflow_pages:
            log_message = "The number of expected overflow pages: {} was not the actual number of overflow pages " \
                          "parsed: {} for b-tree index interior cell index: {} at offset: {} for page: {} in " \
                          "page version: {} for version: {}."
            log_message = log_message.format(self.expected_number_of_overflow_pages, self.number_of_overflow_pages,
                                             self.index, self.start_offset, self.page_number, self.page_version_number,
                                             self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

        if self.expected_last_overflow_page_content_size != self.last_overflow_page_content_size:
            log_message = "The expected last overflow page content size: {} was not the actual last overflow page " \
                          "content size parsed: {} for b-tree index interior cell index: {} at offset: {} for " \
                          "page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.expected_last_overflow_page_content_size,
                                             self.last_overflow_page_content_size, self.index, self.start_offset,
                                             self.page_number, self.page_version_number, self.version_number)
            raise CellParsingError(log_message)

        self.payload = Record(page, self.payload_offset, self.payload_byte_size,
                              self.bytes_on_first_page, self.overflow)

        """

        Note: An index interior cell can be updated without updating the left child page in a version.

        """

        if not self.left_child_pointer:
            log_message = "The left child pointer is not set for b-tree index interior cell index: {} " \
                          "at offset: {} for page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.index, self.start_offset, self.page_number,
                                             self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

        left_child_pointer_page_hex_type = self._version_interface.get_page_data(self.left_child_pointer,
                                                                                 0, PAGE_TYPE_LENGTH)

        if left_child_pointer_page_hex_type == INDEX_INTERIOR_PAGE_HEX_ID:
            self.left_child_page = IndexInteriorPage(self._version_interface, self.left_child_pointer)
        elif left_child_pointer_page_hex_type == INDEX_LEAF_PAGE_HEX_ID:
            self.left_child_page = IndexLeafPage(self._version_interface, self.left_child_pointer)
        else:
            log_message = "The left child pointer does not point to a index interior or index page but instead has " \
                          "a hex type of: {} for b-tree index interior cell index: {} at offset: {} for page: {} " \
                          "in page version: {} for version: {}."
            log_message = log_message.format(hexlify(left_child_pointer_page_hex_type), self.index, self.start_offset,
                                             self.page_number, self.page_version_number, self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Left Child Pointer: {}\n" \
                 + padding + "Payload Byte Size: {}\n" \
                 + padding + "Payload Byte Size VARINT Length: {}\n" \
                 + padding + "Payload Offset: {}\n" \
                 + padding + "Bytes on First Page: {}\n" \
                 + padding + "Has Overflow: {}\n" \
                 + padding + "Overflow Byte Size: {}\n" \
                 + padding + "Expected Number of Overflow Pages: {}\n" \
                 + padding + "Expected Last Overflow Page Content Size: {}\n" \
                 + padding + "Number of Overflow Pages: {}\n" \
                 + padding + "Overflow Page Number Offset: {}\n" \
                 + padding + "Overflow Page Number: {}\n" \
                 + padding + "Last Overflow Page Content Size: {}\n" \
                 + padding + "Overflow (Hex): {}"
        string = string.format(self.left_child_pointer,
                               self.payload_byte_size,
                               self.payload_byte_size_varint_length,
                               self.payload_offset,
                               self.bytes_on_first_page,
                               self.has_overflow,
                               self.overflow_byte_size,
                               self.expected_number_of_overflow_pages,
                               self.expected_last_overflow_page_content_size,
                               self.number_of_overflow_pages,
                               self.overflow_page_number_offset,
                               self.overflow_page_number,
                               self.last_overflow_page_content_size,
                               hexlify(self.overflow))
        string += "\n" + padding + "Payload:\n{}".format(self.payload.stringify(padding + "\t"))
        if self.has_overflow:
            overflow_page = self.overflow_pages[self.overflow_page_number]
            string += "\n" + padding + "Overflow Page:\n{}".format(self.overflow_page.stringify(padding + "\t"))
            while overflow_page.next_overflow_page_number:
                overflow_page = self.overflow_pages[overflow_page.next_overflow_page_number]
                string += "\n" + padding + "Overflow Page:\n{}".format(self.overflow_page.stringify(padding + "\t"))
        string += "\n" + padding + "Left Child Page:\n{}"
        string = string.format(self.left_child_page.stringify(padding + "\t") if self.left_child_page else None)
        return super(IndexInteriorCell, self).stringify(padding) + string

    @property
    def number_of_overflow_pages(self):
        return len(self.overflow_pages) if self.overflow_pages else 0

    @property
    def overflow(self):
        overflow = bytearray()

        if not self.has_overflow:

            return overflow

        else:

            overflow_page = self.overflow_pages[self.overflow_page_number]
            overflow += overflow_page.content
            while overflow_page.next_overflow_page_number:
                overflow_page = self.overflow_pages[overflow_page.next_overflow_page_number]
                overflow += overflow_page.content

            if len(overflow) != self.overflow_byte_size:
                log_message = "The expected overflow size: {} did not match the overflow size parsed: {} " \
                              "for b-tree table leaf cell index: {} at offset: {} for page: {} " \
                              "in page version: {} for version: {}."
                log_message = log_message.format(self.overflow_byte_size, len(overflow), self.index, self.start_offset,
                                                 self.page_number, self.page_version_number, self.version_number)
                raise CellParsingError(log_message)

            return overflow


class IndexLeafCell(BTreeCell):

    def __init__(self, version_interface, page_version_number, file_offset, page_number, page, index, offset):

        super(IndexLeafCell, self).__init__(version_interface, page_version_number, file_offset,
                                            page_number, index, offset)

        self.payload_byte_size, self.payload_byte_size_varint_length = decode_varint(page, self.start_offset)
        self.payload_offset = self.start_offset + self.payload_byte_size_varint_length

        self.has_overflow = False
        self.overflow_pages = 0
        self.overflow_page_number_offset = None
        self.overflow_page_number = None
        self.overflow_page = None
        self.last_overflow_page_content_size = 0

        u = self._page_size
        p = self.payload_byte_size
        x = (((u - 12) * 64) / 255) - 23

        """

        Note:  According to the SQLite documentation (as of version 3.9.2) index interior and leaf cell overflow is
               calculated by first calculating x as (((u - 12) * 64) / 255) - 23.  If the payload size p is less than
               or equal to x, then there is no overflow.  If p is greater than x, than m = (((u - 12) * 32) / 255) - 23.
               If p is greater than x then the number of bytes stored on the b-tree leaf page is the smaller of
               m + ((p - m) % (u - 4)) and x.  The remaining bytes are then moved to overflow pages.

               The above was found to be wrong in the SQLite documentation.

               The documentation is incorrect that it is the smaller of m + ((p - m) % (u - 4)) and x.  After
               a lot of testing and reviewing of the actual SQLite c code it was found out that the actual number of
               bytes stored on the b-tree leaf page is m + ((p - m) % (u - 4)) unless m + ((p - m) % (u - 4)) > x
               in which case the bytes stored on the b-tree index interior or index leaf page is m itself.

               Therefore let b be the bytes on the b-tree index interior or index leaf page:
               u = page size
               p = payload byte size
               x = (((u - 12) * 64) / 255) - 23
               if p > x
                    m = (((u - 12) * 32) / 255) - 23
                    b = m + ((p - m) % (u - 4))
                    if b > x
                        b = m

               Additionally, the bytes stored on the b-tree index interior or index leaf page will always be greater
               to or equal to m once calculated.

        """

        self.bytes_on_first_page = p
        if p > x:
            m = (((u - 12) * 32) / 255) - 23
            self.bytes_on_first_page = m + ((p - m) % (u - 4))
            if self.bytes_on_first_page > x:
                self.bytes_on_first_page = m
            self.has_overflow = True
            self.overflow_page_number_offset = self.payload_offset + self.bytes_on_first_page
            overflow_page_number_end_offset = self.overflow_page_number_offset + FIRST_OVERFLOW_PAGE_NUMBER_LENGTH
            self.overflow_page_number = unpack(b">I", page[self.overflow_page_number_offset:
                                                           overflow_page_number_end_offset])[0]
            if self.bytes_on_first_page < m:
                log_message = "When calculating overflow, the bytes on the first page: {} calculated are less than " \
                              "m: {} for b-tree leaf interior cell index: {} at offset: {} for page: {} in " \
                              "page version: {} for version: {}."
                log_message = log_message.format(self.bytes_on_first_page, m, self.index, self.start_offset,
                                                 self.page_number, self.page_version_number, self.version_number)
                self._logger.error(log_message)
                raise CellParsingError(log_message)

        self.byte_size = self.payload_byte_size_varint_length + self.payload_byte_size
        self.byte_size += FIRST_OVERFLOW_PAGE_NUMBER_LENGTH if self.has_overflow else 0
        self.end_offset = self.start_offset + self.byte_size - self.payload_byte_size + self.bytes_on_first_page

        self.overflow_byte_size = self.payload_byte_size - self.bytes_on_first_page
        self.expected_number_of_overflow_pages, \
            self.expected_last_overflow_page_content_size = calculate_expected_overflow(self.overflow_byte_size, u)

        self.md5_hex_digest = get_md5_hash(page[self.start_offset:self.end_offset])

        if self.has_overflow:

            """

            The overflow pages are in a dictionary keyed off of their page number in the format:
            overflow_page[OVERFLOW_PAGE_NUMBER] = OVERFLOW_PAGE

            Originally, the overflow pages were nested objects, ie. each overflow page had the following overflow
            page within it, and so on.  However, this lead to recursion depth problems with larger cell content.
            It was changed to be a dictionary of pages here instead.

            Note:  Although overflow pages have to be replaced when any overflow page in a chain is updated, the
                   overflow here may not be updated due to a different cell in this page being updated.  Therefore,
                   we allow the first overflow page to be in a earlier version.  However, the overflow pages still
                   check that all overflow versions in respect to the first overflow page and beyond in the linked
                   list are all equal.

            """

            self.overflow_pages = {}
            payload_remaining = self.overflow_byte_size

            overflow_page = OverflowPage(self._version_interface, self.overflow_page_number, self.page_number,
                                         FIRST_OVERFLOW_PARENT_PAGE_NUMBER, FIRST_OVERFLOW_PAGE_INDEX,
                                         payload_remaining)

            self.overflow_pages[overflow_page.number] = overflow_page
            self.last_overflow_page_content_size = overflow_page.content_length

            while overflow_page.next_overflow_page_number:
                payload_remaining = payload_remaining - overflow_page.size + OVERFLOW_HEADER_LENGTH
                overflow_page = OverflowPage(self._version_interface, overflow_page.next_overflow_page_number,
                                             self.page_number, overflow_page.number, overflow_page.index + 1,
                                             payload_remaining)
                self.overflow_pages[overflow_page.number] = overflow_page
                self.last_overflow_page_content_size = overflow_page.content_length

        if self.expected_number_of_overflow_pages != self.number_of_overflow_pages:
            log_message = "The number of expected overflow pages: {} was not the actual number of overflow pages " \
                          "parsed: {} for b-tree index leaf cell index: {} at offset: {} for page: {} in " \
                          "page version: {} for version: {}."
            log_message = log_message.format(self.expected_number_of_overflow_pages, self.number_of_overflow_pages,
                                             self.index, self.start_offset, self.page_number, self.page_version_number,
                                             self.version_number)
            self._logger.error(log_message)
            raise CellParsingError(log_message)

        if self.expected_last_overflow_page_content_size != self.last_overflow_page_content_size:
            log_message = "The expected last overflow page content size: {} was not the actual last overflow page " \
                          "content size parsed: {} for b-tree index leaf cell index: {} at offset: {} for " \
                          "page: {} in page version: {} for version: {}."
            log_message = log_message.format(self.expected_last_overflow_page_content_size,
                                             self.last_overflow_page_content_size, self.index, self.start_offset,
                                             self.page_number, self.page_version_number, self.version_number)
            raise CellParsingError(log_message)

        self.payload = Record(page, self.payload_offset, self.payload_byte_size,
                              self.bytes_on_first_page, self.overflow)

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Payload Byte Size: {}\n" \
                 + padding + "Payload Byte Size VARINT Length: {}\n" \
                 + padding + "Payload Offset: {}\n" \
                 + padding + "Bytes on First Page: {}\n" \
                 + padding + "Has Overflow: {}\n" \
                 + padding + "Overflow Byte Size: {}\n" \
                 + padding + "Expected Number of Overflow Pages: {}\n" \
                 + padding + "Expected Last Overflow Page Content Size: {}\n" \
                 + padding + "Number of Overflow Pages: {}\n" \
                 + padding + "Overflow Page Number Offset: {}\n" \
                 + padding + "Overflow Page Number: {}\n" \
                 + padding + "Last Overflow Page Content Size: {}\n" \
                 + padding + "Overflow (Hex): {}"
        string = string.format(self.payload_byte_size,
                               self.payload_byte_size_varint_length,
                               self.payload_offset,
                               self.bytes_on_first_page,
                               self.has_overflow,
                               self.overflow_byte_size,
                               self.expected_number_of_overflow_pages,
                               self.expected_last_overflow_page_content_size,
                               self.number_of_overflow_pages,
                               self.overflow_page_number_offset,
                               self.overflow_page_number,
                               self.last_overflow_page_content_size,
                               hexlify(self.overflow))
        string += "\n" + padding + "Payload:\n{}".format(self.payload.stringify(padding + "\t"))
        if self.has_overflow:
            overflow_page = self.overflow_pages[self.overflow_page_number]
            string += "\n" + padding + "Overflow Page:\n{}".format(self.overflow_page.stringify(padding + "\t"))
            while overflow_page.next_overflow_page_number:
                overflow_page = self.overflow_pages[overflow_page.next_overflow_page_number]
                string += "\n" + padding + "Overflow Page:\n{}".format(self.overflow_page.stringify(padding + "\t"))
        return super(IndexLeafCell, self).stringify(padding) + string

    @property
    def number_of_overflow_pages(self):
        return len(self.overflow_pages) if self.overflow_pages else 0

    @property
    def overflow(self):
        overflow = bytearray()

        if not self.has_overflow:

            return overflow

        else:

            overflow_page = self.overflow_pages[self.overflow_page_number]
            overflow += overflow_page.content
            while overflow_page.next_overflow_page_number:
                overflow_page = self.overflow_pages[overflow_page.next_overflow_page_number]
                overflow += overflow_page.content

            if len(overflow) != self.overflow_byte_size:
                log_message = "The expected overflow size: {} did not match the overflow size parsed: {} " \
                              "for b-tree table leaf cell index: {} at offset: {} for page: {} " \
                              "in page version: {} for version: {}."
                log_message = log_message.format(self.overflow_byte_size, len(overflow), self.index, self.start_offset,
                                                 self.page_number, self.page_version_number, self.version_number)
                raise CellParsingError(log_message)

            return overflow


class Freeblock(BTreeCell):

    def __init__(self, version_interface, page_version_number, file_offset, page_number, page, index, offset):

        super(Freeblock, self).__init__(version_interface, page_version_number, file_offset, page_number, index, offset)

        next_freeblock_end_offset = self.start_offset + NEXT_FREEBLOCK_OFFSET_LENGTH
        self.next_freeblock_offset = unpack(b">H", page[self.start_offset:next_freeblock_end_offset])[0]
        self.content_start_offset = next_freeblock_end_offset + FREEBLOCK_BYTE_LENGTH
        self.byte_size = unpack(b">H", page[next_freeblock_end_offset:self.content_start_offset])[0]
        self.content_end_offset = self.start_offset + self.byte_size
        self.end_offset = self.content_end_offset

        self.content_length = self.end_offset - self.content_start_offset

        self.md5_hex_digest = get_md5_hash(page[self.start_offset:self.end_offset])

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Next Freeblock Offset: {}\n" \
                 + padding + "Content Start Offset: {}\n" \
                 + padding + "Content End Offset: {}\n" \
                 + padding + "Content Length: {}\n" \
                 + padding + "Content (Hex): {}"
        string = string.format(self.next_freeblock_offset,
                               self.content_start_offset,
                               self.content_end_offset,
                               self.content_length,
                               hexlify(self.content))
        return super(Freeblock, self).stringify(padding) + string

    @property
    def content(self):

        """

        This property returns the content inside this freeblock.  This is only the body of the freeblock, unallocated
        portion, and does not include the 4 byte freeblock header.

        :return: bytearray The byte array for freeblock content.

        """

        if self.content_length == 0:
            return bytearray()
        else:
            return self._version_interface.get_page_data(self.page_number, self.content_start_offset,
                                                         self.content_length)


class Fragment(BTreeCell):

    """



    Note:  It is important to note that fragments are three bytes in length or less.  If four bytes or more become
           unallocated within the cell area of the page, then a freeblock is created since four bytes are required.
           (The first two bytes pointing to the offset of the next freeblock offset in the freeblock linked list
           on the page and the second two bytes being the size of the freeblock in bytes including this 4 byte header.)

           However, fragments can be found with byte sizes greater than three.  This occurs due to the fact that
           multiple cells could be added and deleted next to each other creating fragments of size of 3 or less next
           to each other.  Since we cannot determine exactly where the break between these fragments are, we specify
           the whole block as a fragment resulting in fragment sizes greater than the limit of 3 bytes.

           Therefore, if the fragment is greater than 3 bytes it is comprised of multiple fragments.  Keep in mind
           however that although this is true, the inverse is not true.  If a fragment is three bytes or less, it could
           still be an aggregate of multiple fragments such as a fragment of 1 byte and another fragment of 2 bytes.

    Note:  Since the byte size is the size of the actual content, there is not content size.

    """

    def __init__(self, version_interface, page_version_number, file_offset, page_number,
                 page, index, start_offset, end_offset):

        super(Fragment, self).__init__(version_interface, page_version_number, file_offset,
                                       page_number, index, start_offset)

        self.end_offset = end_offset
        self.byte_size = self.end_offset - self.start_offset

        self.md5_hex_digest = get_md5_hash(page[self.start_offset:self.end_offset])

    def stringify(self, padding=""):
        string = "\n" + padding + "Content (Hex): {}"
        string = string.format(hexlify(self.content))
        return super(Fragment, self).stringify(padding) + string

    @property
    def content(self):

        """

        This property returns the content inside this fragment.

        :return: bytearray The byte array for fragment content.

        """

        return self._version_interface.get_page_data(self.page_number, self.start_offset, self.end_offset)
