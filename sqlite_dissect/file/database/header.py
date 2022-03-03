from abc import ABCMeta
from binascii import hexlify
from logging import getLogger
from re import compile
from re import sub
from struct import error
from struct import unpack
from warnings import warn
from sqlite_dissect.constants import DATABASE_TEXT_ENCODINGS
from sqlite_dissect.constants import INTERIOR_PAGE_HEADER_LENGTH
from sqlite_dissect.constants import LEAF_PAGE_HEADER_LENGTH
from sqlite_dissect.constants import LEAF_PAYLOAD_FRACTION
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MAGIC_HEADER_STRING
from sqlite_dissect.constants import MAGIC_HEADER_STRING_ENCODING
from sqlite_dissect.constants import MASTER_PAGE_HEX_ID
from sqlite_dissect.constants import MAXIMUM_EMBEDDED_PAYLOAD_FRACTION
from sqlite_dissect.constants import MAXIMUM_PAGE_SIZE
from sqlite_dissect.constants import MAXIMUM_PAGE_SIZE_INDICATOR
from sqlite_dissect.constants import MAXIMUM_PAGE_SIZE_LIMIT
from sqlite_dissect.constants import MINIMUM_EMBEDDED_PAYLOAD_FRACTION
from sqlite_dissect.constants import MINIMUM_PAGE_SIZE_LIMIT
from sqlite_dissect.constants import RESERVED_FOR_EXPANSION_REGEX
from sqlite_dissect.constants import RIGHT_MOST_POINTER_LENGTH
from sqlite_dissect.constants import RIGHT_MOST_POINTER_OFFSET
from sqlite_dissect.constants import ROLLBACK_JOURNALING_MODE
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from sqlite_dissect.constants import VALID_SCHEMA_FORMATS
from sqlite_dissect.constants import WAL_JOURNALING_MODE
from sqlite_dissect.constants import HUMAN_READABLE_JOURNALING_MODES
from sqlite_dissect.constants import HUMAN_READABLE_DATABASE_TEXT_ENCODINGS
from sqlite_dissect.exception import HeaderParsingError
from sqlite_dissect.file.header import SQLiteHeader
from sqlite_dissect.utilities import get_md5_hash

"""

header.py

This script holds the header objects used for parsing the header of the database file structure from the root page.

This script holds the following object(s):
DatabaseHeader(SQLiteHeader)

"""


class DatabaseHeader(SQLiteHeader):

    def __init__(self, database_header_byte_array):

        super(DatabaseHeader, self).__init__()

        logger = getLogger(LOGGER_NAME)

        if len(database_header_byte_array) != SQLITE_DATABASE_HEADER_LENGTH:
            log_message = "The database header byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(database_header_byte_array), SQLITE_DATABASE_HEADER_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        try:

            self.magic_header_string = database_header_byte_array[0:16]

        except error:

            logger.error("Failed to retrieve the magic header.")
            raise

        if self.magic_header_string != MAGIC_HEADER_STRING.decode(MAGIC_HEADER_STRING_ENCODING):
            log_message = "The magic header string is invalid."
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        try:

            self.page_size = unpack(b">H", database_header_byte_array[16:18])[0]

        except error:

            logger.error("Failed to retrieve the page size.")
            raise

        if self.page_size == MAXIMUM_PAGE_SIZE_INDICATOR:
            self.page_size = MAXIMUM_PAGE_SIZE
        elif self.page_size < MINIMUM_PAGE_SIZE_LIMIT:
            log_message = "The page size: {} is less than the minimum page size limit: {}."
            log_message = log_message.format(self.page_size, MINIMUM_PAGE_SIZE_LIMIT)
            logger.error(log_message)
            raise HeaderParsingError(log_message)
        elif self.page_size > MAXIMUM_PAGE_SIZE_LIMIT:
            log_message = "The page size: {} is greater than the maximum page size limit: {}."
            log_message = log_message.format(self.page_size, MAXIMUM_PAGE_SIZE_LIMIT)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        try:

            self.file_format_write_version = ord(database_header_byte_array[18:19])

        except TypeError:

            logger.error("Failed to retrieve the file format write version.")
            raise

        if self.file_format_write_version not in [ROLLBACK_JOURNALING_MODE, WAL_JOURNALING_MODE]:
            log_message = "The file format write version: {} is invalid.".format(self.file_format_write_version)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        try:

            self.file_format_read_version = ord(database_header_byte_array[19:20])

        except TypeError:

            logger.error("Failed to retrieve the file format read version.")
            raise

        if self.file_format_read_version not in [ROLLBACK_JOURNALING_MODE, WAL_JOURNALING_MODE]:
            log_message = "The file format read version: {} is invalid.".format(self.file_format_read_version)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        try:

            self.reserved_bytes_per_page = ord(database_header_byte_array[20:21])

        except TypeError:

            logger.error("Failed to retrieve the reserved bytes per page.")
            raise

        if self.reserved_bytes_per_page != 0:
            log_message = "Reserved bytes per page is not 0 but {} and is not implemented."
            log_message = log_message.format(self.reserved_bytes_per_page)
            logger.error(log_message)
            raise NotImplementedError(log_message)

        try:

            self.maximum_embedded_payload_fraction = ord(database_header_byte_array[21:22])

        except TypeError:

            logger.error("Failed to retrieve the maximum embedded payload fraction.")
            raise

        if self.maximum_embedded_payload_fraction != MAXIMUM_EMBEDDED_PAYLOAD_FRACTION:
            log_message = "Maximum embedded payload fraction: {} is not expected the expected value of: {}."
            log_message = log_message.format(self.maximum_embedded_payload_fraction, MAXIMUM_EMBEDDED_PAYLOAD_FRACTION)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        try:

            self.minimum_embedded_payload_fraction = ord(database_header_byte_array[22:23])

        except TypeError:

            logger.error("Failed to retrieve the minimum embedded payload fraction.")
            raise

        if self.minimum_embedded_payload_fraction != MINIMUM_EMBEDDED_PAYLOAD_FRACTION:
            log_message = "Minimum embedded payload fraction: {} is not expected the expected value of: {}."
            log_message = log_message.format(self.minimum_embedded_payload_fraction, MINIMUM_EMBEDDED_PAYLOAD_FRACTION)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        try:

            self.leaf_payload_fraction = ord(database_header_byte_array[23:24])

        except TypeError:

            logger.error("Failed to retrieve the leaf payload fraction.")
            raise

        if self.leaf_payload_fraction != LEAF_PAYLOAD_FRACTION:
            log_message = "Leaf payload fraction: {} is not expected the expected value of: {}."
            log_message = log_message.format(self.leaf_payload_fraction, LEAF_PAYLOAD_FRACTION)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        self.file_change_counter = unpack(b">I", database_header_byte_array[24:28])[0]
        self.database_size_in_pages = unpack(b">I", database_header_byte_array[28:32])[0]
        self.first_freelist_trunk_page_number = unpack(b">I", database_header_byte_array[32:36])[0]
        self.number_of_freelist_pages = unpack(b">I", database_header_byte_array[36:40])[0]
        self.schema_cookie = unpack(b">I", database_header_byte_array[40:44])[0]
        self.schema_format_number = unpack(b">I", database_header_byte_array[44:48])[0]
        self.default_page_cache_size = unpack(b">I", database_header_byte_array[48:52])[0]
        self.largest_root_b_tree_page_number = unpack(b">I", database_header_byte_array[52:56])[0]
        self.database_text_encoding = unpack(b">I", database_header_byte_array[56:60])[0]

        if self.schema_format_number == 0 and self.database_text_encoding == 0:

            """

            Note:  If the schema format number and database text encoding are both 0 then no schema or data has been
                   placed into this database file.  If a schema or any data was inputted and then all tables dropped,
                   the schema format number and database text encoding would then be set.  In this case the database
                   should only be 1 page.  However, we have no way to determine what the size of the database page is
                   unless the version is at least 3.7.0.  We could check on the SQLite version and make sure the
                   version is at least 3.7.0 and then check the database size in pages to make sure it was 1 but we
                   would have no way to handle the case if the version was not at least 3.7.0.  Also, it has been
                   noticed that the SQLite version number is 0 in some database files.  Until this is further
                   thought out and possible solutions are determined, we will not worry about checking that
                   the database has 1 page.

            """

            log_message = "Schema format number and database text encoding are 0 indicating no schema or data."
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        else:

            if self.schema_format_number not in VALID_SCHEMA_FORMATS:
                log_message = "Schema format number: {} not a valid schema format.".format(self.schema_format_number)
                logger.error(log_message)
                raise HeaderParsingError(log_message)

            if self.database_text_encoding not in DATABASE_TEXT_ENCODINGS:
                log_message = "Database text encoding: {} not a valid encoding.".format(self.database_text_encoding)
                logger.error(log_message)
                raise HeaderParsingError(log_message)

        self.user_version = unpack(b">I", database_header_byte_array[60:64])[0]
        self.incremental_vacuum_mode = unpack(b">I", database_header_byte_array[64:68])[0]

        """

        Originally a check was done that if the largest root b-tree page number existed and the database was less
        than or equal to 2 pages in size, an exception was thrown.  This was found to be wrong in the case of where
        a database file was generated initially with one page with no information in it yet.  In this case (where
        auto-vacuuming was turned on resulting in a non-zero largest root b-tree page number) the largest root
        b tree page number was found to be 1.  Therefore no exception is thrown if the database size in pages is 1
        as well as the largest root b-tree page number.  However, this resulted in the check of the largest root
        b-tree page number == 2 as well as the database size in pages == 2.  This was decided an irrelevant use case
        and removed.

        Now the only thing that is checked is that if the incremental vacuum mode is set than the database header
        largest root b-tree page number must be set.  (The inverse of this is not true.)

        Note:  In regards to the above, the checking of the page size was done by the database size in pages calculated
               from the actual parsing of the SQLite file and did not originally reside in this class.  After that
               specific use case was removed, there was no reason not to move this to the database header class.

        """

        if not self.largest_root_b_tree_page_number and self.incremental_vacuum_mode:
            log_message = "The database header largest root b-tree page number was not set when the incremental " \
                          "vacuum mode was: {}."
            log_message = log_message.format(self.incremental_vacuum_mode)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        self.application_id = unpack(b">I", database_header_byte_array[68:72])[0]
        self.reserved_for_expansion = database_header_byte_array[72:92]

        pattern = compile(RESERVED_FOR_EXPANSION_REGEX)
        reserved_for_expansion_hex = hexlify(self.reserved_for_expansion)
        if not pattern.match(reserved_for_expansion_hex):
            log_message = "Header space reserved for expansion is not zero: {}.".format(reserved_for_expansion_hex)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        self.version_valid_for_number = unpack(b">I", database_header_byte_array[92:96])[0]
        self.sqlite_version_number = unpack(b">I", database_header_byte_array[96:100])[0]

        self.md5_hex_digest = get_md5_hash(database_header_byte_array)

    def stringify(self, padding=""):
        string = padding + "Magic Header String: {}\n" \
                 + padding + "Page Size: {}\n" \
                 + padding + "File Format Write Version: {}\n" \
                 + padding + "File Format Read Version: {}\n" \
                 + padding + "Reserved Bytes per Page: {}\n" \
                 + padding + "Maximum Embedded Payload Fraction: {}\n" \
                 + padding + "Minimum Embedded Payload Fraction: {}\n" \
                 + padding + "Leaf Payload Fraction: {}\n" \
                 + padding + "File Change Counter: {}\n" \
                 + padding + "Database Size in Pages: {}\n" \
                 + padding + "First Freelist Trunk Page Number: {}\n" \
                 + padding + "Number of Freelist Pages: {}\n" \
                 + padding + "Schema Cookie: {}\n" \
                 + padding + "Schema Format Number: {}\n" \
                 + padding + "Default Page Cache Size: {}\n" \
                 + padding + "Largest Root B-Tree Page Number: {}\n" \
                 + padding + "Database Text Encoding: {}\n" \
                 + padding + "User Version: {}\n" \
                 + padding + "Incremental Vacuum Mode: {}\n" \
                 + padding + "Application ID: {}\n" \
                 + padding + "Reserved for Expansion (Hex): {}\n" \
                 + padding + "Version Valid for Number: {}\n" \
                 + padding + "SQLite Version Number: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.magic_header_string,
                             self.page_size,
                             HUMAN_READABLE_JOURNALING_MODES[self.file_format_write_version],
                             HUMAN_READABLE_JOURNALING_MODES[self.file_format_read_version],
                             self.reserved_bytes_per_page,
                             self.maximum_embedded_payload_fraction,
                             self.minimum_embedded_payload_fraction,
                             self.leaf_payload_fraction,
                             self.file_change_counter,
                             self.database_size_in_pages,
                             self.first_freelist_trunk_page_number,
                             self.number_of_freelist_pages,
                             self.schema_cookie,
                             self.schema_format_number,
                             self.default_page_cache_size,
                             self.largest_root_b_tree_page_number,
                             HUMAN_READABLE_DATABASE_TEXT_ENCODINGS[self.database_text_encoding],
                             self.user_version,
                             self.incremental_vacuum_mode,
                             self.application_id,
                             hexlify(self.reserved_for_expansion),
                             self.version_valid_for_number,
                             self.sqlite_version_number,
                             self.md5_hex_digest)


class BTreePageHeader(object):

    __metaclass__ = ABCMeta

    def __init__(self, page, header_length):

        self.offset = 0
        self.header_length = header_length

        self.contains_sqlite_database_header = False

        """

        The root_page_only_md5_hex_digest is only set when the SQLite database header is detected in the page.

        """

        self.root_page_only_md5_hex_digest = None

        first_page_byte = page[0:1]
        if first_page_byte == MASTER_PAGE_HEX_ID:
            self.contains_sqlite_database_header = True
            self.root_page_only_md5_hex_digest = get_md5_hash(page[SQLITE_DATABASE_HEADER_LENGTH:])
            self.offset += SQLITE_DATABASE_HEADER_LENGTH

        self.page_type = page[self.offset:self.offset + 1]
        self.first_freeblock_offset = unpack(b">H", page[self.offset + 1:self.offset + 3])[0]
        self.number_of_cells_on_page = unpack(b">H", page[self.offset + 3:self.offset + 5])[0]
        self.cell_content_offset = unpack(b">H", page[self.offset + 5:self.offset + 7])[0]
        self.number_of_fragmented_free_bytes = ord(page[self.offset + 7:self.offset + 8])

        self.md5_hex_digest = get_md5_hash(page[self.offset:self.header_length])

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Contains SQLite Database Header: {}\n" \
                 + padding + "Root Page Only MD5 Hex Digest: {}\n" \
                 + padding + "Page Type (Hex): {}\n" \
                 + padding + "Offset: {}\n" \
                 + padding + "Length: {}\n" \
                 + padding + "First Freeblock Offset: {}\n" \
                 + padding + "Number of Cells on Page: {}\n" \
                 + padding + "Cell Content Offset: {}\n" \
                 + padding + "Number of Fragmented Free Bytes: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.contains_sqlite_database_header,
                             self.root_page_only_md5_hex_digest,
                             hexlify(self.page_type),
                             self.offset,
                             self.header_length,
                             self.first_freeblock_offset,
                             self.number_of_cells_on_page,
                             self.cell_content_offset,
                             self.number_of_fragmented_free_bytes,
                             self.md5_hex_digest)


class LeafPageHeader(BTreePageHeader):

    def __init__(self, page):
        super(LeafPageHeader, self).__init__(page, LEAF_PAGE_HEADER_LENGTH)


class InteriorPageHeader(BTreePageHeader):

    def __init__(self, page):
        super(InteriorPageHeader, self).__init__(page, INTERIOR_PAGE_HEADER_LENGTH)

        right_most_pointer_start_offset = self.offset + RIGHT_MOST_POINTER_OFFSET
        right_most_pointer_end_offset = right_most_pointer_start_offset + RIGHT_MOST_POINTER_LENGTH
        self.right_most_pointer = unpack(b">I", page[right_most_pointer_start_offset:right_most_pointer_end_offset])[0]

    def stringify(self, padding=""):
        string = "\n" \
                 + padding + "Right Most Pointer: {}"
        string = string.format(self.right_most_pointer)
        return super(InteriorPageHeader, self).stringify(padding) + string
