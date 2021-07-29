from binascii import hexlify
from logging import getLogger
from struct import unpack
from re import sub
from warnings import warn
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import ROLLBACK_JOURNAL_ALL_CONTENT_UNTIL_END_OF_FILE
from sqlite_dissect.constants import ROLLBACK_JOURNAL_HEADER_ALL_CONTENT
from sqlite_dissect.constants import ROLLBACK_JOURNAL_HEADER_HEX_STRING
from sqlite_dissect.constants import ROLLBACK_JOURNAL_HEADER_LENGTH
from sqlite_dissect.utilities import get_md5_hash
from sqlite_dissect.file.header import SQLiteHeader

"""

header.py

This script holds the header objects for the rollback journal file and page record.

This script holds the following object(s):
RollbackJournalHeader(SQLiteHeader)
RollbackJournalPageRecordHeader(object)

"""


class RollbackJournalHeader(SQLiteHeader):

    def __init__(self, rollback_journal_header_byte_array):

        super(RollbackJournalHeader, self).__init__()

        logger = getLogger(LOGGER_NAME)

        if len(rollback_journal_header_byte_array) != ROLLBACK_JOURNAL_HEADER_LENGTH:
            log_message = "The rollback journal header byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(rollback_journal_header_byte_array), ROLLBACK_JOURNAL_HEADER_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        self.header_string = rollback_journal_header_byte_array[0:8]

        if self.header_string != ROLLBACK_JOURNAL_HEADER_HEX_STRING.decode("hex"):

            """

            Instead of throwing an error here, a warning is thrown instead.  This is due to the fact that the header
            string was found in a few files that did not match the appropriate rollback journal header string.
            Additional research needs to be done into what use cases this could lead to and if these are valid use
            cases or not. 

            """

            log_message = "The header string is invalid."
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        self.page_count = unpack(b">I", rollback_journal_header_byte_array[8:12])[0]

        if rollback_journal_header_byte_array[8:12] == ROLLBACK_JOURNAL_HEADER_ALL_CONTENT.decode("hex"):
            self.page_count = ROLLBACK_JOURNAL_ALL_CONTENT_UNTIL_END_OF_FILE

        self.random_nonce_for_checksum = unpack(b">I", rollback_journal_header_byte_array[12:16])[0]
        self.initial_size_of_database_in_pages = unpack(b">I", rollback_journal_header_byte_array[16:20])[0]
        self.disk_sector_size = unpack(b">I", rollback_journal_header_byte_array[20:24])[0]
        self.size_of_pages_in_journal = unpack(b">I", rollback_journal_header_byte_array[24:28])[0]

        # The page size will be the same size as the "size of pages in journal" attribute of the header.
        self.page_size = self.size_of_pages_in_journal

        self.md5_hex_digest = get_md5_hash(rollback_journal_header_byte_array)

    def stringify(self, padding=""):
        string = padding + "Header String (Hex): {}\n" \
                 + padding + "Page Count: {}\n" \
                 + padding + "Random Nonce for Checksum: {}\n" \
                 + padding + "Initial Size of Database in Pages: {}\n" \
                 + padding + "Disk Sector Size: {}\n" \
                 + padding + "Size of Pages in Journal: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(hexlify(self.header_string), self.page_count, self.random_nonce_for_checksum,
                             self.initial_size_of_database_in_pages, self.disk_sector_size,
                             self.size_of_pages_in_journal, self.md5_hex_digest)


class RollbackJournalPageRecordHeader(object):

    def __init__(self):
        pass

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        pass
