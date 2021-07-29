from logging import getLogger
from re import sub
from struct import unpack
from warnings import warn
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import WAL_FILE_FORMAT_VERSION
from sqlite_dissect.constants import WAL_FRAME_HEADER_LENGTH
from sqlite_dissect.constants import WAL_HEADER_LENGTH
from sqlite_dissect.constants import WAL_MAGIC_NUMBER_BIG_ENDIAN
from sqlite_dissect.constants import WAL_MAGIC_NUMBER_LITTLE_ENDIAN
from sqlite_dissect.exception import HeaderParsingError
from sqlite_dissect.file.header import SQLiteHeader
from sqlite_dissect.utilities import get_md5_hash

"""

header.py

This script holds the header objects used for parsing the header of the WAL file and WAL frames.

This script holds the following object(s):
WriteAheadLogHeader(SQLiteHeader)
WriteAheadLogFrameHeader(object)

"""


class WriteAheadLogHeader(SQLiteHeader):

    def __init__(self, wal_header_byte_array):

        super(WriteAheadLogHeader, self).__init__()

        logger = getLogger(LOGGER_NAME)

        if len(wal_header_byte_array) != WAL_HEADER_LENGTH:
            log_message = "The wal header byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(wal_header_byte_array), WAL_HEADER_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        self.magic_number = unpack(b">I", wal_header_byte_array[0:4])[0]

        """

        Note: The magic number specifies either big endian or little endian encoding for checksums.

        """

        if self.magic_number not in [WAL_MAGIC_NUMBER_BIG_ENDIAN, WAL_MAGIC_NUMBER_LITTLE_ENDIAN]:
            log_message = "The magic number: {} is valid.".format(self.magic_number)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        self.file_format_version = unpack(b">I", wal_header_byte_array[4:8])[0]

        if self.file_format_version != WAL_FILE_FORMAT_VERSION:
            log_message = "An unsupported file format version was found: {} instead of the expected value: {}."
            log_message = log_message.format(self.file_format_version, WAL_FILE_FORMAT_VERSION)
            logger.error(log_message)
            raise HeaderParsingError(log_message)

        self.page_size = unpack(b">I", wal_header_byte_array[8:12])[0]
        self.checkpoint_sequence_number = unpack(b">I", wal_header_byte_array[12:16])[0]

        if self.checkpoint_sequence_number != 0:
            log_message = "Checkpoint sequence number is {} instead of 0 and may cause inconsistencies in wal parsing."
            log_message = log_message.format(self.checkpoint_sequence_number)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        self.salt_1 = unpack(b">I", wal_header_byte_array[16:20])[0]
        self.salt_2 = unpack(b">I", wal_header_byte_array[20:24])[0]
        self.checksum_1 = unpack(b">I", wal_header_byte_array[24:28])[0]
        self.checksum_2 = unpack(b">I", wal_header_byte_array[28:32])[0]

        self.md5_hex_digest = get_md5_hash(wal_header_byte_array)

    def stringify(self, padding=""):
        string = padding + "Magic Number: {}\n" \
                 + padding + "File Format Version: {}\n" \
                 + padding + "Page Size: {}\n" \
                 + padding + "Checkpoint Sequence Number: {}\n" \
                 + padding + "Salt 1: {}\n" \
                 + padding + "Salt 2: {}\n" \
                 + padding + "Checksum 1: {}\n" \
                 + padding + "Checksum 2: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.magic_number,
                             self.file_format_version,
                             self.page_size,
                             self.checkpoint_sequence_number,
                             self.salt_1,
                             self.salt_2,
                             self.checksum_1,
                             self.checksum_2,
                             self.md5_hex_digest)


class WriteAheadLogFrameHeader(object):

    def __init__(self, wal_frame_header_byte_array):

        logger = getLogger(LOGGER_NAME)

        if len(wal_frame_header_byte_array) != WAL_FRAME_HEADER_LENGTH:
            log_message = "The wal frame header byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(wal_frame_header_byte_array), WAL_FRAME_HEADER_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        self.page_number = unpack(b">I", wal_frame_header_byte_array[0:4])[0]
        self.page_size_after_commit = unpack(b">I", wal_frame_header_byte_array[4:8])[0]
        self.salt_1 = unpack(b">I", wal_frame_header_byte_array[8:12])[0]
        self.salt_2 = unpack(b">I", wal_frame_header_byte_array[12:16])[0]
        self.checksum_1 = unpack(b">I", wal_frame_header_byte_array[16:20])[0]
        self.checksum_2 = unpack(b">I", wal_frame_header_byte_array[20:24])[0]

        self.md5_hex_digest = get_md5_hash(wal_frame_header_byte_array)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Page Number: {}\n" \
                 + padding + "Page Size After Commit: {}\n" \
                 + padding + "Salt 1: {}\n" \
                 + padding + "Salt 2: {}\n" \
                 + padding + "Checksum 1: {}\n" \
                 + padding + "Checksum 2: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        return string.format(self.page_number,
                             self.page_size_after_commit,
                             self.salt_1,
                             self.salt_2,
                             self.checksum_1,
                             self.checksum_2,
                             self.md5_hex_digest)
