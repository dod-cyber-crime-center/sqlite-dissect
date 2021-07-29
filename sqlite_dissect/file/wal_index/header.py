from binascii import hexlify
from logging import getLogger
from re import sub
from struct import unpack
from sqlite_dissect.constants import ENDIANNESS
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import WAL_INDEX_CHECKPOINT_INFO_LENGTH
from sqlite_dissect.constants import WAL_INDEX_FILE_FORMAT_VERSION
from sqlite_dissect.constants import WAL_INDEX_HEADER_LENGTH
from sqlite_dissect.constants import WAL_INDEX_LOCK_RESERVED_LENGTH
from sqlite_dissect.constants import WAL_INDEX_NUMBER_OF_SUB_HEADERS
from sqlite_dissect.constants import WAL_INDEX_NUMBER_OF_FRAMES_BACKFILLED_IN_DATABASE_LENGTH
from sqlite_dissect.constants import WAL_INDEX_READER_MARK_LENGTH
from sqlite_dissect.constants import WAL_INDEX_READER_MARK_SIZE
from sqlite_dissect.constants import WAL_INDEX_SUB_HEADER_LENGTH
from sqlite_dissect.exception import HeaderParsingError
from sqlite_dissect.file.header import SQLiteHeader
from sqlite_dissect.utilities import get_md5_hash

"""

header.py

This script holds the header objects used for parsing the header of the wal index file.

This script holds the following object(s):
WriteAheadLogIndexHeader(SQLiteHeader)
WriteAheadLogIndexSubHeader(SQLiteHeader)
WriteAheadLogIndexCheckpointInfo(object)

"""


class WriteAheadLogIndexHeader(SQLiteHeader):

    def __init__(self, wal_index_header_byte_array):

        super(WriteAheadLogIndexHeader, self).__init__()

        logger = getLogger(LOGGER_NAME)

        if len(wal_index_header_byte_array) != WAL_INDEX_HEADER_LENGTH:
            log_message = "The wal index header byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(wal_index_header_byte_array), WAL_INDEX_HEADER_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        """

        Note:  The sub header will always be followed by an exact copy of itself in the WAL index file header.
               Therefore, there will always be two (WAL_INDEX_NUMBER_OF_SUB_HEADERS) headers.  Instead of having two
               separate sub header variables, it was decided to do an array for the two since it is similarly
               implemented like this in the sqlite c code.

        """

        self.sub_headers = []

        for sub_header_index in range(WAL_INDEX_NUMBER_OF_SUB_HEADERS):
            start_offset = sub_header_index * WAL_INDEX_SUB_HEADER_LENGTH
            end_offset = start_offset + WAL_INDEX_SUB_HEADER_LENGTH
            self.sub_headers.append(WriteAheadLogIndexSubHeader(sub_header_index,
                                                                wal_index_header_byte_array[start_offset:end_offset]))

        """

        Note:  Since both of the sub headers are the same, they should each have the same endianness as well as page
               size and therefore it does not matter from which one we retrieve it from.

        """

        # Set variables for this class for page size and endianness
        self.page_size = self.sub_headers[0].page_size
        self.endianness = self.sub_headers[0].endianness

        checkpoint_start_offset = WAL_INDEX_NUMBER_OF_SUB_HEADERS * WAL_INDEX_SUB_HEADER_LENGTH
        checkpoint_end_offset = checkpoint_start_offset + WAL_INDEX_CHECKPOINT_INFO_LENGTH
        wal_index_checkpoint_info_byte_array = wal_index_header_byte_array[checkpoint_start_offset:
                                                                           checkpoint_end_offset]
        self.checkpoint_info = WriteAheadLogIndexCheckpointInfo(wal_index_checkpoint_info_byte_array, self.endianness)

        lock_reserved_start_offset = checkpoint_start_offset + WAL_INDEX_CHECKPOINT_INFO_LENGTH
        lock_reserved_end_offset = lock_reserved_start_offset + WAL_INDEX_LOCK_RESERVED_LENGTH
        self.lock_reserved = wal_index_header_byte_array[lock_reserved_start_offset:lock_reserved_end_offset]

        self.md5_hex_digest = get_md5_hash(wal_index_header_byte_array)

    def stringify(self, padding=""):
        string = padding + "Page Size: {}\n" \
                 + padding + "MD5 Hex Digest: {}"
        string = string.format(self.page_size,
                               self.md5_hex_digest)
        for sub_header_index in range(len(self.sub_headers)):
            string += "\n" + padding + "Sub Header:\n{}"
            string = string.format(self.sub_headers[sub_header_index].stringify(padding + "\t"))
        string += "\n" + padding + "Checkpoint Info:\n{}".format(self.checkpoint_info.stringify(padding + "\t"))
        string += "\n" + padding + "Lock Reserved (Hex): {}".format(hexlify(self.lock_reserved))
        return string


class WriteAheadLogIndexSubHeader(SQLiteHeader):

    def __init__(self, index, wal_index_sub_header_byte_array):

        super(WriteAheadLogIndexSubHeader, self).__init__()

        logger = getLogger(LOGGER_NAME)

        if index < 0 or index > WAL_INDEX_NUMBER_OF_SUB_HEADERS:
            log_message = "Invalid wal index sub header index: {}.".format(index)
            logger.error(log_message)
            raise ValueError(log_message)

        self.index = index

        if len(wal_index_sub_header_byte_array) != WAL_INDEX_SUB_HEADER_LENGTH:
            log_message = "The wal index sub header byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(wal_index_sub_header_byte_array), WAL_INDEX_SUB_HEADER_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        self.endianness = ENDIANNESS.LITTLE_ENDIAN

        # Retrieve the file format version in little endian
        self.file_format_version = unpack(b"<I", wal_index_sub_header_byte_array[0:4])[0]

        """

        Note:  Little endian was noticed to be used more than big endian and that is the only reason why we first
               choose to check against little endian.

        """

        if self.file_format_version != WAL_INDEX_FILE_FORMAT_VERSION:

            # Retrieve the file format version in big endian
            self.file_format_version = unpack(b">I", wal_index_sub_header_byte_array[0:4])[0]

            if self.file_format_version != WAL_INDEX_FILE_FORMAT_VERSION:

                log_message = "The file format version is invalid"
                logger.error(log_message)
                raise HeaderParsingError(log_message)

            else:

                self.endianness = ENDIANNESS.BIG_ENDIAN

                log_message = "The wal index file is in big endian which is currently not supported."
                logger.error(log_message)
                raise NotImplementedError(log_message)

        self.unused_padding_field = unpack(b"<I", wal_index_sub_header_byte_array[4:8])[0]
        self.change_counter = unpack(b"<I", wal_index_sub_header_byte_array[8:12])[0]
        self.initialized = ord(wal_index_sub_header_byte_array[12:13])
        self.checksums_in_big_endian = ord(wal_index_sub_header_byte_array[13:14])
        self.page_size = unpack(b"<H", wal_index_sub_header_byte_array[14:16])[0]
        self.last_valid_frame_index = unpack(b"<I", wal_index_sub_header_byte_array[16:20])[0]
        self.database_size_in_pages = unpack(b"<I", wal_index_sub_header_byte_array[20:24])[0]
        self.frame_checksum_1 = unpack(b"<I", wal_index_sub_header_byte_array[24:28])[0]
        self.frame_checksum_2 = unpack(b"<I", wal_index_sub_header_byte_array[28:32])[0]
        self.salt_1 = unpack(b"<I", wal_index_sub_header_byte_array[32:36])[0]
        self.salt_2 = unpack(b"<I", wal_index_sub_header_byte_array[36:40])[0]
        self.checksum_1 = unpack(b"<I", wal_index_sub_header_byte_array[40:44])[0]
        self.checksum_2 = unpack(b"<I", wal_index_sub_header_byte_array[44:48])[0]

        self.md5_hex_digest = get_md5_hash(wal_index_sub_header_byte_array)

    def stringify(self, padding=""):
        string = padding + "Index: {}\n" \
                 + padding + "File Format Version: {}\n" \
                 + padding + "Unused Padding Field: {}\n" \
                 + padding + "Change Counter: {}\n" \
                 + padding + "Initialized: {}\n" \
                 + padding + "Checksums in Big Endian: {}\n" \
                 + padding + "Database Page Size: {}\n" \
                 + padding + "Last Valid Frame Index: {}\n" \
                 + padding + "Database Size in Pages: {}\n" \
                 + padding + "Frame Checksum 1: {}\n" \
                 + padding + "Frame Checksum 2: {}\n" \
                 + padding + "Salt 1: {}\n" \
                 + padding + "Salt 2: {}\n" \
                 + padding + "Checksum 1: {}\n" \
                 + padding + "Checksum 2: {}\n" \
                 + padding + "MD5 Hex Digest {}"
        return string.format(self.index,
                             self.file_format_version,
                             self.unused_padding_field,
                             self.change_counter,
                             self.initialized,
                             self.checksums_in_big_endian,
                             self.page_size,
                             self.last_valid_frame_index,
                             self.database_size_in_pages,
                             self.frame_checksum_1,
                             self.frame_checksum_2,
                             self.salt_1,
                             self.salt_2,
                             self.checksum_1,
                             self.checksum_2,
                             self.md5_hex_digest)


class WriteAheadLogIndexCheckpointInfo(object):

    def __init__(self, wal_index_checkpoint_info_byte_array, endianness):

        logger = getLogger(LOGGER_NAME)

        self.endianness = endianness

        if len(wal_index_checkpoint_info_byte_array) != WAL_INDEX_CHECKPOINT_INFO_LENGTH:
            log_message = "The wal index checkpoint info byte array of size: {} is not the expected size of: {}."
            log_message = log_message.format(len(wal_index_checkpoint_info_byte_array),
                                             WAL_INDEX_CHECKPOINT_INFO_LENGTH)
            logger.error(log_message)
            raise ValueError(log_message)

        self.number_of_frames_backfilled_in_database = unpack(b"<I", wal_index_checkpoint_info_byte_array[0:4])[0]

        """

        Note:  The reader marks will always be an array of 5 reader marks.

        """

        self.reader_marks = []

        for index in range(WAL_INDEX_READER_MARK_SIZE):
            start_offset = index * WAL_INDEX_READER_MARK_LENGTH
            start_offset += WAL_INDEX_NUMBER_OF_FRAMES_BACKFILLED_IN_DATABASE_LENGTH
            end_offset = start_offset + WAL_INDEX_READER_MARK_LENGTH
            self.reader_marks.append(unpack(b"<I", wal_index_checkpoint_info_byte_array[start_offset:end_offset])[0])

        self.md5_hex_digest = get_md5_hash(wal_index_checkpoint_info_byte_array)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Endianness: {}\n" \
                 + padding + "Number of Frames Backfilled in Database: {}"
        string = string.format(self.endianness,
                               self.number_of_frames_backfilled_in_database)
        for reader_mark_index in range(len(self.reader_marks)):
            string += "\n" + padding + "Reader Mark {}: {}".format(reader_mark_index + 1,
                                                                   self.reader_marks[reader_mark_index])
        string += "\n" + padding + "MD5 Hex Digest: {}".format(self.md5_hex_digest)
        return string
