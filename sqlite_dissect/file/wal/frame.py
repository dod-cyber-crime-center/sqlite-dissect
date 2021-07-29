from binascii import hexlify
from logging import getLogger
from re import sub
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_PAGE_HEX_ID
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from sqlite_dissect.constants import WAL_FRAME_HEADER_LENGTH
from sqlite_dissect.constants import WAL_HEADER_LENGTH
from sqlite_dissect.exception import WalParsingError
from sqlite_dissect.file.wal.header import WriteAheadLogFrameHeader

"""

frame.py

This script holds the objects used for parsing the WAL frame.

Note:  The WriteAheadLogFrame class is not responsible for parsing the page data itself.  It is meant to give
       information on the WALv frame and offsets of the page data but in order to parse the page data, the set of all
       page changes to the commit record this frame belongs in is needed.  Therefore the commit record class
       (WriteAheadLogCommitRecord) will be responsible for parsing pages.

       There was some discussion about the page being stored back in the WriteAheadLogFrame once parsed but it was
       decided that this made little to no difference and should just be retrieved from the commit record.

       As a side note, there are some basic things parsed from the page such as the page type.  This is only for
       debugging and logging purposes.

This script holds the following object(s):
WriteAheadLogFrame(object)

"""


class WriteAheadLogFrame(object):

    def __init__(self, file_handle, frame_index, commit_record_number):

        logger = getLogger(LOGGER_NAME)

        if file_handle.file_type != FILE_TYPE.WAL:
            log_message = "The wal frame file handle file type is not {} as expected but is {} for frame index: {} " \
                          "commit record number: {}."
            log_message = log_message.format(FILE_TYPE.WAL, file_handle.file_type, frame_index, commit_record_number)
            logger.error(log_message)
            raise ValueError(log_message)

        self.frame_index = frame_index
        self.frame_number = self.frame_index + 1
        self.commit_record_number = commit_record_number

        self.offset = self._get_write_ahead_log_frame_offset(self.frame_index, file_handle.header.page_size)
        self.frame_size = WAL_FRAME_HEADER_LENGTH + file_handle.header.page_size

        wal_frame = file_handle.read_data(self.offset, self.frame_size)
        self.header = WriteAheadLogFrameHeader(wal_frame[:WAL_FRAME_HEADER_LENGTH])
        self.commit_frame = True if self.header.page_size_after_commit else False
        page_content = wal_frame[WAL_FRAME_HEADER_LENGTH:]

        if len(page_content) != file_handle.header.page_size:
            log_message = "Page content was found to be: {} when expected to be: {} as declared in the wal file " \
                          "header for frame index: {} commit record number: {}."
            log_message = log_message.format(len(page_content), file_handle.header.page_size,
                                             frame_index, commit_record_number)
            logger.error(log_message)
            raise WalParsingError(log_message)

        self.contains_sqlite_database_header = False
        self.page_hex_type = page_content[0:1]

        if self.page_hex_type == MASTER_PAGE_HEX_ID:
            self.page_hex_type = page_content[SQLITE_DATABASE_HEADER_LENGTH:SQLITE_DATABASE_HEADER_LENGTH + 1]
            self.contains_sqlite_database_header = True

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Frame Index: {}\n" \
                 + padding + "Frame Number: {}\n" \
                 + padding + "Commit Record Number: {}\n" \
                 + padding + "Offset: {}\n" \
                 + padding + "Frame Size: {}\n" \
                 + padding + "Commit Frame: {}\n" \
                 + padding + "Header:\n{}\n"\
                 + padding + "Contains SQLite Database Header: {}\n" \
                 + padding + "Page Hex Type (Hex): {}"
        string = string.format(self.frame_index,
                               self.frame_number,
                               self.commit_record_number,
                               self.offset,
                               self.frame_size,
                               self.commit_frame,
                               self.header.stringify(padding + "\t"),
                               self.contains_sqlite_database_header,
                               hexlify(self.page_hex_type))
        return string

    @staticmethod
    def _get_write_ahead_log_frame_offset(index, page_size):
        wal_frame_size = WAL_FRAME_HEADER_LENGTH + page_size
        return WAL_HEADER_LENGTH + index * wal_frame_size
