from logging import getLogger
from re import sub
from struct import unpack
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import WAL_INDEX_HEADER_LENGTH
from sqlite_dissect.file.file_handle import FileHandle

"""

wal_index.py

This script holds the class to parse the wal index file.

This script holds the following object(s):
WriteAheadLogIndex(object)

"""


class WriteAheadLogIndex(object):

    def __init__(self, file_name, file_size=None):

        logger = getLogger(LOGGER_NAME)

        self._file_handle = FileHandle(FILE_TYPE.WAL_INDEX, file_name, file_size=file_size)

        zero = False
        start = WAL_INDEX_HEADER_LENGTH
        while not zero:
            i = (start - WAL_INDEX_HEADER_LENGTH) / 4
            data = unpack(b"<I", self._file_handle.read_data(start, 4))[0]
            if data == 0:
                zero = True
            else:
                key = (data * 383) & 8191
                log_message = "Entry {} at offset: {} is page #{} with key of {}.".format(i, start, data, key)
                logger.debug(log_message)
                start += 4

        u16_offset = start
        number_found = 0
        while u16_offset < self._file_handle.file_size:
            i = (u16_offset - start) / 2
            data = unpack(b"<H", self._file_handle.read_data(u16_offset, 2))[0]
            if data != 0:
                number_found += 1
                log_message = "Number {}: {} at offset: {} with relative offset: {} index (/2): {} and N#: {}"
                log_message = log_message.format(number_found, i, u16_offset, u16_offset-16384,
                                                 (u16_offset-16384)/2, data)
                logger.debug(log_message)
            u16_offset += 2

        logger.debug("Number of entries found: {}.".format(number_found))

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "File Handle:\n{}"
        string = string.format(self._file_handle.stringify(padding + "\t"))
        return string
