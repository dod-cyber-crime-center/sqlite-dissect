from abc import ABCMeta
from abc import abstractmethod
from logging import getLogger
from re import sub
from sqlite_dissect.constants import LOGGER_NAME

"""

header.py

This script holds an abstract class for file header objects to extend and inherit from.  File headers such as that
of the wal, journal, and database file headers will extend this class.

Note:  The database file header is the same as the file header for the sqlite database.  However, for cases like the wal
       file, the file has a file header that is not related to the actual database information and then depending on how
       many commits were done with the first page in them, could have many database headers.

This script holds the following object(s):
SQLiteHeader(object)

"""


class SQLiteHeader(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.page_size = None
        self.md5_hex_digest = None

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    @abstractmethod
    def stringify(self, padding=""):
        log_message = "The abstract method stringify was called directly and is not implemented."
        getLogger(LOGGER_NAME).error(log_message)
        raise NotImplementedError(log_message)
