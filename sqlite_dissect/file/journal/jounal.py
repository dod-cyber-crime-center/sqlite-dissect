from re import sub
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.file.file_handle import FileHandle

"""

journal.py

This script holds the class to parse the rollback journal file.

This script holds the following object(s):
RollbackJournal(object)

"""


class RollbackJournal(object):

    def __init__(self, file_identifier, file_size=None):

        self.file_handle = FileHandle(FILE_TYPE.ROLLBACK_JOURNAL, file_identifier, file_size=file_size)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "File Handle:\n{}"
        string = string.format(self.file_handle.stringify(padding + "\t"))
        return string
