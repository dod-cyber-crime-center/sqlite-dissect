import os
from logging import getLogger
from re import sub
from warnings import warn
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import LOCK_BYTE_PAGE_START_OFFSET
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import ROLLBACK_JOURNAL_HEADER_LENGTH
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from sqlite_dissect.constants import UTF_8
from sqlite_dissect.constants import UTF_8_DATABASE_TEXT_ENCODING
from sqlite_dissect.constants import UTF_16BE
from sqlite_dissect.constants import UTF_16BE_DATABASE_TEXT_ENCODING
from sqlite_dissect.constants import UTF_16LE
from sqlite_dissect.constants import UTF_16LE_DATABASE_TEXT_ENCODING
from sqlite_dissect.constants import WAL_HEADER_LENGTH
from sqlite_dissect.constants import WAL_INDEX_HEADER_LENGTH
from sqlite_dissect.file.database.header import DatabaseHeader
from sqlite_dissect.file.journal.header import RollbackJournalHeader
from sqlite_dissect.file.wal.header import WriteAheadLogHeader
from sqlite_dissect.file.wal_index.header import WriteAheadLogIndexHeader

"""

file_handle.py

This script holds the file handle for file objects to be worked with in relation to the database, wal, journal and other
supported file types specified in the FILE_TYPE file types list.

This script holds the following object(s):
FileHandle(object)

"""


class FileHandle(object):

    def __init__(self, file_type, file_identifier, database_text_encoding=None, file_size=None):

        """

        Constructor.  This constructor initializes this object.

        Note:  Either the file name or the file object needs to be specified as the file_identifier.  The file name
               is derived from the file object in order to derive the file size of the object by calling getsize on
               the file name as well as for informational and logging purposes.

        :param file_type: str  The type of the file.  Must be one of the file types in the FILE_TYPE list.
        :param file_identifier: str or file  The full file path to the file to be opened or the file object.
        :param database_text_encoding: str  The encoding of the text strings in the sqlite database file.
        :param file_size: int  Optional parameter to supply the file size.

        :raise: IOError  If the file_name is specified and upon opening the file:
                         1.) the file name specifies a file that does not exist, or
                         2.) the file name specified a file that is not a file, or
                         3.) the file name is unable to be opened in "rb" mode.
        :raise: ValueError  If:
                            1.) both the file name and file are set, or
                            2.) neither the file name or file are set, or
                            3.) the file type is not a valid file type.

        """

        self._logger = getLogger(LOGGER_NAME)

        self.file_type = file_type
        self.file_object = None
        self.file_externally_controlled = False
        self._database_text_encoding = database_text_encoding

        if isinstance(file_identifier, basestring):

            """

            Note: The file identifier is the name (full path) of the file if it is an instance of basestring.  We check
                  to make sure the file exists and it is actually a file.

            """

            if not os.path.exists(file_identifier):
                log_message = "The file name specified does not exist: {}".format(file_identifier)
                self._logger.error(log_message)
                raise IOError(log_message)

            if not os.path.isfile(file_identifier):
                log_message = "The file name specified is not a file: {}".format(file_identifier)
                self._logger.error(log_message)
                raise IOError(log_message)

            try:
                self.file_object = open(file_identifier, "rb")
            except IOError:
                log_message = "Unable to open the file in \"rb\" mode with file name: {}.".format(file_identifier)
                self._logger.error(log_message)
                raise

        else:
            self.file_object = file_identifier
            self.file_externally_controlled = True

        if file_size:
            self.file_size = file_size
        else:
            try:
                self.file_size = os.fstat(self.file_object.fileno()).st_size
            except AttributeError:
                # If all else fails, use the seek to the end of the file trick.
                self.file_object.seek(0, os.SEEK_END)
                self.file_size = self.file_object.tell()
                self.file_object.seek(0)

        if self.file_type == FILE_TYPE.DATABASE:

            if self.file_size > LOCK_BYTE_PAGE_START_OFFSET:
                log_message = "The file size: {} is >= lock byte offset: {} and the lock byte page is not supported."
                self._logger.error(log_message)
                raise NotImplementedError(log_message)

            try:

                database_header = DatabaseHeader(self.file_object.read(SQLITE_DATABASE_HEADER_LENGTH))

                if self._database_text_encoding:
                    log_message = "Database text encoding specified as: {} when should not be set."
                    self._logger.error(log_message)
                    raise ValueError(log_message)

                if database_header.database_text_encoding == UTF_8_DATABASE_TEXT_ENCODING:
                    self._database_text_encoding = UTF_8
                elif database_header.database_text_encoding == UTF_16LE_DATABASE_TEXT_ENCODING:
                    self._database_text_encoding = UTF_16LE
                elif database_header.database_text_encoding == UTF_16BE_DATABASE_TEXT_ENCODING:
                    self._database_text_encoding = UTF_16BE
                elif database_header.database_text_encoding:
                    log_message = "The database text encoding: {} is not recognized as a valid database text encoding."
                    log_message = log_message.format(database_header.database_text_encoding)
                    self._logger.error(log_message)
                    raise RuntimeError(log_message)

                self.header = database_header

            except:
                log_message = "Failed to initialize the database header."
                self._logger.error(log_message)
                raise

        elif self.file_type == FILE_TYPE.WAL:

            try:
                self.header = WriteAheadLogHeader(self.file_object.read(WAL_HEADER_LENGTH))
            except:
                log_message = "Failed to initialize the write ahead log header."
                self._logger.error(log_message)
                raise

        elif self.file_type == FILE_TYPE.WAL_INDEX:

            try:
                self.header = WriteAheadLogIndexHeader(self.file_object.read(WAL_INDEX_HEADER_LENGTH))
            except:
                log_message = "Failed to initialize the write ahead log index header."
                self._logger.error(log_message)
                raise

        elif self.file_type == FILE_TYPE.ROLLBACK_JOURNAL:

            try:
                self.header = RollbackJournalHeader(self.file_object.read(ROLLBACK_JOURNAL_HEADER_LENGTH))
            except:
                log_message = "Failed to initialize the rollback journal header."
                self._logger.error(log_message)
                raise

        else:

            log_message = "Invalid file type specified: {}.".format(self.file_type)
            self._logger.error(log_message)
            raise ValueError(log_message)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_header=True):
        string = padding + "File Type: {}\n" \
                 + padding + "File Size: {}\n" \
                 + padding + "Database Text Encoding: {}"
        string = string.format(self.file_type,
                               self.file_size,
                               self.database_text_encoding)
        if print_header:
            string += "\n" + padding + "Header:\n{}".format(self.header.stringify(padding + "\t"))
        return string

    @property
    def database_text_encoding(self):
        return self._database_text_encoding

    @database_text_encoding.setter
    def database_text_encoding(self, database_text_encoding):

        if self._database_text_encoding and self._database_text_encoding != database_text_encoding:
            log_message = "Database text encoding is set to: {} and cannot be set differently to: {}.  " \
                          "Operation not permitted."
            log_message = log_message.format(self._database_text_encoding, database_text_encoding)
            self._logger.error(log_message)
            raise TypeError(log_message)

        if database_text_encoding not in [UTF_8, UTF_16LE, UTF_16BE]:
            log_message = "The database text encoding: {} is not recognized as a valid database text encoding."
            log_message = log_message.format(database_text_encoding)
            self._logger.error(log_message)
            raise ValueError(log_message)

        self._database_text_encoding = database_text_encoding

    def close(self):

        if self.file_externally_controlled:

            log_message = "Ignored request to close externally controlled file."
            self._logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        else:

            try:

                self.file_object.close()

            except IOError:

                log_message = "Unable to close the file object."
                self._logger.exception(log_message)
                raise

    def read_data(self, offset, number_of_bytes):

        if offset >= self.file_size:
            log_message = "Requested offset: {} is >= the file size: {}."
            log_message = log_message.format(offset, self.file_size)
            self._logger.error(log_message)
            raise EOFError(log_message)

        if offset + number_of_bytes > self.file_size:
            log_message = "Requested length of data: {} at offset {} to {} is > than the file size: {}."
            log_message = log_message.format(number_of_bytes, offset, number_of_bytes + offset, self.file_size)
            self._logger.error(log_message)
            raise EOFError(log_message)

        try:

            self.file_object.seek(offset)
            return self.file_object.read(number_of_bytes)

        except ValueError:
            log_message = "An error occurred while reading from the file at offset: {} for {} number of bytes."
            log_message = log_message.format(offset, number_of_bytes)
            self._logger.error(log_message)
            raise
