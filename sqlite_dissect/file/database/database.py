from copy import copy
from warnings import warn
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import FIRST_FREELIST_TRUNK_PAGE_INDEX
from sqlite_dissect.constants import FIRST_FREELIST_TRUNK_PARENT_PAGE_NUMBER
from sqlite_dissect.constants import SQLITE_3_7_0_VERSION_NUMBER
from sqlite_dissect.constants import SQLITE_MASTER_SCHEMA_ROOT_PAGE
from sqlite_dissect.exception import DatabaseParsingError
from sqlite_dissect.file.database.page import FreelistTrunkPage
from sqlite_dissect.file.database.utilities import create_pointer_map_pages
from sqlite_dissect.file.file_handle import FileHandle
from sqlite_dissect.file.schema.master import MasterSchema
from sqlite_dissect.file.version import Version

"""

database.py

This script holds the objects used for parsing the database file.

This script holds the following object(s):
Database(Version)

"""


class Database(Version):

    def __init__(self, file_identifier, store_in_memory=False, file_size=None, strict_format_checking=True):

        """

        Constructor.  This constructor initializes this object.

        :param file_identifier: str  The full file path to the file to be opened or the file object.
        :param store_in_memory: boolean  Tells this class to store it's particular version information in memory or not.
        :param file_size: int  Optional parameter to supply the file size.
        :param strict_format_checking: boolean  Specifies if the application should exit if structural validations fail.

        """

        """

        Note:  We pass the file name and file object to the file handle and let that do any needed error checking
               for us.

        """

        database_file_handle = FileHandle(FILE_TYPE.DATABASE, file_identifier, file_size=file_size)
        super(Database, self).__init__(database_file_handle, BASE_VERSION_NUMBER,
                                       store_in_memory, strict_format_checking)

        """

        Retrieve the database header from the file handle.

        """

        self._database_header = self.file_handle.header

        """

        Make sure the database size in pages is not 0.  If this occurs, the version has to be prior to 3.7.0.  If the
        size is 0 and the version is < 3.7.0 we set the database size in pages to the calculated number of pages
        computed from the file size multiplied by the page size.  If the version is >= 3.7.0, we raise an exception.

        If the database size in pages is not 0, there is still a use case that could cause the page size to be
        incorrect.  This is when the version valid for number does not match the file change counter.  Versions before
        3.7.0 did not know to update the page size, but also did not know to update the version valid for number.  Only
        the change counter was updated.  Therefore, in the use case where a file could be made with a version >= 3.7.0
        where the database size in pages is set as well as the version valid for number but then closed down, opened
        with a SQLite driver version < 3.7.0 and modified, the version valid for number would not match the change
        counter resulting in what could possibly be a bad database size in pages.

        Note:  If the file is opened back up in a version >= 3.7.0 after being opened in a previous version, the
               database size in pages and version valid for number are set correctly again along with the file change
               counter on the first modification to the database.  It is important to note this was only tested using
               WAL mode and the base database file remained with the incorrect information until the WAL updated it
               either at a checkpoint or file closure.  Rollback journals are assumed to also update this but have not
               been observed as of yet.

        """

        # The database header size in pages is not set
        if self.database_header.database_size_in_pages == 0:

            log_message = "Database header for version: {} specifies a database size in pages of 0 for " \
                          "sqlite version: {}."
            log_message = log_message.format(self.version_number, self.database_header.sqlite_version_number)
            self._logger.info(log_message)

            if self.database_header.sqlite_version_number >= SQLITE_3_7_0_VERSION_NUMBER:
                log_message = "The database header database size in pages is 0 when the sqlite version: {} is " \
                              "greater or equal than 3.7.0 in version: {} and should be set."
                log_message = log_message.format(self.database_header.sqlite_version_number, self.version_number)
                self._logger.error(log_message)
                raise DatabaseParsingError(log_message)

            # Calculate the number of pages from the file size and page size
            self.database_size_in_pages = self.file_handle.file_size / self.page_size

        # The database header size in pages is set and the version valid for number does not equal the change counter
        elif self.database_header.version_valid_for_number != self.database_header.file_change_counter:

            """

            We now know that the database has been modified by a legacy version and the database size may not
            be correct.  We have to rely on calculating the page size here.

            """

            # Calculate the number of pages from the file size and page size
            self.database_size_in_pages = self.file_handle.file_size / self.page_size

            log_message = "Database header for version: {} specifies a database size in pages of {} but version " \
                          "valid for number: {} does not equal the file change counter: {} for sqlite " \
                          "version: {}.  Setting the database size in pages to the calculated page size of: {}."
            log_message = log_message.format(self.version_number, self.database_header.database_size_in_pages,
                                             self.database_header.version_valid_for_number,
                                             self.database_header.file_change_counter,
                                             self.database_header.sqlite_version_number,
                                             self.database_size_in_pages)
            self._logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        # The database header size in pages is set and the version valid for number does equals the change counter
        else:

            """

            Check to make sure the calculated size in pages matches the database header database size in pages as
            it should.

            Note:  The calculated number of pages can and has been found to be wrong in some cases where the database
                   size in pages is specified where the version valid for number equals the file change counter.  It is
                   still unsure of why this can occur but in the use cases this was seen, the database size in pages was
                   correct and the file was inflated (padded) with empty space at the end indicating additional pages
                   when calculating page size from file size.  For this reason a warning is thrown instead of an
                   exception (in the case that the version valid for number equals the file change counter and database
                   size in pages is set).

                   The use case has not been seen where the database size in pages is 0 and the database size in pages
                   has been calculated.  More investigation is needed.

            """

            calculated_size_in_pages = self.file_handle.file_size / self.page_size

            if self.database_header.database_size_in_pages != calculated_size_in_pages:

                # Set the database size in pages to the database header size in pages
                self.database_size_in_pages = self.database_header.database_size_in_pages

                log_message = "Database header for version: {} specifies a database size in pages of {} but the " \
                              "calculated size in pages is {} instead for sqlite version: {}.  The database size in " \
                              "pages will remain unchanged but possibly erroneous use cases may occur when parsing."
                log_message = log_message.format(self.version_number, self.database_header.database_size_in_pages,
                                                 calculated_size_in_pages, self.database_header.sqlite_version_number)
                self._logger.warn(log_message)
                warn(log_message, RuntimeWarning)

            else:

                self.database_size_in_pages = self.database_header.database_size_in_pages

        """

        Since the main database file is the first version (version number 0) all pages are considered "updated"
        since they are new in terms of the information retrieved from them.

        The page version index will set all page numbers currently in the database pages to the version number of
        this first version (version number 0).

        """

        self.updated_page_numbers = [page_index + 1 for page_index in range(self.database_size_in_pages)]
        self.page_version_index = dict(map(lambda x: [x, self.version_number], self.updated_page_numbers))

        self._logger.debug("Updated page numbers initialized as: {} in version: {}.".format(self.updated_page_numbers,
                                                                                            self.version_number))
        self._logger.debug("Page version index initialized as: {} in version: {}.".format(self.page_version_index,
                                                                                          self.version_number))

        """

        Here we setup the updated b-tree page numbers.  This array will be removed from as we parse through the file
        to leave just the b-tree pages of the commit record that were updated at the end.

        """

        self.updated_b_tree_page_numbers = copy(self.updated_page_numbers)

        """

        Create the freelist trunk and leaf pages.

        Note:  If there are no freelist pages, the first freelist trunk page will be None and there will be an empty
               array for the freelist page numbers.

        """

        if self.database_header.first_freelist_trunk_page_number:
            self.first_freelist_trunk_page = FreelistTrunkPage(self,
                                                               self.database_header.first_freelist_trunk_page_number,
                                                               FIRST_FREELIST_TRUNK_PARENT_PAGE_NUMBER,
                                                               FIRST_FREELIST_TRUNK_PAGE_INDEX)

        self.freelist_page_numbers = []
        observed_freelist_pages = 0
        freelist_trunk_page = self.first_freelist_trunk_page
        while freelist_trunk_page:

            # Remove it from the updated b-tree pages
            self.updated_b_tree_page_numbers.remove(freelist_trunk_page.number)

            self.freelist_page_numbers.append(freelist_trunk_page.number)
            observed_freelist_pages += 1
            for freelist_leaf_page in freelist_trunk_page.freelist_leaf_pages:
                self.freelist_page_numbers.append(freelist_leaf_page.number)
                observed_freelist_pages += 1
            freelist_trunk_page = freelist_trunk_page.next_freelist_trunk_page

        if observed_freelist_pages != self.database_header.number_of_freelist_pages:
            log_message = "The number of observed freelist pages: {} does not match the number of freelist pages " \
                          "specified in the header: {} for version: {}."
            log_message = log_message.format(observed_freelist_pages, self.database_header.number_of_freelist_pages,
                                             self.version_number)
            self._logger.error(log_message)
            raise DatabaseParsingError(log_message)

        """

        Create the pointer map pages.

        Note:  If there are no pointer map pages, both the pointer map pages and pointer map page numbers will be an
               empty array.

        """

        if self.database_header.largest_root_b_tree_page_number:
            self.pointer_map_pages = create_pointer_map_pages(self, self.database_size_in_pages, self.page_size)
        else:
            self.pointer_map_pages = []

        self.pointer_map_page_numbers = []
        for pointer_map_page in self.pointer_map_pages:

            # Remove it from the updated b-tree pages
            self.updated_b_tree_page_numbers.remove(pointer_map_page.number)

            self.pointer_map_page_numbers.append(pointer_map_page.number)

        """

        Create the root page of the SQLite database.

        """

        self._root_page = self.get_b_tree_root_page(SQLITE_MASTER_SCHEMA_ROOT_PAGE)

        """

        Create the master schema from the root page of the SQLite database.

        Note:  There is the possibility that there is no information in the master schema (ie. a "blank" root page).
               To check this we make sure the schema format number and database text encoding are 0 in the header.
               A warning is already printed in the database header if this use case is determined.

               In this case the master schema will double check that the root page is indeed devoid of information
               and will have no schema entries but maintain its fields such as the master schema page numbers which
               will be a list of just the root page such as: [1].

        """

        self._master_schema = MasterSchema(self, self.root_page)

        #  Remove the master schema pages from the updated b-tree pages (this will always include the root page number)
        for master_schema_page_number in self.master_schema.master_schema_page_numbers:
            self.updated_b_tree_page_numbers.remove(master_schema_page_number)

        """

        Since we do not check the schema format number and database text encoding in the master schema, we do that here.
        This is due to the fact that the database header is not sent into the master schema (although if needed it could
        retrieve it through the instance of this class sent in).

        """

        if len(self.master_schema.master_schema_entries) == 0:
            if self.database_header.schema_format_number != 0 or self.database_header.database_text_encoding != 0:
                log_message = "No master schema entries found in master schema for version: {} when the database " \
                              "schema format number was: {} and the database text encoding was: {} when both should " \
                              "be 0."
                log_message = log_message.format(self.version_number, self.database_header.schema_format_number,
                                                 self.database_header.database_text_encoding)
                self._logger.error(log_message)
                raise DatabaseParsingError(log_message)

        """

        Setup the flags to report on modifications.

        See the version superclass for more documentation on the setup of these flags for the Database class.

        """

        self.database_header_modified = True
        self.root_b_tree_page_modified = True
        self.master_schema_modified = True

        if self.first_freelist_trunk_page:
            self.freelist_pages_modified = True

        if self.database_header.largest_root_b_tree_page_number:
            self.pointer_map_pages_modified = True

        """

        If the version information is being stored in memory, parse out the pages and store them as a private variable.

        """

        self._pages = {}
        if self.store_in_memory:
            self._pages = self.pages

    @Version.database_text_encoding.setter
    def database_text_encoding(self, database_text_encoding):
        log_message = "Database text encoding {} requested to be set on database.  Operation not permitted.  " \
                      "Should be set during object construction."
        log_message = log_message.format(database_text_encoding)
        self._logger.error(log_message)
        raise TypeError(log_message)

    def get_page_data(self, page_number, offset=0, number_of_bytes=None):

        # Set the number of bytes to the rest of the page if it was not set
        number_of_bytes = self.page_size - offset if not number_of_bytes else number_of_bytes

        if offset >= self.page_size:
            log_message = "Requested offset: {} is >= the page size: {} for page: {}."
            log_message = log_message.format(offset, self.page_size, page_number)
            self._logger.error(log_message)
            raise ValueError(log_message)

        if offset + number_of_bytes > self.page_size:
            log_message = "Requested length of data: {} at offset {} to {} is greater than the page " \
                          "size: {} for page: {}."
            log_message = log_message.format(number_of_bytes, offset, number_of_bytes + offset,
                                             self.page_size, page_number)
            self._logger.error(log_message)
            raise ValueError(log_message)

        page_offset = self.get_page_offset(page_number)

        return self.file_handle.read_data(page_offset + offset, number_of_bytes)

    def get_page_offset(self, page_number):

        if page_number < 1 or page_number > self.database_size_in_pages:
            log_message = "Invalid page number: {} for version: {} with database size in pages: {}."
            log_message = log_message.format(page_number, self.version_number, self.database_size_in_pages)
            self._logger.error(log_message)
            raise ValueError(log_message)

        return (page_number - 1) * self.page_size
