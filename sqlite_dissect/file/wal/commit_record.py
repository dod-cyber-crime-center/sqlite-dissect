from copy import copy
from warnings import warn
from sqlite_dissect.constants import DATABASE_HEADER_VERSIONED_FIELDS
from sqlite_dissect.constants import FIRST_FREELIST_TRUNK_PARENT_PAGE_NUMBER
from sqlite_dissect.constants import FIRST_FREELIST_TRUNK_PAGE_INDEX
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from sqlite_dissect.constants import SQLITE_MASTER_SCHEMA_ROOT_PAGE
from sqlite_dissect.constants import UTF_8
from sqlite_dissect.constants import UTF_8_DATABASE_TEXT_ENCODING
from sqlite_dissect.constants import UTF_16BE
from sqlite_dissect.constants import UTF_16BE_DATABASE_TEXT_ENCODING
from sqlite_dissect.constants import UTF_16LE
from sqlite_dissect.constants import UTF_16LE_DATABASE_TEXT_ENCODING
from sqlite_dissect.constants import WAL_FRAME_HEADER_LENGTH
from sqlite_dissect.constants import WAL_HEADER_LENGTH
from sqlite_dissect.exception import WalCommitRecordParsingError
from sqlite_dissect.file.database.header import DatabaseHeader
from sqlite_dissect.file.database.page import FreelistTrunkPage
from sqlite_dissect.file.database.utilities import create_pointer_map_pages
from sqlite_dissect.file.schema.master import MasterSchema
from sqlite_dissect.file.version import Version
from sqlite_dissect.file.wal.utilities import compare_database_headers
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.utilities import get_md5_hash

"""

version.py

This script holds the objects used for parsing the write ahead log commit records.

This script holds the following object(s):
WriteAheadLogCommitRecord(Version)

"""


class WriteAheadLogCommitRecord(Version):

    """

    This class extends the Version class and represents a version based on a commit record in the WAL file.  The
    database is not considered "committed" until a frame appears in the WAL file with a size of database in pages field
    set declaring it a commit record.  The SQLite drivers do not read any information out after the last commit record
    (if there is any information).  Therefore we structure each set of frames up to a commit record as a commit record
    version and parse it as such.

    Due to the way only parts of the commit record are updated, only parts of the SQLite database will be parsed and
    stored in this class.  For instance, the database header and master schema will only be parsed if they are changed
    from the previous version.  Otherwise, the last database header and last master schema will be set with the previous
    version's for reference.  If the database header and/or master schema is modified, then the objects will be parsed.
    Also, their respective modified flags will be set.  This is to reduce memory and parsing time.

    The idea here is that the database header or master schema should never be needed unless changes were done which
    can be checked by their respective modified flags which are set in the version and set to true for the original
    database.

    However, in order to support the version class, functions have been put in place that will pull the master schema,
    root page, and database header for this version if needed, on demand (unless the "store in memory flag" is set).

    The freelist pages and pointer map pages are always parsed since the overhead to do so is minimal and freelist pages
    need to be parsed in order to ensure changes in the pages.

    If the "store in memory" flag is set, the commit record will be fully parsed and stored in memory.  This includes
    the database header and master schema, regardless of changes, and all pages including b-tree pages.  This flag is
    defaulted to False rather than True as it is defaulted to in the database class due to the nature of how the commit
    records are parsed vs the original database.

    Note: The version number of the first commit record defined must start at 1.  The previous version to the first
          WAL commit record is 0 and will be the base SQLite database file.

    Note: The following fields will be parsed on demand unless this commit record has specific updated pages with
          regards to them (unless the "store in memory" flag is set):
          1.) self._database_header
          2.) self._root_page

          Note:  The root page may not be set if the database header is set since the root page refers to the master
                 schema and not the database header.  However, the root page will always be set if the master schema
                 is set and vice-versa.

          3.) self._master_schema

    """

    def __init__(self, version_number, database, write_ahead_log, frames, page_frame_index, page_version_index,
                 last_database_header, last_master_schema, store_in_memory=False, strict_format_checking=True):

        super(WriteAheadLogCommitRecord, self).__init__(write_ahead_log.file_handle, version_number,
                                                        store_in_memory, strict_format_checking)

        """

        Note:  The database is needed to refer to the file handle in order to read page data out of the database file
               if the particular page being requested has not been updated in the WAL file frames yet.

        Note:  The write ahead log is needed only for the use case of setting the database text encoding if it was
               not previously set by the database file (Due to a database file with "no content").

        """

        self._database = database

        for page_version_number in page_version_index.itervalues():
            if page_version_number >= version_number:
                log_message = "Page version number: {} is greater than the commit record specified version: {}."
                log_message = log_message.format(page_version_number, version_number)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

        max_version_number_in_page_version_index = max(page_version_index.values())
        if self.version_number != max_version_number_in_page_version_index + 1:
            log_message = "Version number: {} is not the next version number from the max version: {} in the page " \
                          "version index: {}.."
            log_message = log_message.format(version_number, max_version_number_in_page_version_index,
                                             page_version_index)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        """

        Below we declare a boolean value for committed which explains if this commit record was "committed" to the
        database.  There should be at most one commit record where committed would be false.  As the frames are
        parsed, if a commit frame is found, the committed flag is set to true.  If there are multiple commit frames,
        then an exception is thrown since this is not allowed.

        Note:  If there are more than one commit frames, then that use case needs to be checked outside of this class.

        Note:  As of yet, the use case where there is a set of frames with no commit record has not been seen and
               therefore a committed flag will determine if this commit frame was committed to the WAL file or not.
               In the creating class (VersionHistory), a warning will be thrown if this use case is detected since it
               has not been investigated and handled correctly.

        The committed page size is determined from the commit frame in the frames and may be left as None if this is
        the commit record at the end of the file (if it exists) that was not committed and does not have a commit frame.

        The frames variable is a dictionary of page number to frame:
        self.frames[FRAME.PAGE_NUMBER] = FRAME

        """

        self.committed = False
        self.committed_page_size = None
        self.frames = {}

        # Iterate through the frames
        for frame in frames:

            # Make sure the page number to the current frame doesn't already exist in the previous frames
            if frame.header.page_number in self.frames:
                log_message = "Frame page number: {} found already existing in frame page numbers: {} in version: {}."
                log_message = log_message.format(frame.header.page_number, self.frames.keys(), self.version_number)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Check if the frame is a commit frame
            if frame.commit_frame:

                # Make sure this commit frame hasn't already been committed
                if self.committed:
                    log_message = "Frame page number: {} is a commit frame when commit record was already committed " \
                                  "with frame page numbers: {} in version: {}."
                    log_message = log_message.format(frame.header.page_number, self.frames.keys(), self.version_number)
                    self._logger.error(log_message)
                    raise WalCommitRecordParsingError(log_message)

                # Set the committed flag to true
                self.committed = True

                # Make sure the committed page size has not already been set and set it
                if self.committed_page_size:
                    log_message = "Frame page number: {} has a committed page size of: {} when it was already set " \
                                  "to: {} with frame page numbers: {} in version: {}."
                    log_message = log_message.format(frame.header.page_number, frame.header.page_size_after_commit,
                                                     self.committed_page_size, self.frames.keys(), self.version_number)
                    self._logger.error(log_message)
                    raise WalCommitRecordParsingError(log_message)

                self.committed_page_size = frame.header.page_size_after_commit

            # Add this frame to the frames dictionary
            self.frames[frame.header.page_number] = frame

        # Set the updated page numbers derived from this commit records frame keys
        self.updated_page_numbers = copy(self.frames.keys())

        log_message = "Commit Record Version: {} has the updated page numbers: {}."
        log_message = log_message.format(self.version_number, self.updated_page_numbers)
        self._logger.debug(log_message)

        """

        Here we setup the updated b-tree page numbers.  This array will be removed from as we parse through the file
        to leave just the b-tree pages of the commit record that were updated at the end.

        """

        self.updated_b_tree_page_numbers = copy(self.updated_page_numbers)

        self.page_frame_index = dict.copy(page_frame_index)
        self.page_version_index = dict.copy(page_version_index)
        for updated_page_number in self.updated_page_numbers:
            self.page_version_index[updated_page_number] = self.version_number
            self.page_frame_index[updated_page_number] = self.frames[updated_page_number].frame_number

        self.database_size_in_pages = self.committed_page_size

        """

        Check to make sure the page version index length match the database size in pages as it should.

        Note:  The database size in pages can and has been found to be wrong in some cases where the database
               size in pages is specified where the version valid for number equals the file change counter.  It is
               still unsure of why this can occur but in the use cases this was seen, the database size in pages was
               correct and the file was inflated (padded) with empty space at the end indicating additional pages.
               For this reason a warning is thrown instead of an exception (in the case that the version valid for
               number equals the file change counter and database e in pages is set).

               This may involve the WAL file and checkpoints as the file referred to above had a checkpoint sequence
               number that was not 0.  More investigation is needed.

        """

        if len(self.page_version_index) != self.database_size_in_pages:
            log_message = "The page version index of length: {} does not equal the database size in pages: {} " \
                          "in version: {} for page version index: {}.  Possibly erroneous use cases may occur " \
                          "when parsing."
            log_message = log_message.format(len(self.page_version_index), self.database_size_in_pages,
                                             self.version_number, self.page_version_index)
            self._logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        """

        Initialize the root page and master schema to none.

        Note:  These are only initialized if the SQLite master schema root page is in the updated pages and the root
               b-tree (not including the header) is updated or the master schema is updated.  If the root page is set
               the master schema will always be set, and vice-versa.

        """

        self._root_page = None
        self._master_schema = None

        """

        Here we check to see if the SQLite root page was updated or if any of the master schema pages were
        updated since the previous version.  This is done by keeping track of the master schema pages (which
        will always include the root page SQLITE_MASTER_SCHEMA_ROOT_PAGE (1)) and checking if the new
        commit record contains any of these pages in the frame array.

        If the root page is in the frame array that means that either:
        a.)  The database header was updated and the rest of the root page remained unchanged.
        b.)  Both the database header and root page were changed.
        c.)  Neither the database header or root page was changed.

        The most observed case is a.) since the schema itself does not seem to change often but rather the
        freelist pages, database size in pages, and other fields found in the database header.

        If any of the non-root master schema pages are in the frame array then the master schema was
        updated.  The master schema is assumed to be able to be updated without always updating the root
        page.  However, any change in the master schema should result in the schema cookie being updated
        in the database header meaning that there should never be a case where the master schema is updated
        without updating the database header.

        First we will check to see if the root page is in this commit record's updated page numbers.  If it is, then
        we will check the database header md5 against the last database header md5 and the root page only md5 hex
        digest against the previous master schema root page root page only md5 hex digest.

        This will tell us if the database header changed, and insight into if the master schema changed.
        We will not know 100% if the master schema changed until we check all master schema pages against the updated
        pages in this commit record.  However, if we did find out that the master schema has changed this last step
        is not needed.

        """

        if SQLITE_MASTER_SCHEMA_ROOT_PAGE in self.updated_page_numbers:

            # Remove it from the updated b-tree pages
            self.updated_b_tree_page_numbers.remove(SQLITE_MASTER_SCHEMA_ROOT_PAGE)

            """

            Note:  There is a redundancy here in calculating these md5 hash values but the trade off is to
                   parse the objects when not needed versus calculating md5s of a small portion of that data.
                   Keep in mind this only occurs when the SQLite master schema root page is in the updated page numbers.

            """

            root_page_data = self.get_page_data(SQLITE_MASTER_SCHEMA_ROOT_PAGE)
            database_header_md5_hex_digest = get_md5_hash(root_page_data[:SQLITE_DATABASE_HEADER_LENGTH])
            root_page_only_md5_hex_digest = get_md5_hash(root_page_data[SQLITE_DATABASE_HEADER_LENGTH:])

            if last_database_header.md5_hex_digest != database_header_md5_hex_digest:
                self.database_header_modified = True
                self._database_header = DatabaseHeader(root_page_data[:SQLITE_DATABASE_HEADER_LENGTH])

                if self._database_header.md5_hex_digest != database_header_md5_hex_digest:
                    log_message = "The database header md5 hex digest: {} did not match the previously retrieved " \
                                  "calculated database header md5 hex digest: {} in commit record version: {} " \
                                  "on updated pages: {}."
                    log_message = log_message.format(self._database_header.md5_hex_digest,
                                                     database_header_md5_hex_digest, self.version_number,
                                                     self.updated_page_numbers)
                    self._logger.error(log_message)
                    raise WalCommitRecordParsingError(log_message)

            """

            Note:  The root b-tree page modified flag may be False where the master schema modified flag may be True
                   depending on if the pages in the master schema updated included the SQLite master schema root
                   page (1) or not.

            """

            if last_master_schema.root_page.header.root_page_only_md5_hex_digest != root_page_only_md5_hex_digest:
                self.root_b_tree_page_modified = True
                self.master_schema_modified = True

            """

            The root page may be in the updated page numbers in the WAL commit record even if neither the database
            header or the root page itself was modified (ie. the page in general).  It is not sure why this occurs
            and more research needs to be done into the exact reasoning.  One theory is that if pointer map pages
            are updated, then the root page is automatically included.  This could be a flag in the SQLite source
            code that sets the root page to have been modified for instance if the largest b-tree root page number
            is updated, but updated to the same number.  For this reason, we throw a warning below

            """

            if not self.database_header_modified and not self.root_b_tree_page_modified:
                log_message = "The sqlite database root page was found in version: {} in the updated pages: {} when " \
                              "both the database header and the root b-tree page were not modified."
                log_message = log_message.format(self.version_number, self.updated_page_numbers)
                self._logger.warn(log_message)
                warn(log_message, RuntimeWarning)

        if not self.master_schema_modified:

            for last_master_schema_page_number in last_master_schema.master_schema_page_numbers:

                """

                Since we are removing the use case of the SQLite master schema root page and checking for master
                schema modifications on other pages, as long as we find at least one page here, we satisfy our
                use case and can break.

                Note:  We could argue that we should parse the master schema again to make sure the master schema
                       did not change, but we can do the same by checking the previous master schema pages and if
                       any of them were updated, as they would have to be if any change was made, figure out from there
                       without having to deal with the extra overhead of parsing the master schema.

                """

                if last_master_schema_page_number != SQLITE_MASTER_SCHEMA_ROOT_PAGE:
                    if last_master_schema_page_number in self.updated_page_numbers:
                        self.master_schema_modified = True
                        break

        if not self.database_header_modified and self.master_schema_modified:
            log_message = "The database header was not modified when the master schema was modified in version: {}."
            log_message = log_message.format(self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        """

        The database header differences will be a dictionary with the key being within the
        DATABASE_HEADER_VERSIONED_FIELDS Enum constant variables and value will be a tuple where
        the first element will be the value that field held previously and the second element will
        be the new value of that field.

        """

        if self.database_header_modified:

            if not self._database_header:
                log_message = "The database header does not exist when the database header was modified in commit " \
                              "record version: {} on updated pages: {}."
                log_message = log_message.format(self.version_number, self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            self.database_header_differences = compare_database_headers(last_database_header, self._database_header)

            log_message = "Database header was modified in version: {} with differences: {}."
            log_message = log_message.format(self.version_number, self.database_header_differences)
            self._logger.info(log_message)

        else:

            self.database_header_differences = {}

            """

            Note:  Below we do not need to worry about the database page in sizes being 0 since this is a write ahead
                   log file being parsed which requires SQLite version >= 3.7.0.  However, there may still be a use
                   case where the page number is wrong depending on if it was previously opened with a SQLite version
                   < 3.7.0 and has not been updated yet, however, this use case may not occur and has still yet to be
                   seen.  For now, an exception is raised.

            Note:  Below a warning is thrown instead of an exception because the committed page size has been found to
                   be wrong in some cases where the database size in pages is specified where the version valid for
                   number equals the file change counter.  It is still unsure of why this can occur but in the use cases
                   this was seen, the committed page size was correct and the file was inflated (padded) with empty
                   space at the end indicating additional pages  when calculating page size from file size.  The
                   database class has additional documentation on this occurring and allows this since it has not been
                   determined why exactly this occurs.

            """

            # Make sure the database size in pages remained the same as the committed page size
            if self.committed_page_size != last_database_header.database_size_in_pages:

                log_message = "Database header for version: {} specifies a database size in pages of {} but the " \
                              "committed page size is {}.  Possibly erroneous use cases may occur when parsing."
                log_message = log_message.format(self.version_number, last_database_header.database_size_in_pages,
                                                 self.committed_page_size)
                self._logger.warn(log_message)
                warn(log_message, RuntimeWarning)

        if self.master_schema_modified:

            log_message = "Master schema was modified in version: {}."
            log_message = log_message.format(self.version_number)
            self._logger.info(log_message)

        """

        Below are fields that are set in the case that the database header is modified.

        These variables are set by the parse database header differences private function.  If the value is
        not a boolean, then it will only be set if it was updated in the header.

        Note:  Even though the number of freelist pages modified may not be set, it does not mean that there have not
               been updates to the pages.  Same with the first freelist trunk page as well as both fields.

        Note:  Pointer map pages may still be updated even if the modified largest root b-tree page number was not
               modified.  (Assuming it was not 0 and auto-vacuuming is turned on.)

        Note:  If the database text encoding was not previously set in the versions, it will be set here.

        """

        self.file_change_counter_incremented = False
        self.version_valid_for_number_incremented = False
        self.database_size_in_pages_modified = False
        self.modified_first_freelist_trunk_page_number = None
        self.modified_number_of_freelist_pages = None
        self.modified_largest_root_b_tree_page_number = None
        self.schema_cookie_modified = False
        self.schema_format_number_modified = False
        self.database_text_encoding_modified = False
        self.user_version_modified = False

        """

        Call the _parse_database_header_differences method to setup the above variables and check header use cases.

        """

        self._parse_database_header_differences()

        """

        Create the root page and master schema if the master schema was detected to be modified.  Also, remove all
        master schema page numbers from the updated b-tree pages.

        """

        if self.master_schema_modified:

            self._root_page = self.get_b_tree_root_page(SQLITE_MASTER_SCHEMA_ROOT_PAGE)

            self._master_schema = MasterSchema(self, self._root_page)

            # Remove the master schema page numbers from the updated b-tree pages
            for master_schema_page_number in self._master_schema.master_schema_page_numbers:
                if master_schema_page_number in self.updated_b_tree_page_numbers:
                    self.updated_b_tree_page_numbers.remove(master_schema_page_number)

        """

        Since we do not know if the freelist pages could have been updated or not we always set them here.
        We also set the pointer map pages if they the largest root b-tree page number is specified.

        Note:  If there are no freelist pages, the first freelist trunk page will be None and there will be an empty
               array for the freelist page numbers.

        Note:  We could check and only set the pointer map pages if they were updated but it was decided to do that
               regardless in order to fit the object structure of the version and database better and due to the low
               overhead of doing this.

        """

        first_freelist_trunk_page_number = last_database_header.first_freelist_trunk_page_number
        if self._database_header:
            first_freelist_trunk_page_number = self._database_header.first_freelist_trunk_page_number

        if first_freelist_trunk_page_number:
            self.first_freelist_trunk_page = FreelistTrunkPage(self, first_freelist_trunk_page_number,
                                                               FIRST_FREELIST_TRUNK_PARENT_PAGE_NUMBER,
                                                               FIRST_FREELIST_TRUNK_PAGE_INDEX)

        self.freelist_page_numbers = []
        observed_freelist_pages = 0
        freelist_trunk_page = self.first_freelist_trunk_page
        while freelist_trunk_page:
            self.freelist_page_numbers.append(freelist_trunk_page.number)
            observed_freelist_pages += 1
            for freelist_leaf_page in freelist_trunk_page.freelist_leaf_pages:
                self.freelist_page_numbers.append(freelist_leaf_page.number)
                observed_freelist_pages += 1
            freelist_trunk_page = freelist_trunk_page.next_freelist_trunk_page

        number_of_freelist_pages = last_database_header.number_of_freelist_pages
        if self._database_header:
            number_of_freelist_pages = self._database_header.number_of_freelist_pages

        if observed_freelist_pages != number_of_freelist_pages:
            log_message = "The number of observed freelist pages: {} does not match the number of freelist pages " \
                          "specified in the header: {} for version: {}."
            log_message = log_message.format(observed_freelist_pages, number_of_freelist_pages, self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        for freelist_page_number in self.freelist_page_numbers:
            if freelist_page_number in self.updated_page_numbers:
                self.freelist_pages_modified = True

                # Remove the freelist page numbers from the updated b-tree pages
                if freelist_page_number in self.updated_b_tree_page_numbers:
                    self.updated_b_tree_page_numbers.remove(freelist_page_number)

        """

        Create the pointer map pages.

        Note:  If there are no pointer map pages, both the pointer map pages and pointer map page numbers will be an
               empty array.

        """

        largest_root_b_tree_page_number = last_database_header.largest_root_b_tree_page_number
        if self._database_header:
            largest_root_b_tree_page_number = self._database_header.largest_root_b_tree_page_number

        if largest_root_b_tree_page_number:
            self.pointer_map_pages = create_pointer_map_pages(self, self.database_size_in_pages, self.page_size)
        else:
            self.pointer_map_pages = []

        self.pointer_map_page_numbers = []
        for pointer_map_page in self.pointer_map_pages:
            self.pointer_map_page_numbers.append(pointer_map_page.number)

        for pointer_map_page_number in self.pointer_map_page_numbers:
            if pointer_map_page_number in self.updated_page_numbers:
                self.pointer_map_pages_modified = True

                # Remove the pointer map page numbers from the updated b-tree pages
                if pointer_map_page_number in self.updated_b_tree_page_numbers:
                    self.updated_b_tree_page_numbers.remove(pointer_map_page_number)

        """

        Note:  At this point the updated_b_tree_page_numbers has all of the page numbers that refer to updated b-trees
               in this commit record with all master schema, freelist, and pointer map pages filtered out.

        """

        """

        The last database header and last master schema are set if no database header or master schema was parsed from
        this commit record for reference.

        """

        self.last_database_header = None
        if not self.database_header_modified:
            self.last_database_header = last_database_header

        self.last_master_schema = None
        if not self.master_schema_modified:
            self.last_master_schema = last_master_schema

        """

        If the version information is being stored in memory, parse out the database header, root page, and master
        schema (if it was already not parsed out) and pages and store them as a private variable.

        """

        if self.store_in_memory:

            if not self._database_header:
                root_page_data = self.get_page_data(SQLITE_MASTER_SCHEMA_ROOT_PAGE)
                self._database_header = DatabaseHeader(root_page_data[:SQLITE_DATABASE_HEADER_LENGTH])

            if not self._root_page:
                self._root_page = self.get_b_tree_root_page(SQLITE_MASTER_SCHEMA_ROOT_PAGE)

            if not self._master_schema:
                self._master_schema = MasterSchema(self, self._root_page)

            self._pages = self.pages

        log_message = "Commit record: {} on page numbers: {} successfully created."
        log_message = log_message.format(self.version_number, self.updated_page_numbers)
        self._logger.info(log_message)

    def stringify(self, padding="", print_pages=True, print_schema=True, print_frames=True):

        # Create the initial string
        string = "\n" \
                 + padding + "Committed: {}\n" \
                 + padding + "Committed Page Size: {}\n" \
                 + padding + "Frames Length: {}\n" \
                 + padding + "Page Frame Index: {}\n" \
                 + padding + "File Change Counter Incremented: {}\n" \
                 + padding + "Version Valid for Number Incremented: {}\n" \
                 + padding + "Database Size in Pages Modified: {}\n" \
                 + padding + "Modified First Freelist Trunk Page Number: {}\n" \
                 + padding + "Modified Number of Freelist Pages: {}\n" \
                 + padding + "Modified Largest Root B-Tree Page Number: {}\n" \
                 + padding + "Schema Cookie Modified: {}\n" \
                 + padding + "Schema Format Number Modified: {}\n" \
                 + padding + "Database Text Encoding Modified: {}\n" \
                 + padding + "User Version Modified: {}"

        # Format the string
        string = string.format(self.committed,
                               self.committed_page_size,
                               self.frames_length,
                               self.page_frame_index,
                               self.file_change_counter_incremented,
                               self.version_valid_for_number_incremented,
                               self.database_size_in_pages_modified,
                               self.modified_first_freelist_trunk_page_number,
                               self.modified_number_of_freelist_pages,
                               self.modified_largest_root_b_tree_page_number,
                               self.schema_cookie_modified,
                               self.schema_format_number_modified,
                               self.database_text_encoding_modified,
                               self.user_version_modified)

        # Add the database header differences
        string += "\n" + padding + "Database Header Differences:"

        # Parse the database header differences
        for field, difference in self.database_header_differences.iteritems():
            difference_string = "\n" + padding + "\t" + "Field: {} changed from previous Value: {} to new Value: {}"
            string += difference_string.format(field, difference[0], difference[1])

        # Print the frames if specified
        if print_frames:
            for page_number in self.frames:
                string += "\n" + padding + "Frame:\n{}".format(self.frames[page_number].stringify(padding + "\t"))

        # Get the super stringify information and concatenate it with this string and return it
        return super(WriteAheadLogCommitRecord, self).stringify(padding, print_pages, print_schema) + string

    @property
    def frames_length(self):
        return len(self.frames)

    def get_page_data(self, page_number, offset=0, number_of_bytes=None):

        page_version = self.page_version_index[page_number]

        if page_version == BASE_VERSION_NUMBER:

            return self._database.get_page_data(page_number, offset, number_of_bytes)

        else:

            # Set the number of bytes to the rest of the page if it was not set
            number_of_bytes = self.page_size - offset if not number_of_bytes else number_of_bytes

            if offset >= self.page_size:
                log_message = "Requested offset: {} is >= the page size: {} for page: {}."
                log_message = log_message.format(offset, self.page_size, page_number)
                self._logger.error(log_message)
                raise ValueError(log_message)

            if offset + number_of_bytes > self.page_size:
                log_message = "Requested length of data: {} at offset {} to {} is > than the page size: {} " \
                              "for page: {}."
                log_message = log_message.format(number_of_bytes, offset, number_of_bytes + offset,
                                                 self.page_size, page_number)
                self._logger.error(log_message)
                raise ValueError(log_message)

            page_offset = self.get_page_offset(page_number)

            return self.file_handle.read_data(page_offset + offset, number_of_bytes)

    def get_page_offset(self, page_number):

        """



        Note:  This method will return the correct page offset depending on where it last showed up in relation to
               this commit frame.  Therefore the page offset may be very close to the beginning of the WAL file when
               the last committed record in the set of frames is near the end of the WAL file.  This could also return
               an offset in the database file if the WAL file did not have the page updated in it's frames yet.

               This is presumed safe since the get_page_data takes in a page number and unless people are using the
               read method directly from the file handles, this function is more for informative purposes.  If someone
               was reading directly from the file handles, it is assumed they would know the inner workings of this
               library.

        :param page_number:

        :return:

        """

        if page_number < 1 or page_number > self.database_size_in_pages:
            log_message = "Invalid page number: {} for version: {} with database size in pages: {}."
            log_message = log_message.format(page_number, self.version_number, self.database_size_in_pages)
            self._logger.error(log_message)
            raise ValueError(log_message)

        page_version = self.page_version_index[page_number]

        if page_version == BASE_VERSION_NUMBER:

            return (page_number - 1) * self.page_size

        else:

            if page_version == self.version_number:

                if page_number not in self.frames:
                    log_message = "Page number has version: {} but not in frame pages: {}."
                    log_message = log_message.format(page_number, self.frames.keys())
                    self._logger.error(log_message)
                    raise WalCommitRecordParsingError(log_message)

            if page_number not in self.page_frame_index:
                log_message = "Page number: {} with version: {} is not in the page frame index: {}."
                log_message = log_message.format(page_number, page_version, self.page_frame_index)
                self._logger.error(log_message)
                raise KeyError(log_message)

            frame_number = self.page_frame_index[page_number]

            """

            The WAL file is structured with a file header, then a series of frames that each have a frame header and
            page in them.  The offset is determined by adding the WAL header length to the number of frame header
            before the page content and then added to the page size multiplied by the number of frames (minus the
            current one).

            """

            # Return where the offset of the page to this commit record in the WAL file would start at
            return WAL_HEADER_LENGTH + WAL_FRAME_HEADER_LENGTH * frame_number + self.page_size * (frame_number - 1)

    def _parse_database_header_differences(self):

        """

        This function is a private function that will check and set the variables for this commit record for differences
        in database headers between this commit record and the last database header.

        Note:  The database header differences will be a dictionary keyed by the DATABASE_HEADER_VERSIONED_FIELDS
               which will refer to a tuple where the first value will be the previous database header value and the
               second value will be the new database header value.

        :param self:

        :raise:

        """

        # Make sure there are database header differences
        if not self.database_header_differences:

            # There are no differences so return
            return

        # Make a copy of the database header differences to work with
        database_header_differences = dict.copy(self.database_header_differences)

        """

        This shows that the database headers are different and therefore one of the database header fields
        have been updated.  There are only a specific set of database header fields we expect to change here.
        These are found in the DATABASE_HEADER_VERSIONED_FIELDS constant as the following properties of
        the database header class:
        1.)  MD5_HEX_DIGEST: md5_hex_digest
        2.)  FILE_CHANGE_COUNTER: file_change_counter
        3.)  VERSION_VALID_FOR_NUMBER: version_valid_for_number
        4.)  DATABASE_SIZE_IN_PAGES: database_size_in_pages
        5.)  FIRST_FREELIST_TRUNK_PAGE_NUMBER: first_freelist_trunk_page_number
        6.)  NUMBER_OF_FREE_LIST_PAGES: number_of_freelist_pages
        7.)  LARGEST_ROOT_B_TREE_PAGE_NUMBER: largest_root_b_tree_page_number
        8.)  SCHEMA_COOKIE: schema_cookie
        9.)  SCHEMA_FORMAT_NUMBER: schema_format_number
        10.) DATABASE_TEXT_ENCODING: database_text_encoding
        11.) USER_VERSION: user_version

        In order to check these fields we first compare the two headers to get back a dictionary keyed by
        the property name (above in capitals) with a tuple value where the first element is the previous
        database header value and the second element is the modified database header value.  The property will
        only exist in the dictionary if the values between the two headers are different.  If additional
        fields not defined above are found to be different, an exception is thrown in order to alert us
        to the "assumed not to happen" use case.

        Note:  The MD5_HEX_DIGEST: md5_hex_digest is a field of the database header class but not a field in the
               actual database header itself.

        """

        """

        1.) MD5_HEX_DIGEST: md5_hex_digest:
        This will be different between both database headers since it was checked in order to enter this
        area of code.  However, this is still a property of the database header class and therefore needs to
        be accounted for.  If the md5 hex digests are not different (are not in the returned database
        header differences dictionary), then a very weird use case has shown up.

        """

        if DATABASE_HEADER_VERSIONED_FIELDS.MD5_HEX_DIGEST not in database_header_differences:
            log_message = "The database header md5 hex digests are not different in the database headers " \
                          "for version: {}."
            log_message = log_message.format(self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        # Delete the entry from the dictionary
        del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.MD5_HEX_DIGEST]

        """

        The next two fields we will check together are:
        2.) FILE_CHANGE_COUNTER: file_change_counter
        3.) VERSION_VALID_FOR_NUMBER: version_valid_for_number

        These fields are changed whenever the database file is unlocked after having been modified.  However,
        since this is parsed in a commit record, WAL mode will be in use.  In WAL mode, changes to the database
        are instead detected using the wal-index (shm) file so this change counter is not needed.  Therefore,
        the change counter may not be incremented on each transaction.

        Previously, an assumption was made that these fields were incremented only when a checkpoint occurred in
        a WAL file.  However, these fields were found incremented in commit records of the WAL file outside of
        checkpoints occurring.  It is still not sure exactly what may or may not cause these fields to increment
        in the WAL commit record itself.

        If either one of these fields is incremented, then the other field must also be incremented and both
        must be equal.  If the case appears that one has been modified and the other one has been not, an
        exception will be thrown.

        """

        # Check that the file change counter was not modified without the version valid for number
        if DATABASE_HEADER_VERSIONED_FIELDS.FILE_CHANGE_COUNTER in database_header_differences \
                and DATABASE_HEADER_VERSIONED_FIELDS.VERSION_VALID_FOR_NUMBER not in database_header_differences:
            log_message = "The database header file change counter: {} was found in the database header " \
                          "differences but the version valid for number was not for version: {}."
            log_message = log_message.format(database_header_differences[
                                                 DATABASE_HEADER_VERSIONED_FIELDS.FILE_CHANGE_COUNTER],
                                             self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        # Check that the version valid for number was not modified without the file change counter
        elif DATABASE_HEADER_VERSIONED_FIELDS.VERSION_VALID_FOR_NUMBER in database_header_differences \
                and DATABASE_HEADER_VERSIONED_FIELDS.FILE_CHANGE_COUNTER not in database_header_differences:
            log_message = "The database header version valid for number: {} was found in the database header " \
                          "differences but the file change counter was not for version: {}."
            log_message = log_message.format(database_header_differences[
                                                 DATABASE_HEADER_VERSIONED_FIELDS.VERSION_VALID_FOR_NUMBER],
                                             self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        # Check if both file change counter and version valid for number was modified
        elif DATABASE_HEADER_VERSIONED_FIELDS.FILE_CHANGE_COUNTER in database_header_differences \
                and DATABASE_HEADER_VERSIONED_FIELDS.VERSION_VALID_FOR_NUMBER in database_header_differences:

            """

            Note:  We check both fields are incremented only one value from their value in the previous version.
                   If they are not, an exception is thrown.  This may be incorrect and their values may be able to
                   increment more than one value but more investigation is needed on this.

            """

            # Get the file change counter difference
            file_change_counter_difference = database_header_differences[
                                                   DATABASE_HEADER_VERSIONED_FIELDS.FILE_CHANGE_COUNTER]

            # Check the file change counter difference against it's previous value as stated above
            if file_change_counter_difference[0] + 1 != file_change_counter_difference[1]:
                log_message = "The previous database header file change counter: {} is more than one off from the " \
                              "new database header file change counter: {} for version: {}."
                log_message = log_message.format(file_change_counter_difference[0], file_change_counter_difference[1],
                                                 self.version_number)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Get the version valid for number difference
            version_valid_for_number_difference = database_header_differences[
                                                    DATABASE_HEADER_VERSIONED_FIELDS.VERSION_VALID_FOR_NUMBER]

            # Check the version valid for number difference against it's previous value as stated above
            if version_valid_for_number_difference[0] + 1 != version_valid_for_number_difference[1]:
                log_message = "The previous database header version valid for number: {} is more than one off from " \
                              "the new database header version valid for number: {} for version: {}."
                log_message = log_message.format(version_valid_for_number_difference[0],
                                                 version_valid_for_number_difference[1], self.version_number)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Update the class variables to signify these fields were incremented
            self.file_change_counter_incremented = True
            self.version_valid_for_number_incremented = True

            # Delete the entries from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.FILE_CHANGE_COUNTER]
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.VERSION_VALID_FOR_NUMBER]

        """

        4.) DATABASE_SIZE_IN_PAGES: database_size_in_pages:

        Here we check if the database size in pages was updated from it's previous size.  If it was we check this
        against the committed page size for the commit record.

        Note: We check that the committed page size obtained from the size of the database file in pages field
              in the commit record frame is equal to the database size in pages.  This should always be equal
              unless the previous use case occurs which is checked for above where the "version valid for"
              field does not match the change counter.  But this will cause an exception preventing the code
              from reaching this point.  This should additionally be checked since the committed page size
              should equal the database header of the previous version database header if the database size
              in pages field did not change.

        """

        if DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_SIZE_IN_PAGES in database_header_differences:

            # Get the database size in pages difference
            database_size_in_pages_difference = database_header_differences[
                                                    DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_SIZE_IN_PAGES]

            # The committed page size is checked here but should also be checked at the end of this process
            if self.committed_page_size != database_size_in_pages_difference[1]:
                log_message = "The committed page size: {} of commit record version: {} does not match the database" \
                              "header size in pages: {} changed from {} on updated pages: {}."
                log_message = log_message.format(self.committed_page_size, self.version_number,
                                                 database_size_in_pages_difference[1],
                                                 database_size_in_pages_difference[0],
                                                 self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Set the database size in pages modified flag
            self.database_size_in_pages_modified = True

            # Delete the entry from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_SIZE_IN_PAGES]

        """

        The next two fields we are going to pay attention to are in respect to freelist pages:
        5.) FIRST_FREELIST_TRUNK_PAGE_NUMBER: first_freelist_trunk_page_number
        6.) NUMBER_OF_FREELIST_PAGES: number_of_freelist_pages

        If either of these two fields are different it signifies that the freelist pages in the database were
        changed.  If there were no freelist pages previously then both of these should values should be 0 and
        not included in the database header differences dictionary after comparison.

        Additional use cases:

        1.) The first freelist trunk page number could be 0 as well as the number of freelist pages whereas
        previously there was at least one freelist trunk page existing.  This is checked by making sure
        all previous freelist pages are checked that they are either accounted for in this freelist page set
        or not in this freelist set but in the pages of this commit record as another page.  If not, an
        exception is thrown.

        2.) There is a possibility where the freelist pages were updated without changing the
        number of freelist pages and/or freelist trunk page which additionally needs to be checked.
        This would mean freelist pages could change without updates to the database header itself.

        3.) If the database size in pages changed then the freelist pages could be out of range if the modified
        size is less than the previous size.  However, this use case applies to all other page types as well
        and will be checked when the database size is checked against all of the page numbers in the
        database/WAL commit record so it is not needed to be worried about here.

        """

        if DATABASE_HEADER_VERSIONED_FIELDS.FIRST_FREELIST_TRUNK_PAGE_NUMBER in database_header_differences:
            value = database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.FIRST_FREELIST_TRUNK_PAGE_NUMBER]
            self.modified_first_freelist_trunk_page_number = value[1]

            # Delete the entry from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.FIRST_FREELIST_TRUNK_PAGE_NUMBER]

        if DATABASE_HEADER_VERSIONED_FIELDS.NUMBER_OF_FREE_LIST_PAGES in database_header_differences:
            value = database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.NUMBER_OF_FREE_LIST_PAGES]
            self.modified_number_of_freelist_pages = value[1]

            # Delete the entry from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.NUMBER_OF_FREE_LIST_PAGES]

        """

        7.) LARGEST_ROOT_B_TREE_PAGE_NUMBER: largest_root_b_tree_page_number
        The next thing to check in the header is the largest root b tree page number.  We will check further
        down if pointer map pages are being used by seeing if this field is set to a non-zero value.  Here
        we are going to see if it changed.  If it did change, we are only worried over the use case of it going
        from 0 to a non-zero value.  According to the SQLite documentation, the auto-vacuuming mode has to be set
        (enabled) before any tables are created in the schema.  Once a table has been created, it cannot be turned
        off.  However, the mode can be changed between full (1) and incremental (2).

        """

        if DATABASE_HEADER_VERSIONED_FIELDS.LARGEST_ROOT_B_TREE_PAGE_NUMBER in database_header_differences:
            change = database_header_differences[
                                            DATABASE_HEADER_VERSIONED_FIELDS.LARGEST_ROOT_B_TREE_PAGE_NUMBER]
            previous_largest_root_b_tree_page_number = change[0]
            new_largest_root_b_tree_page_number = change[1]

            # Check if auto-vacuuming was turned off
            if previous_largest_root_b_tree_page_number and not new_largest_root_b_tree_page_number:
                log_message = "The previous largest root b-tree page number: {} existed where the new one does not " \
                              "meaning that auto-vacuuming was turned off which cannot occur in version: {} on " \
                              "updated pages: {}."
                log_message = log_message.format(previous_largest_root_b_tree_page_number, self.version_number,
                                                 self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Check if auto-vacuuming was turned on
            elif not previous_largest_root_b_tree_page_number and new_largest_root_b_tree_page_number:
                log_message = "The previous largest root b-tree page number did not exist where the new one is: {} " \
                              "meaning that auto-vacuuming was turned on which cannot occur in version: {} on " \
                              "updated pages: {}."
                log_message = log_message.format(previous_largest_root_b_tree_page_number, self.version_number,
                                                 self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            """

            Note: Since an exception is being thrown here, we do not delete the entry from the dictionary.

            """

            """

            At this point we know that auto-vacuuming was on and has remained on and only the largest root
            b tree page number changed.  We had five use cases to be concerned about here:
            1.) Auto-Vacuuming was on initially and then turned off:
                This use case was handled above and an exception is currently thrown.
            2.) Auto-Vacuuming was off initially and then turned on:
                This use case was handled above and an exception is currently thrown.
            3.) Auto-Vacuuming was never on:
                In this case there would be a zero in both headers meaning there would not be a change
                from the previous version and this portion of the code would not be executing.
            4.) Auto-Vacuuming was turned on and the largest root b tree page number did not change:
                In this case both headers would have the same non-zero value meaning there would not be a change
                from the previous version and this portion of the code would not be executing.
            5.) Auto-Vacuuming was turned on and the largest root b tree page number changed:
                Here we don't have to worry about doing anything extra other than removing the change from the
                database header differences so it does not cause a exception later on.  Other areas of the code
                will use the modified largest root b-tree page number to handle pointer map pages.

            """

            # Set the modified largest root b-tree page number
            self.modified_largest_root_b_tree_page_number = new_largest_root_b_tree_page_number

            # Delete the entry from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.LARGEST_ROOT_B_TREE_PAGE_NUMBER]

        """

        8.) SCHEMA_COOKIE: schema_cookie
        Next we check for the schema cookie.  This field is incremented if a change to the database schema
        occurs.  This will mean that at least one of the master schema pages had to change and be in this
        version's pages.  This could be the root page or any of it's b-tree pages (if any).  Keep in mind
        that the schema cookie being incremented does not mean the root page b-tree content has to change, rather
        a leaf page to the root page could change.  Later on in this process, the schema cookie will be checked
        against the master schema pages to make make sure at least one of the pages was in this version, otherwise
        an exception is thrown since this is not expected.

        Note:  If the schema cookie is updated, then the master schema must have been updated so this is check as well.

        """

        if DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_COOKIE in database_header_differences:

            # Get the schema cookie difference
            schema_cookie_difference = database_header_differences[
                                                    DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_COOKIE]

            # Check the schema cookie difference against 'previous value to make sure it is not less
            if schema_cookie_difference[0] > schema_cookie_difference[1]:
                log_message = "The schema cookie was modified but the previous value: {} is greater than the new " \
                              "value: {} which cannot occur in version: {} on updated pages: {}."
                log_message = log_message.format(schema_cookie_difference[0], schema_cookie_difference[1],
                                                 self.version_number, self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Update the file change counter modified variable to signify this field was incremented
            self.schema_cookie_modified = True

            if not self.master_schema_modified:
                log_message = "The schema cookie was modified from {} to: {} indicating the master schema was " \
                              "modified but was found not to have been in version: {} on updated pages: {}."
                log_message = log_message.format(schema_cookie_difference[0], schema_cookie_difference[1],
                                                 self.version_number, self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Delete the entry from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_COOKIE]

        elif self.master_schema_modified:
            log_message = "The schema cookie was not modified indicating the master schema was not modified " \
                          "as well but was found to have been in version: {} on updated pages: {}."
            log_message = log_message.format(self.version_number, self.updated_page_numbers)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        """

        The next two fields to check are the:
        9.)  SCHEMA_FORMAT_NUMBER: schema_format_number
        10.) DATABASE_TEXT_ENCODING: database_text_encoding

        These should only appear where the master schema was originally empty and then had entries added to it.  In
        this case both of these numbers should originally have been zero.  When changed, the schema format number will
        be within the VALID_SCHEMA_FORMATS and the the database text encoding will be within the
        DATABASE_TEXT_ENCODINGS.  However, it is not needed that we check against this since this is done when parsing
        the database header itself.

        When these are specified we check for the following use cases to validate:
        1.) Both fields exist in the database header differences.
        1.) Both of their values were originally 0.
        2.) The database size in pages was originally 1.

        """

        # Check that the schema format number was not modified without the database text encoding
        if DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_FORMAT_NUMBER in database_header_differences \
                and DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_TEXT_ENCODING not in database_header_differences:
            log_message = "The database header schema format number: {} was found in the database header " \
                          "differences but the database text encoding was not for version: {}."
            log_message = log_message.format(database_header_differences[
                                                 DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_FORMAT_NUMBER],
                                             self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        # Check that the database text encoding was not modified without the schema format number
        elif DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_TEXT_ENCODING in database_header_differences \
                and DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_FORMAT_NUMBER not in database_header_differences:
            log_message = "The database header database text encoding: {} was found in the database header " \
                          "differences but the schema format number was not for version: {}."
            log_message = log_message.format(database_header_differences[
                                                 DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_TEXT_ENCODING],
                                             self.version_number)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)

        # Check if both the schema format number was not modified without the database text encoding was modified
        elif DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_FORMAT_NUMBER in database_header_differences \
                and DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_TEXT_ENCODING in database_header_differences:

            # Get the schema format number difference
            schema_format_number_difference = database_header_differences[
                                                    DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_FORMAT_NUMBER]

            # Check that the schema format number was previously 0
            if schema_format_number_difference[0] != 0:
                log_message = "The previous database header schema format number: {} is not equal to 0 as expected " \
                              "and has a new database header schema format number: {} for version: {}."
                log_message = log_message.format(schema_format_number_difference[0], schema_format_number_difference[1],
                                                 self.version_number)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Get the database text encoding difference
            database_text_encoding_difference = database_header_differences[
                                                    DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_TEXT_ENCODING]

            # Check that the database text encoding was previously 0
            if database_text_encoding_difference[0] != 0:
                log_message = "The previous database header database text encoding: {} is not equal to 0 as expected " \
                              "and has a new database header database text encoding: {} for version: {}."
                log_message = log_message.format(database_text_encoding_difference[0],
                                                 database_text_encoding_difference[1], self.version_number)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            """

            Make sure the database size in pages was previously 1.

            Note:  This is pulled from the original database header differences dictionary since it has already been
                   removed from the local copy.

            """

            if DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_SIZE_IN_PAGES not in self.database_header_differences:
                log_message = "The schema format number was changed from: {} to: {} and database text encoding was " \
                              "changed from: {} to: {} when the database size in pages was not updated and " \
                              "stayed the same size of: {} when it should have initially been 1 and changed to a " \
                              "greater number in version: {} on updated pages: {}."
                log_message = log_message.format(schema_format_number_difference[0], schema_format_number_difference[1],
                                                 database_text_encoding_difference[0],
                                                 database_text_encoding_difference[1],
                                                 self.database_size_in_pages,
                                                 self.version_number, self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Get the database size in pages difference
            database_size_in_pages_difference = self.database_header_differences[
                                                                DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_SIZE_IN_PAGES]

            # Check the database size in pages was previously 1
            if database_size_in_pages_difference[0] != 1:
                log_message = "The schema format number was changed from: {} to: {} and database text encoding was " \
                              "changed from: {} to: {} when the database size in pages was updated from: {} to:{} " \
                              "when it should have initially been 1 in version: {} on updated pages: {}."
                log_message = log_message.format(schema_format_number_difference[0], schema_format_number_difference[1],
                                                 database_text_encoding_difference[0],
                                                 database_text_encoding_difference[1],
                                                 database_size_in_pages_difference[0],
                                                 database_size_in_pages_difference[1],
                                                 self.version_number, self.updated_page_numbers)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Update the class variables to signify these fields were incremented
            self.schema_format_number_modified = True
            self.database_text_encoding_modified = True

            """

            Since the database encoding as not been set yet, we set it in the WAL file handle by calling the
            database_text_encoding property of the superclass..  Since nothing should be reading from the database
            since nothing was written to it, we do not have to worry about setting the database text encoding in
            the database.

            Note:  Once the database text encoding is set, it can no longer be changed.

            """

            database_text_encoding = database_text_encoding_difference[1]

            if database_text_encoding == UTF_8_DATABASE_TEXT_ENCODING:
                self.database_text_encoding = UTF_8
            elif database_text_encoding == UTF_16LE_DATABASE_TEXT_ENCODING:
                self.database_text_encoding = UTF_16LE
            elif database_text_encoding == UTF_16BE_DATABASE_TEXT_ENCODING:
                self.database_text_encoding = UTF_16BE
            elif database_text_encoding:
                log_message = "The database text encoding: {} is not recognized as a valid database text encoding."
                log_message = log_message.format(database_text_encoding)
                self._logger.error(log_message)
                raise WalCommitRecordParsingError(log_message)

            # Delete the entries from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.SCHEMA_FORMAT_NUMBER]
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.DATABASE_TEXT_ENCODING]

        """

        11.) USER_VERSION: user_version:

        The user version is not used by SQLite and is a user-defined version for developers to be able to track their
        own versions of a SQLite database file for instances where the schema may be modified constantly, etc.

        Here we only check for this, and report it by setting the flag.  Afterwards, we remove it from the database
        header differences dictionary since it cannot be used to gleam any information about the database file while
        parsing.

        """

        if DATABASE_HEADER_VERSIONED_FIELDS.USER_VERSION in database_header_differences:

            # Set the user version modified flag
            self.user_version_modified = True

            # Delete the entry from the dictionary
            del database_header_differences[DATABASE_HEADER_VERSIONED_FIELDS.USER_VERSION]

        """

        Make sure there are no additional differences that are not accounted for.  If there are, throw an
        exception in order to flag the use case for occurring.

        """

        # Throw an exception if any database header differences still exist
        if database_header_differences:
            log_message = "Database header differences still exist after checking the last database header against " \
                          "this current commit record version: {} on updated pages: {}.  The main set of differences " \
                          "was: {} with remaining differences: {}."
            log_message = log_message.format(self.version_number, self.updated_page_numbers,
                                             self.database_header_differences,
                                             database_header_differences)
            self._logger.error(log_message)
            raise WalCommitRecordParsingError(log_message)
