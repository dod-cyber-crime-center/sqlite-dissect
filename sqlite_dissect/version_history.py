from logging import getLogger
from re import sub
from warnings import warn
from sqlite_dissect.carving.carver import SignatureCarver
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.constants import CELL_SOURCE
from sqlite_dissect.constants import COMMIT_RECORD_BASE_VERSION_NUMBER
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.exception import VersionParsingError
from sqlite_dissect.exception import WalCommitRecordParsingError
from sqlite_dissect.exception import WalFrameParsingError
from sqlite_dissect.file.database.page import BTreePage
from sqlite_dissect.file.database.utilities import aggregate_leaf_cells
from sqlite_dissect.file.database.utilities import get_pages_from_b_tree_page
from sqlite_dissect.file.schema.master import VirtualTableRow
from sqlite_dissect.file.wal.commit_record import WriteAheadLogCommitRecord
from sqlite_dissect.file.version_parser import VersionParser

"""

version_history.py

This script holds the superclass objects used for parsing the database and write ahead log in a sequence of versions
throughout all of the commit records in the write ahead log.

This script holds the following object(s):
VersionHistory(object)
VersionHistoryParser(VersionParser) (with VersionHistoryParserIterator(object) as an inner class)
Commit(object)

"""


class VersionHistory(object):

    """



    This class represents the SQL database and WAL commit records as a sequence of versions.  This way the changes
    from commit record to commit record can be viewed and worked with and each version has information in them that
    lends to them being carved easier.  Here version 0 (BASE_VERSION_NUMBER) is used to always represent the main
    database and then 1 to N versions following the base version represent the commit records up to N.  To note,
    the final commit record, N, has the possibility of being half written and not committed depending if the
    committed page size is set in one of the frames in the commit record or not.

    """

    def __init__(self, database, write_ahead_log=None):

        logger = getLogger(LOGGER_NAME)

        # Set the database and write ahead log
        self._database = database
        self._write_ahead_log = write_ahead_log

        """

        Initialize the versions in for them of:
        versions[VERSION_NUMBER] = database where VERSION_NUMBER = BASE_VERSION_NUMBER (0)
        versions[VERSION_NUMBER] = commit_record_VERSION_NUMBER where VERSION_NUMBER is 1 to N for N commit records.

        """

        self.versions = {BASE_VERSION_NUMBER: self._database}

        if self._write_ahead_log:

            # Set the database text encoding to the write ahead log file if it was set in the database file
            if self._database.database_text_encoding:
                self._write_ahead_log.file_handle.database_text_encoding = self._database.database_text_encoding

            # Set the last database header and master schema to refer to
            last_database_header = self._database.database_header
            last_master_schema = self._database.master_schema

            # These two dictionaries will be updated and sent into every commit record
            page_version_index = self._database.page_version_index
            page_frame_index = {}

            # Setup variables for frame association with commit records
            frames = []
            commit_record_number = COMMIT_RECORD_BASE_VERSION_NUMBER

            # Iterate through all of the frames in the write ahead log
            for frame_index in range(len(self._write_ahead_log.frames)):

                # Set the frame
                frame = self._write_ahead_log.frames[frame_index]

                # Make sure the frame index matches the frame
                if frame_index != frame.frame_index:
                    log_message = "Current frame index: {} did not match the expected frame index: {} while parsing " \
                                  "frames for commit record version: {}."
                    log_message = log_message.format(frame_index, frame.frame_index, commit_record_number)
                    logger.error(log_message)
                    raise WalFrameParsingError(log_message)

                # Add the frame to the frames array
                frames.append(frame)

                # Make sure the frame belongs to the commit record we are currently working on creating
                if frame.commit_record_number != commit_record_number:
                    log_message = "Current frame commit record number: {} did not match the expected commit record " \
                                  "number : {}."
                    log_message = log_message.format(frame.commit_record_number, commit_record_number)
                    logger.error(log_message)
                    raise WalFrameParsingError(log_message)

                """

                According to SQLite documentation, the frame with the page size after commit field in the header set
                is the commit frame and therefore all frames before this one (up to the previous one) are considered
                the commit record.  No frames will appear beyond this frame with additional information in this commit
                record.

                """

                # Check if this frame is a commit frame
                if frame.commit_frame:

                    # Create the commit record since we now have all the frames for this commit record
                    commit_record = WriteAheadLogCommitRecord(commit_record_number, self._database,
                                                              self._write_ahead_log, frames, page_frame_index,
                                                              page_version_index, last_database_header,
                                                              last_master_schema,
                                                              store_in_memory=write_ahead_log.store_in_memory,
                                                              strict_format_checking=write_ahead_log.
                                                              strict_format_checking)

                    if commit_record.database_header_modified:
                        last_database_header = commit_record.database_header

                        if not last_database_header:
                            log_message = "Database header was detected as modified for commit record version: {} " \
                                          "but no database header was found."
                            log_message = log_message.format(commit_record_number)
                            logger.error(log_message)
                            raise WalCommitRecordParsingError(log_message)

                    if commit_record.master_schema_modified:
                        last_master_schema = commit_record.master_schema

                        if not last_master_schema:
                            log_message = "Master schema was detected as modified for commit record version: {} " \
                                          "but no master schema was found."
                            log_message = log_message.format(commit_record_number)
                            logger.error(log_message)
                            raise WalCommitRecordParsingError(log_message)

                    # Set the page version and page frame dictionaries variables for the next commit record
                    page_frame_index = commit_record.page_frame_index
                    page_version_index = commit_record.page_version_index

                    self.versions[commit_record_number] = commit_record

                    # Increment the commit record number and clear the frames array (reset to an empty array).
                    commit_record_number += 1
                    frames = []

            # Check if there are remaining frames which indicates the last commit record was not committed
            if len(frames) > 0:

                # Create the commit record
                commit_record = WriteAheadLogCommitRecord(commit_record_number, self._database, self._write_ahead_log,
                                                          frames, page_frame_index, page_version_index,
                                                          last_database_header, last_master_schema,
                                                          store_in_memory=write_ahead_log.store_in_memory,
                                                          strict_format_checking=write_ahead_log.strict_format_checking)

                """

                Note:  We do not need to worry about setting the last database header or last master schema here.  We
                       also do not need to worry about setting the page frame index or page version index.

                """

                self.versions[commit_record_number] = commit_record

                """

                Since we have not seen use cases where the write ahead log file has had additional frames beyond the
                last frame that was a commit frame, we throw a warning here since this use case could result in
                adverse logic.

                """

                log_message = "Version (commit record): {} has additional frames beyond the last commit frame found " \
                              "in the write ahead log and erroneous use cases may occur when parsing."
                log_message = log_message.format(commit_record_number)
                logger.warn(log_message)
                warn(log_message, RuntimeWarning)

        # Set the number of versions
        self.number_of_versions = len(self.versions)

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_versions=True):
        string = "File Type: {}"
        string = string.format(self.number_of_versions)
        if print_versions:
            for version in self.versions:
                string += "\n" + padding + "Page:\n{}".format(version.stringify(padding + "\t"))
        return string


class VersionHistoryParser(VersionParser):

    def __init__(self, version_history, master_schema_entry,
                 version_number=None, ending_version_number=None, signature=None, carve_freelist_pages=False):

        """



        Note:  The updated cells currently only apply to table leaf pages (table master schema entries that are not
               "without rowid" tables).  Therefore no index pages will have updated cells.  This is due to the fact
               that updates are determined off of the row id at the moment which is only available in the b-tree table
               pages.  However, it is important to note that even if the row id is the same and that cell is determined
               to have been updated by this process, there is still a chance that it is not an offset and has error
               in this assumption.  Additional checking may need to be done into the file offsets and/or primary keys.
               (Although file offsets can change as well as page numbers on updates or vacuuming.)  Investigation needs
               to be done more into table pages as well as how to determine updates for index pages.

        Note:  If there are duplicate entries found in consecutive versions (ie. entries that did not change), those
               will be left out and only reported in the first version they are found ("added").  The first version
               to be parsed, whether that be the base version or one of the commit records, will have all entries
               considered "added" and no deleted or updated entries.

        :param version_history:
        :param master_schema_entry:
        :param version_number:
        :param ending_version_number:

        :return:

        :raise:

        """

        # Call to the super class
        super(VersionHistoryParser, self).__init__(version_history, master_schema_entry,
                                                   version_number, ending_version_number)

        logger = getLogger(LOGGER_NAME)

        self._versions = version_history.versions

        log_message = "Creating version history parser for master schema entry with name: {} table name: {} " \
                      "row type: {} and sql: {} for version number: {} and ending version number: {}."
        log_message = log_message.format(self.name, self.table_name, self.row_type, self.sql,
                                         self.parser_starting_version_number, self.parser_ending_version_number)
        logger.debug(log_message)

        self._virtual_table = isinstance(master_schema_entry, VirtualTableRow)

        if signature:

            if signature.name != self.name:
                log_message = "Invalid signature name: {} for version history parser on master schema entry name: {}."
                log_message = log_message.format(signature.name, self.name)
                logger.error(log_message)
                raise ValueError(log_message)

            if signature.row_type != self.row_type:
                log_message = "Invalid signature row type: {} for signature name: {} for version history parser on " \
                              "master schema entry name: {} and row type: {}."
                log_message = log_message.format(signature.row_type, signature.name, self.name, self.row_type)
                logger.error(log_message)
                raise ValueError(log_message)

            if signature.row_type != MASTER_SCHEMA_ROW_TYPE.TABLE:
                log_message = "Not carving version history parser for master schema entry with name: {} table " \
                              "name: {} row type: {} and sql: {} for version number: {} and ending version number: " \
                              "{} since the row type is not a {} type but: {}."
                log_message = log_message.format(self.name, self.table_name, self.row_type, self.sql,
                                                 self.parser_starting_version_number, self.parser_ending_version_number,
                                                 MASTER_SCHEMA_ROW_TYPE.TABLE, signature.row_type)
                logger.warn(log_message)
                warn(log_message, RuntimeWarning)

        # Set the signature
        self.signature = signature

        self.carve_freelist_pages = carve_freelist_pages

        if self.carve_freelist_pages and not self.signature:
            log_message = "Carve freelist pages set with no signature defined.  A signatures is needed in order to " \
                          "carve freelist pages for master schema entry with name: {} table name: {} row type: {} " \
                          "and sql: {} for version number: {} and ending version number: {}."
            log_message = log_message.format(self.name, self.table_name, self.row_type, self.sql,
                                             self.parser_starting_version_number, self.parser_ending_version_number)
            logger.error(log_message)
            raise ValueError(log_message)

    def __iter__(self):
        if self.row_type not in [MASTER_SCHEMA_ROW_TYPE.TABLE, MASTER_SCHEMA_ROW_TYPE.INDEX]:
            # Return an empty iterator
            return iter([])
        elif self._virtual_table:

            """

            In the case this is a virtual table, we check to see if the root page is 0.  Additional use cases
            handling for virtual tables needs to be investigated.  For now, if a virtual table exists with a 
            root page of 0 we do not iterate through it and return a StopIteration() since we do not have anything
            to iterate.  We do throw a warning here (again) for informative purposes.

            Note:  If all root page numbers in the root page number version index are not 0, an exception is raised.

            """

            # Check to make sure all root page numbers are 0 as should be with virtual tables.
            if not all(root_page_number == 0 for root_page_number in self.root_page_number_version_index.values()):
                log_message = "Virtual table found with root page version index: {} where all root page numbers " \
                              "are not equal to 0 in version history parser for master schema entry with " \
                              "name: {} table name: {} row type: {} and sql: {} for version number: {} " \
                              "and ending version number: {}."
                log_message = log_message.format(self.root_page_number_version_index,
                                                 self.name, self.table_name, self.row_type, self.sql,
                                                 self.parser_starting_version_number,
                                                 self.parser_ending_version_number)
                getLogger(LOGGER_NAME).error(log_message)
                raise ValueError(log_message)

            log_message = "Virtual table found with root page 0 for in version history parser for master schema " \
                          "entry with name: {} table name: {} row type: {} and sql: {} for version number: {} " \
                          "and ending version number: {}.  An iterator will not be returned since there " \
                          "is no content."
            log_message = log_message.format(self.name, self.table_name, self.row_type, self.sql,
                                             self.parser_starting_version_number, self.parser_ending_version_number)
            getLogger(LOGGER_NAME).warn(log_message)
            warn(log_message, RuntimeWarning)

            # Return an empty iterator
            return iter([])

        elif self.parser_starting_version_number is not None and self.parser_ending_version_number is not None:
            return self.VersionParserIterator(self.name, self._versions, self.page_type,
                                              self.parser_starting_version_number, self.parser_ending_version_number,
                                              self.root_page_number_version_index,
                                              self.signature, self.carve_freelist_pages)
        else:
            # Return an empty iterator
            return iter([])

    def stringify(self, padding="", print_cells=True):
        string = ""
        for commit in self:
            string += "\n" + padding + "Commit:\n{}".format(commit.stringify(padding + "\t", print_cells))
        return super(VersionHistoryParser, self).stringify(padding) + string

    class VersionParserIterator(object):

        """



        Note:  See VersionHistoryParser class documentation regarding entries returned from this iterator
               (specifically on updates).

        """

        def __init__(self, name, versions, page_type, parser_starting_version_number, parser_ending_version_number,
                     root_page_number_version_index, signature=None, carve_freelist_pages=False):
            self._name = name
            self._versions = versions
            self._page_type = page_type
            self._parser_starting_version_number = parser_starting_version_number
            self._parser_ending_version_number = parser_ending_version_number
            self._root_page_number_version_index = root_page_number_version_index

            # Set the signature
            self._signature = signature

            self._carve_freelist_pages = carve_freelist_pages

            # Initialize the current cells
            self._current_cells = {}

            # Initialize the carved cell md5 hex digests
            self._carved_cell_md5_hex_digests = []

            # Initialize the current b-tree page numbers
            self._current_b_tree_page_numbers = []

            self._current_version_number = self._parser_starting_version_number

        def __iter__(self):
            return self

        def __repr__(self):
            return self.__str__().encode("hex")

        def __str__(self):
            return sub("\t", "", sub("\n", " ", self.stringify()))

        def stringify(self, padding="", print_cells=True):
            string = padding + "Page Type: {}\n" \
                     + padding + "Parser Starting Version Number: {}\n" \
                     + padding + "Parser Ending Version Number: {}\n" \
                     + padding + "Root Page Number Version Index: {}\n" \
                     + padding + "Current Version Number: {}\n" \
                     + padding + "Current B-Tree Page Numbers: {}\n" \
                     + padding + "Carve Freelist Pages: {}"
            string = string.format(self._page_type,
                                   self._parser_starting_version_number,
                                   self._parser_ending_version_number,
                                   self._current_version_number,
                                   self._current_b_tree_page_numbers,
                                   self._carve_freelist_pages)
            if print_cells:
                for current_cell in self._current_cells.itervalues():
                    string += "\n" + padding + "Cell:\n{}".format(current_cell.stringify(padding + "\t"))
            return string

        def next(self):

            if self._current_version_number <= self._parser_ending_version_number:

                version = self._versions[self._current_version_number]
                root_page_number = self._root_page_number_version_index[self._current_version_number]

                # Create the commit object
                commit = Commit(self._name, version.file_type, self._current_version_number,
                                version.database_text_encoding, self._page_type, root_page_number,self._current_b_tree_page_numbers)

                b_tree_updated = False

                # Check if this is the first version to be investigated
                if self._current_version_number == self._parser_starting_version_number:
                    b_tree_updated = True

                # Check if the root page number changed
                elif root_page_number != self._root_page_number_version_index[self._current_version_number - 1]:
                    b_tree_updated = True

                # Check if any of the pages changed (other than the root page specifically here)
                elif [page_number for page_number in self._current_b_tree_page_numbers
                      if page_number in version.updated_b_tree_page_numbers]:
                    b_tree_updated = True

                # Parse the b-tree page structure if it was updated
                if b_tree_updated:

                    # Get the root page and root page numbers from the first version
                    root_page = version.get_b_tree_root_page(root_page_number)
                    b_tree_pages = get_pages_from_b_tree_page(root_page)
                    self._current_b_tree_page_numbers = [b_tree_page.number for b_tree_page in b_tree_pages]

                    # Update the b-tree page numbers in the commit record
                    commit.b_tree_page_numbers = self._current_b_tree_page_numbers

                    updated_b_tree_page_numbers = [page_number for page_number in self._current_b_tree_page_numbers
                                                   if page_number in version.updated_b_tree_page_numbers]

                    # Set the updated b-tree page numbers in the commit object
                    commit.updated_b_tree_page_numbers = updated_b_tree_page_numbers

                    """

                    Below we aggregate the cells together.  This function returns the total of cells and then
                    a dictionary of cells indexed by their cell md5 hex digest to record.  Here, we do not
                    want to ignore any entries since we want to be able to obtain those that were added along
                    with cells that were deleted and/or updated.  Therefore, the total should match the length
                    of the cells returned.

                    """

                    total, cells = aggregate_leaf_cells(root_page)

                    if total != len(cells):
                        log_message = "The total aggregated leaf cells: {} does not match the length of the " \
                                      "cells parsed: {} for version: {} of page type: {} iterating between versions " \
                                      "{} and {} over b-tree page numbers: {} with updated b-tree pages: {}."
                        log_message = log_message.format(total, len(cells), self._current_version_number,
                                                         self._page_type, self._parser_starting_version_number,
                                                         self._parser_ending_version_number,
                                                         self._current_b_tree_page_numbers,
                                                         updated_b_tree_page_numbers)
                        getLogger(LOGGER_NAME).error(log_message)
                        raise VersionParsingError(log_message)

                    """

                    Go through the cells and determine which cells have been added, deleted, and/or updated.

                    """

                    # Copy the newly found cells to a new dictionary
                    added_cells = dict.copy(cells)

                    # Initialize the deleted cells
                    deleted_cells = {}

                    # Iterate through the current cells
                    for current_cell_md5, current_cell in self._current_cells.iteritems():

                        # Remove the cell from the added cells if it was already pre-existing
                        if current_cell_md5 in added_cells:
                            del added_cells[current_cell_md5]

                        # The cell was in the previously current cells but now deleted
                        else:
                            deleted_cells[current_cell_md5] = current_cell

                    # Set the current cells to this versions cells
                    self._current_cells = cells

                    """

                    At this point we have the following two dictionaries:
                    added_cells:    All of the cells that were found to be new in this version for this table/index.
                    deleted_cells:  All of the cells that were found to be deleted in this version for this table/index.

                    The current cells are set back to the cells for future version iterations to compare against.  This
                    is set to the whole dictionary of cells and not the added cells since pre-existing cells can carry
                    over into consecutive versions.

                    """

                    if self._page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

                        # Organize a added cells dictionary keyed off of row id
                        added_cells_by_row_id = {added_cell.row_id: added_cell for added_cell in added_cells.values()}

                        # Get the row ids of the cells that were updated by checking against the deleted cells
                        updated_cell_row_ids = [deleted_cell.row_id for deleted_cell in deleted_cells.values()
                                                if deleted_cell.row_id in added_cells_by_row_id]

                        # Get the cells that might possibly have been updated by comparing the row ids
                        updated_cells = {updated_cell.md5_hex_digest: updated_cell
                                         for updated_cell in added_cells.values()
                                         if updated_cell.row_id in updated_cell_row_ids}

                        # Update the deleted cells to remove any possibly updated cells just determined
                        deleted_cells = {deleted_cell.md5_hex_digest: deleted_cell
                                         for deleted_cell in deleted_cells.values()
                                         if deleted_cell.row_id not in updated_cell_row_ids}

                        # Before we can set the added cells, we need to remove the updated cells detected above
                        added_cells = {added_cell.md5_hex_digest: added_cell
                                       for added_cell in added_cells.values()
                                       if added_cell.md5_hex_digest not in updated_cells}

                        # Set the added, updated, and deleted cells
                        commit.added_cells = added_cells
                        commit.updated_cells = updated_cells
                        commit.deleted_cells = deleted_cells

                        """

                        Right now we only carve if the signature is specified and only from pages that were updated in
                        this particular b-tree in this version.

                        Note:  Once index page carving is implemented this section will need to be updated to correctly
                               address it.

                        """

                        if self._signature:
                            log_message = "Carving table master schema entry name: {} for page type: {} for version: " \
                                          "{} with root page: {} between versions {} and {} over b-tree page " \
                                          "numbers: {} with updated b-tree pages: {}."
                            log_message = log_message.format(self._signature.name, self._page_type,
                                                             self._current_version_number,
                                                             root_page_number, self._parser_starting_version_number,
                                                             self._parser_ending_version_number,
                                                             self._current_b_tree_page_numbers,
                                                             updated_b_tree_page_numbers)
                            getLogger(LOGGER_NAME).debug(log_message)

                            # Initialize the carved cells
                            carved_cells = []

                            b_tree_pages_by_number = {b_tree_page.number: b_tree_page for b_tree_page in b_tree_pages}

                            for updated_b_tree_page_number in updated_b_tree_page_numbers:

                                page = b_tree_pages_by_number[updated_b_tree_page_number]

                                # For carving freeblocks make sure the page is a b-tree page and not overflow
                                if isinstance(page, BTreePage):
                                    carvings = SignatureCarver.carve_freeblocks(version, CELL_SOURCE.B_TREE,
                                                                                page.freeblocks, self._signature)
                                    carved_cells.extend(carvings)

                                # Carve unallocated space
                                carvings = SignatureCarver.carve_unallocated_space(version, CELL_SOURCE.B_TREE,
                                                                                   updated_b_tree_page_number,
                                                                                   page.unallocated_space_start_offset,
                                                                                   page.unallocated_space,
                                                                                   self._signature)
                                carved_cells.extend(carvings)

                            # Remove all carved cells that may be duplicates from previous version carvings
                            carved_cells = {carved_cell.md5_hex_digest: carved_cell for carved_cell in carved_cells
                                            if carved_cell.md5_hex_digest not in self._carved_cell_md5_hex_digests}

                            # Update the carved cells in the commit object
                            commit.carved_cells.update(carved_cells)

                            # Update the carved cell md5 hex digests
                            self._carved_cell_md5_hex_digests.extend([cell_md5_hex_digest
                                                                      for cell_md5_hex_digest in carved_cells.keys()])

                    elif self._page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

                        # Set the added cells
                        commit.added_cells = added_cells

                        # As noted above, we will not define updates for index cells yet so just set the deleted cells
                        commit.deleted_cells = deleted_cells

                    else:
                        log_message = "Invalid page type: {} found for version: {} iterating between versions {} " \
                                      "and {} over b-tree page numbers: {} with updated b-tree pages: {}."
                        log_message = log_message.format(self._page_type, self._current_version_number,
                                                         self._parser_starting_version_number,
                                                         self._parser_ending_version_number,
                                                         self._current_b_tree_page_numbers,
                                                         updated_b_tree_page_numbers)
                        getLogger(LOGGER_NAME).error(log_message)
                        raise VersionParsingError(log_message)

                """

                Note:  The outer class checks on if the signature is defined in relation to the carving of freelist
                       pages being set and handles it accordingly.  Here we can assume that the signature is defined
                       if we are carving freelist pages.

                """

                # See if we are also
                if self._carve_freelist_pages:

                    freelist_pages_updated = False

                    # Check if this is the first version to be investigated
                    if self._current_version_number == self._parser_starting_version_number:
                        freelist_pages_updated = True

                    # Check if the freelist pages were modified in this version
                    elif version.freelist_pages_modified:
                        freelist_pages_updated = True

                    # Carve the freelist pages if any were updated
                    if freelist_pages_updated:

                        """

                        Note:  We only have to worry about caring the B_TREE_TABLE_LEAF pages right now since this is
                               the only page really supported in carving so far.  The super class already prints the
                               needed warnings that carving will not occur if it is an B_TREE_INDEX_LEAF page.

                        Note:  As also stated above the signature by this point will be set.

                        """

                        if self._page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

                            # Populate the updated freelist pages into a dictionary keyed by page number
                            updated_freelist_pages = {}
                            freelist_trunk_page = version.first_freelist_trunk_page
                            while freelist_trunk_page:
                                if freelist_trunk_page.number in version.updated_page_numbers:
                                    updated_freelist_pages[freelist_trunk_page.number] = freelist_trunk_page
                                for freelist_leaf_page in freelist_trunk_page.freelist_leaf_pages:
                                    if freelist_leaf_page.number in version.updated_page_numbers:
                                        updated_freelist_pages[freelist_leaf_page.number] = freelist_leaf_page
                                freelist_trunk_page = freelist_trunk_page.next_freelist_trunk_page

                            # Update the commit object
                            commit.freelist_pages_carved = True
                            commit.updated_freelist_page_numbers = updated_freelist_pages.keys()

                            log_message = "Carving freelist pages for table master schema entry name: {} " \
                                          "for page type: {} for version: {} with root page: {} between versions {} " \
                                          "and {} over updated freelist pages: {}."
                            log_message = log_message.format(self._signature.name, self._page_type,
                                                             self._current_version_number,
                                                             root_page_number, self._parser_starting_version_number,
                                                             self._parser_ending_version_number,
                                                             updated_freelist_pages.keys())
                            getLogger(LOGGER_NAME).debug(log_message)

                            # Initialize the carved cells
                            carved_cells = []

                            for freelist_page_number, freelist_page in updated_freelist_pages.iteritems():

                                # Carve unallocated space
                                carvings = SignatureCarver.carve_unallocated_space(version, CELL_SOURCE.FREELIST,
                                                                                   freelist_page_number,
                                                                                   freelist_page.
                                                                                   unallocated_space_start_offset,
                                                                                   freelist_page.unallocated_space,
                                                                                   self._signature)

                                carved_cells.extend(carvings)

                            # Remove all carved cells that may be duplicates from previous version carvings
                            carved_cells = {carved_cell.md5_hex_digest: carved_cell for carved_cell in carved_cells
                                            if carved_cell.md5_hex_digest not in self._carved_cell_md5_hex_digests}

                            # Update the carved cells in the commit object
                            commit.carved_cells.update(carved_cells)

                            # Update the carved cell md5 hex digests
                            self._carved_cell_md5_hex_digests.extend([cell_md5_hex_digest
                                                                     for cell_md5_hex_digest in carved_cells.keys()])

                # Increment the current version number
                self._current_version_number += 1

                # Return the commit object
                return commit

            else:
                raise StopIteration()


class Commit(object):

    def __init__(self, name, file_type, version_number, database_text_encoding, page_type, root_page_number,
                 b_tree_page_numbers, updated_b_tree_page_numbers=None, freelist_pages_carved=False,
                 updated_freelist_page_numbers=None):

        """



        Note:  This may not be updated in the case where carved cells were found, but found to be duplicates of a
               previous commit and therefore removed.

        :param name:
        :param file_type:
        :param version_number:
        :param database_text_encoding:
        :param page_type:
        :param root_page_number:
        :param b_tree_page_numbers:
        :param updated_b_tree_page_numbers:
        :param freelist_pages_carved:
        :param updated_freelist_page_numbers:

        :return:

        """

        self.name = name
        self.file_type = file_type
        self.version_number = version_number
        self.database_text_encoding = database_text_encoding
        self.page_type = page_type
        self.root_page_number = root_page_number

        self.b_tree_page_numbers = b_tree_page_numbers

        self.updated_b_tree_page_numbers = updated_b_tree_page_numbers
        self.freelist_pages_carved = freelist_pages_carved
        self.updated_freelist_page_numbers = updated_freelist_page_numbers
        self.added_cells = {}
        self.deleted_cells = {}
        self.updated_cells = {}
        self.carved_cells = {}

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding="", print_cells=True):
        string = padding + "Version Number: {}\n" \
                 + padding + "Database Text Encoding: {}\n" \
                 + padding + "Page Type: {}\n" \
                 + padding + "Root Page Number: {}\n" \
                 + padding + "B-Tree Page Numbers: {}\n" \
                 + padding + "Updated: {}\n" \
                 + padding + "Updated B-Tree Page Numbers: {}\n" \
                 + padding + "Freelist Pages Carved: {}\n" \
                 + padding + "Updated Freelist Page Numbers: {}\n"
        string = string.format(self.version_number,
                               self.database_text_encoding,
                               self.page_type,
                               self.root_page_number,
                               self.b_tree_page_numbers,
                               self.updated,
                               self.updated_b_tree_page_numbers,
                               self.freelist_pages_carved,
                               self.updated_freelist_page_numbers)
        if print_cells:
            for added_cell in self.added_cells.itervalues():
                string += "\n" + padding + "Added Cell:\n{}".format(added_cell.stringify(padding + "\t"))
            for deleted_cell in self.deleted_cells.itervalues():
                string += "\n" + padding + "Deleted Cell:\n{}".format(deleted_cell.stringify(padding + "\t"))
            for updated_cell in self.updated_cells.itervalues():
                string += "\n" + padding + "Updated Cell:\n{}".format(updated_cell.stringify(padding + "\t"))
            for carved_cell in self.carved_cells.itervalues():
                string += "\n" + padding + "Carved Cell:\n{}".format(carved_cell.stringify(padding + "\t"))
        return string

    @property
    def updated(self):
        return True if (self.added_cells or self.deleted_cells or self.updated_cells or self.carved_cells) else False
