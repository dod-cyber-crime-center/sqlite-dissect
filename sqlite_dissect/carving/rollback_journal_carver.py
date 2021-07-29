from binascii import hexlify
from logging import getLogger
from struct import unpack
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.carving.carver import SignatureCarver
from sqlite_dissect.version_history import Commit

"""

rollback_journal_carver.py

This script carves through a rollback journal file with the specified master schema entry and 
signature and returns the entries.

This script holds the following object(s):
RollBackJournalCarver(Carver)

"""


class RollBackJournalCarver(object):

    @staticmethod
    def carve(rollback_journal, version, master_schema_entry, signature):

        logger = getLogger(LOGGER_NAME)

        """
        
        Read the page size in from the version class (the base SQLite database).  This will be used instead of checking
        the journal header since that is overwritten with zeros in most cases.  If there is no database file, then
        other means to determine the page size can be used by analyzing the journal file.  This is something outside
        the current scope of this project and could be something followed up on in the future for stand alone rollback
        journal carving.
        
        """

        page_size = version.page_size

        """
        
        This is currently a hard coded value as to what is currently seen (sector size).
        Some research was done and this value appeared to be hard coded in the SQLite c library.
        Newer version so the library should be checked as to this was the 3090200 version.
        
        """

        sector_size = 512

        # The page record header and checksum sizes are fixed
        page_record_header_size = 4
        page_record_checksum_size = 4

        page_record_size = page_record_header_size + page_size + page_record_checksum_size

        # Initialize the carve commits
        carved_commits = []

        logger.debug("Starting carving table: %s... " % master_schema_entry.name)

        has_data = True
        offset = sector_size
        while has_data:

            page_number = unpack(b">I", rollback_journal.file_handle.read_data(offset, page_record_header_size))[0]
            page_content = rollback_journal.file_handle.read_data(offset + page_record_header_size, page_size)
            page_type = hexlify(page_content[:1])
            page_checksum = hexlify(rollback_journal.file_handle.read_data(offset + page_record_header_size +
                                                                           page_size, page_record_checksum_size))

            logger.debug("At offset: %s page Number: %s of type: %s has content with checksum of: %s"
                         % (offset, page_number, page_type, page_checksum))

            if page_type in ["0d", "05"]:

                page_type_string = PAGE_TYPE.B_TREE_TABLE_LEAF if page_type == "0d" else PAGE_TYPE.B_TREE_TABLE_INTERIOR
                carved_cells = SignatureCarver.carve_unallocated_space(version, FILE_TYPE.ROLLBACK_JOURNAL, page_number,
                                                                       0, page_content, signature,
                                                                       offset + page_record_header_size)

                commit = Commit(master_schema_entry.name, FILE_TYPE.ROLLBACK_JOURNAL, -1,
                                version.database_text_encoding, page_type_string, -1, None)
                commit.carved_cells.update({cell.md5_hex_digest: cell for cell in carved_cells})
                carved_commits.append(commit)

            offset += page_record_size

            # Check if the next page record is a full page record size or not
            if (offset + page_record_size) >= rollback_journal.file_handle.file_size:

                # The page record is cut off since it is goes beyond the end of the file
                has_data = False

                """
                
                This accounts for the last incomplete block/frame of the journal file for carving.

                Since this isn't a full page record, we do not care about the checksum since it should be cut off.
                
                """

                page_number = unpack(b">I", rollback_journal.file_handle.read_data(offset, 4))[0]
                page_content = rollback_journal.file_handle.read_data(offset + page_record_header_size,
                                                                      rollback_journal.file_handle.file_size -
                                                                      page_record_header_size - offset)
                page_type = hexlify(page_content[:1])

                if page_type in ["0d", "05"]:

                    page_type_string = PAGE_TYPE.B_TREE_TABLE_LEAF if page_type == "0d" \
                        else PAGE_TYPE.B_TREE_TABLE_INTERIOR
                    carved_cells = SignatureCarver.carve_unallocated_space(version, FILE_TYPE.ROLLBACK_JOURNAL,
                                                                           page_number, 0, page_content, signature,
                                                                           offset + page_record_header_size)

                    commit = Commit(master_schema_entry.name, FILE_TYPE.ROLLBACK_JOURNAL, -1,
                                    version.database_text_encoding, page_type_string, -1, None)
                    commit.carved_cells.update({cell.md5_hex_digest: cell for cell in carved_cells})
                    carved_commits.append(commit)

        logger.debug("Finished carving table: %s... " % master_schema_entry.name)
        return carved_commits
