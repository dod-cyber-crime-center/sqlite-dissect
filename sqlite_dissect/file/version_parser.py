from abc import ABCMeta
from logging import getLogger
from re import sub
from warnings import warn
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.exception import VersionParsingError
from sqlite_dissect.file.schema.master import OrdinaryTableRow
from sqlite_dissect.file.schema.master import VirtualTableRow

"""

version_parser.py

This script holds the objects for parsing through the version history for master schema entries.  This can be used
for retrieving cells (records), carving, signature generation, etc..

This script holds the following object(s):
VersionParser(object)

"""


class VersionParser(object):

    __metaclass__ = ABCMeta

    def __init__(self, version_history, master_schema_entry, version_number=None, ending_version_number=None):

        """



        The version history will be iterated through and the respective subclass will use the master schema entry
        parsed from every version where that master schema entry is found.  The version numbers where the master schema
        entry is found until the last version it is found in (if applicable) will be set at the parser starting version
        number and parser ending version number.

        In addition, the version number may be set for a specific version to be parsed.  This way if you only want a
        specific version to be parsed, you can specify the version number.  If you want the range between two specific
        versions, the version number and ending version number can be specified to parse the versions in between
        (including the specified version number and ending version number).  If these fields are set the parser
        starting and ending version number will be set accordingly to be within the range of these versions, if
        existing, otherwise None.  If the master schema entry does not exist in between the versions, a warning will
        be raised and the subclass will handle the use case accordingly (either by creating and empty object(s) or a
        "empty" class depending on implementation).

        The md5_hash_identifier field is used from the master schema entry to identify it across the versions.  Due
        to this, it does not matter what master schema entry from what version you choose.  The md5_hash_identifier
        is derived from the row id, name, table name, type, and sql to ensure uniqueness.  (Root page numbers can be
        updated.)

        Note:  The use case where the same master schema entry is removed and re-added needs to be addressed in the wal
               file and is not fully supported here.

        :param version_history:
        :param master_schema_entry:
        :param version_number:
        :param ending_version_number:

        :return:

        :raise:

        """

        logger = getLogger(LOGGER_NAME)

        if version_number is None and ending_version_number:
            log_message = "Version number not specified where ending version number was specified as: {} for " \
                          "master schema entry with root page number: {} row type: {} name: {} table name: {} " \
                          "and sql: {}."
            log_message = log_message.format(ending_version_number, master_schema_entry.root_page_number,
                                             master_schema_entry.row_type, master_schema_entry.name,
                                             master_schema_entry.table_name, master_schema_entry.sql)
            logger.error(log_message)
            raise ValueError(log_message)

        if version_number is not None and version_number == ending_version_number:
            log_message = "Version number: {} specified where ending version number was also specified as: {} for " \
                          "master schema entry with root page number: {} row type: {} name: {} table name: {} and " \
                          "sql: {}."
            log_message = log_message.format(version_number, ending_version_number,
                                             master_schema_entry.root_page_number, master_schema_entry.row_type,
                                             master_schema_entry.name, master_schema_entry.table_name,
                                             master_schema_entry.sql)
            logger.error(log_message)
            raise ValueError(log_message)

        number_of_versions = version_history.number_of_versions

        """

        The ending version number needs to be less than the number of versions since version numbers start from
        0 and go to the last version.  Therefore, the number of versions will be one greater than the last version
        number.

        """

        if ending_version_number is not None and (ending_version_number >= number_of_versions or
                                                  ending_version_number <= version_number):
            log_message = "Invalid ending version number: {} with {} number of versions with version number: {} for " \
                          "master schema entry with root page number: {} row type: {} name: {} table name: {} " \
                          "and sql: {}."
            log_message = log_message.format(ending_version_number, number_of_versions, version_number,
                                             master_schema_entry.root_page_number, master_schema_entry.row_type,
                                             master_schema_entry.name, master_schema_entry.table_name,
                                             master_schema_entry.sql)
            logger.error(log_message)
            raise ValueError(log_message)

        self.version_number = version_number
        self.ending_version_number = ending_version_number

        self.parser_starting_version_number = version_number if version_number is not None else BASE_VERSION_NUMBER
        self.parser_ending_version_number = ending_version_number \
            if ending_version_number is not None else number_of_versions - 1

        """

        According to the sqlite documentation the only pages with a root page are table and index types (excluding
        virtual tables.)  Therefore we can only parse cells from these types.  In the case that trigger or
        view master schema entry row types were specified we raise a warning here.  This will result in having a
        no entries to parse through.

        Note:  Support for virtual table modules that may or may not have database b-tree pages need to be accounted
               for.  A warning will be displayed if a virtual table is encountered.

        Note: Support for "without rowid" tables are not accounted for properly.  For now, a warning will be displayed.

        """

        if master_schema_entry.row_type not in [MASTER_SCHEMA_ROW_TYPE.TABLE, MASTER_SCHEMA_ROW_TYPE.INDEX]:
            log_message = "Invalid master schema entry row type: {} for master schema entry with root page " \
                          "number: {} name: {} table name: {} and sql: {}.  Only table and index master " \
                          "schema entries have associated cells to be parsed."
            log_message = log_message.format(master_schema_entry.row_type, master_schema_entry.root_page_number,
                                             master_schema_entry.name, master_schema_entry.table_name,
                                             master_schema_entry.sql)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        # Set the page type and update it as appropriate
        self.page_type = PAGE_TYPE.B_TREE_TABLE_LEAF

        if isinstance(master_schema_entry, VirtualTableRow):
            log_message = "A virtual table row type was found for the version parser which is not fully supported " \
                          "for master schema entry root page number: {} type: {} name: {} table name: {} and sql: {}."
            log_message = log_message.format(master_schema_entry.root_page_number,
                                             master_schema_entry.row_type, master_schema_entry.name,
                                             master_schema_entry.table_name, master_schema_entry.sql)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

        elif isinstance(master_schema_entry, OrdinaryTableRow) and master_schema_entry.without_row_id:
            log_message = "A \"without rowid\" table row type was found for the version parser which is not " \
                          "supported for master schema entry root page number: {} row type: {} name: {} " \
                          "table name: {} and sql: {}.  Erroneous cells may be generated."
            log_message = log_message.format(master_schema_entry.root_page_number,
                                             master_schema_entry.row_type, master_schema_entry.name,
                                             master_schema_entry.table_name, master_schema_entry.sql)
            logger.warn(log_message)
            warn(log_message, RuntimeWarning)

            self.page_type = PAGE_TYPE.B_TREE_INDEX_LEAF

        # Set the page type if the master schema row type is a index
        if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.INDEX:
            self.page_type = PAGE_TYPE.B_TREE_INDEX_LEAF

        """

        Set the master schema entry fields we care about in this class.  Since root page numbers can be different
        depending on versions, root page numbers is a dictionary in the form of:
        root_page_number_version_index[VERSION_NUMBER] = ROOT_PAGE_NUMBER(VERSION)

        """

        self.row_type = master_schema_entry.row_type
        self.name = master_schema_entry.name
        self.table_name = master_schema_entry.table_name
        self.sql = master_schema_entry.sql
        self.root_page_number_version_index = {}

        # Get the md5_hash_identifier from the master schema entry
        self.master_schema_entry_md5_hash_identifier = master_schema_entry.md5_hash_identifier

        """

        Setup the version numbers to parse through for the version history.

        Note:  If the master schema entry is either not found, or stops being found and then re-found, a warning will
               be raised.  The master schema entry uniqueness is determined by the master schema entry md5 hash
               identifier from the MasterSchemaRow class.

        """

        versions = version_history.versions
        starting_version_number = None
        ending_version_number = None
        for version_number in range(self.parser_starting_version_number, self.parser_ending_version_number + 1):

            version = versions[version_number]

            if version.master_schema_modified:
                master_schema = version.master_schema
            else:
                master_schema = version.last_master_schema

            if not master_schema:
                log_message = "Master schema was unable to be found in starting version number: {} while parsing " \
                              "the version history for master schema entry with name: {} table name: {} " \
                              "row type: {} and sql: {} for version number: {} and ending version number: {}."
                log_message = log_message.format(version_number, self.name, self.table_name, self.row_type, self.sql,
                                                 self.parser_starting_version_number,
                                                 self.parser_ending_version_number)
                logger.error(log_message)
                raise VersionParsingError(log_message)

            entries = master_schema.master_schema_entries
            entries_dictionary = dict(map(lambda entry: [entry.md5_hash_identifier, entry], entries))

            if self.master_schema_entry_md5_hash_identifier in entries_dictionary:

                if ending_version_number is None:

                    if starting_version_number is not None:
                        log_message = "The starting version number was set already when it should not have been " \
                                      "since the ending version number was still not set for master schema entry " \
                                      "row type: {} with root page number: {} name: {} table name: {} and sql: {}."
                        log_message = log_message.format(master_schema_entry.row_type,
                                                         master_schema_entry.root_page_number, master_schema_entry.name,
                                                         master_schema_entry.table_name, master_schema_entry.sql)
                        logger.error(log_message)
                        raise VersionParsingError(log_message)

                    starting_version_number = version_number
                    ending_version_number = version_number

                    if self.root_page_number_version_index:
                        log_message = "The root page number version index has already been populated with values " \
                                      "when it should not have been for master schema entry row type: {} with root " \
                                      "page number: {} name: {} table name: {} and sql: {}."
                        log_message = log_message.format(master_schema_entry.row_type,
                                                         master_schema_entry.root_page_number, master_schema_entry.name,
                                                         master_schema_entry.table_name, master_schema_entry.sql)
                        logger.error(log_message)
                        raise VersionParsingError(log_message)

                    # Add the first version number and b-tree root page number into the root page number version index
                    root_page_number = entries_dictionary[self.master_schema_entry_md5_hash_identifier].root_page_number
                    self.root_page_number_version_index[version_number] = root_page_number

                elif ending_version_number == version_number - 1:
                    ending_version_number = version_number

                    if not self.root_page_number_version_index:
                        log_message = "The root page number version index has not already been populated with values " \
                                      "when it should have been for master schema entry row type: {} with root " \
                                      "page number: {} name: {} table name: {} and sql: {}."
                        log_message = log_message.format(master_schema_entry.row_type,
                                                         master_schema_entry.root_page_number, master_schema_entry.name,
                                                         master_schema_entry.table_name, master_schema_entry.sql)
                        logger.error(log_message)
                        raise VersionParsingError(log_message)

                    # Add the version number and b-tree root page number into the root page number version index
                    root_page_number = entries_dictionary[self.master_schema_entry_md5_hash_identifier].root_page_number
                    self.root_page_number_version_index[version_number] = root_page_number

                else:
                    log_message = "Version number: {} did not have a master schema entry for the previous " \
                                  "version number for master schema entry with name: {} table name: {} " \
                                  "row type: {} and sql: {} for version number: {} and ending version number: {}."
                    log_message = log_message.format(version_number,  self.name, self.table_name, self.row_type,
                                                     self.sql, self.parser_starting_version_number,
                                                     self.parser_ending_version_number)
                    logger.warn(log_message)
                    warn(log_message, RuntimeWarning)

            if starting_version_number is None and ending_version_number is None:
                log_message = "Was unable to find any matching schema entries between version numbers {} " \
                              "and {}.  The version parser will not parse anything for master schema entry with " \
                              "name: {} table name: {} row type: {} and sql: {}."
                log_message = log_message.format(self.parser_starting_version_number,
                                                 self.parser_ending_version_number,  self.name, self.table_name,
                                                 self.row_type, self.sql)
                logger.warn(log_message)
                warn(log_message, RuntimeWarning)

            self.parser_starting_version_number = starting_version_number
            self.parser_ending_version_number = ending_version_number

        """

        We now have the parser starting and ending version numbers that we need to parse between and a root
        page number version index referring to each version and it's root b-tree page in case it was updated.

        Note: The root pages to the master schema entries are generated on demand from the version which will return
              the b-tree page if it is already in memory, or parse it and then return it if it is not.  Versions can
              either be stored in memory or read out on demand for b-tree pages.  This is allowed for conserving 
              memory and speeding up parsing (so each b-tree page does not need to be parsed in the case where
              they do not change).

        """

    def __repr__(self):
        return self.__str__().encode("hex")

    def __str__(self):
        return sub("\t", "", sub("\n", " ", self.stringify()))

    def stringify(self, padding=""):
        string = padding + "Row Type: {}\n" \
                 + padding + "Page Type: {}\n" \
                 + padding + "Name: {}\n" \
                 + padding + "Table Name: {}\n" \
                 + padding + "SQL: {}\n" \
                 + padding + "Root Page Number Version Index: {}\n" \
                 + padding + "Master Schema Entry MD5 Hash Identifier: {}\n" \
                 + padding + "Version Number: {}\n" \
                 + padding + "Ending Version Number: {}\n" \
                 + padding + "Parser Starting Version Number: {}\n" \
                 + padding + "Parser Ending Version Number: {}"
        string = string.format(self.row_type,
                               self.page_type,
                               self.name,
                               self.table_name,
                               self.sql,
                               self.root_page_number_version_index,
                               self.master_schema_entry_md5_hash_identifier,
                               self.version_number,
                               self.ending_version_number,
                               self.parser_starting_version_number,
                               self.parser_ending_version_number)
        return string
