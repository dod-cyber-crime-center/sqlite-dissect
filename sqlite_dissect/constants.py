from collections import MutableMapping
from logging import getLogger
from re import compile
from sys import maxunicode

"""

constants.py

This script holds constants defined for reference by the sqlite carving library.  Additionally, a class has been
added to this script for constant enumerations.

This script holds the following object(s):
Enum(MutableMapping)

"""


LOGGER_NAME = "sqlite_dissect"


class Enum(MutableMapping):

    def __init__(self, data):
        if isinstance(data, list):
            self._store = {value: value for value in data}
        elif isinstance(data, dict):
            self._store = data
        else:
            log_message = "Unable to initialize enumeration for: {} with type: {}.".format(data, type(data))
            getLogger(LOGGER_NAME).error(log_message)
            raise ValueError(log_message)

    def __getattr__(self, key):
        return self._store[key]

    def __getitem__(self, key):
        return self._store[key]

    def __setitem__(self, key, value):
        self._store[key] = value

    def __delitem__(self, key):
        del self._store[key]

    def __contains__(self, key):
        return True if key in self._store else False

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)


UTF_8 = "utf-8"
UTF_16BE = "utf-16-be"
UTF_16LE = "utf-16-le"

ENDIANNESS = Enum(["BIG_ENDIAN", "LITTLE_ENDIAN"])

# Supported file types
FILE_TYPE = Enum(["DATABASE", "WAL", "WAL_INDEX", "ROLLBACK_JOURNAL"])

SQLITE_3_7_0_VERSION_NUMBER = 3007000

PAGE_TYPE_LENGTH = 1

MASTER_PAGE_HEX_ID = b'\x53'
TABLE_LEAF_PAGE_HEX_ID = b'\x0d'
TABLE_INTERIOR_PAGE_HEX_ID = b'\x05'
INDEX_LEAF_PAGE_HEX_ID = b'\x0a'
INDEX_INTERIOR_PAGE_HEX_ID = b'\x02'

PAGE_TYPE = Enum(["LOCK_BYTE", "FREELIST_TRUNK", "FREELIST_LEAF", "B_TREE_TABLE_INTERIOR", "B_TREE_TABLE_LEAF",
                  "B_TREE_INDEX_INTERIOR", "B_TREE_INDEX_LEAF", "OVERFLOW", "POINTER_MAP"])

LOCK_BYTE_PAGE_START_OFFSET = 1073741824
LOCK_BYTE_PAGE_END_OFFSET = 1073742336

SQLITE_DATABASE_HEADER_LENGTH = 100
MAGIC_HEADER_STRING = "SQLite format 3\000"
MAGIC_HEADER_STRING_ENCODING = UTF_8
MAXIMUM_PAGE_SIZE_INDICATOR = 1
MINIMUM_PAGE_SIZE_LIMIT = 512
MAXIMUM_PAGE_SIZE_LIMIT = 32768
MAXIMUM_PAGE_SIZE = 65536
ROLLBACK_JOURNALING_MODE = 1
WAL_JOURNALING_MODE = 2
HUMAN_READABLE_JOURNALING_MODES = {ROLLBACK_JOURNALING_MODE: "JOURNAL",
                                   WAL_JOURNALING_MODE: "WAL"}
MAXIMUM_EMBEDDED_PAYLOAD_FRACTION = 64
MINIMUM_EMBEDDED_PAYLOAD_FRACTION = 32
LEAF_PAYLOAD_FRACTION = 32
VALID_SCHEMA_FORMATS = [1, 2, 3, 4]
UTF_8_DATABASE_TEXT_ENCODING = 1
UTF_16LE_DATABASE_TEXT_ENCODING = 2
UTF_16BE_DATABASE_TEXT_ENCODING = 3
DATABASE_TEXT_ENCODINGS = [UTF_8_DATABASE_TEXT_ENCODING,
                           UTF_16LE_DATABASE_TEXT_ENCODING,
                           UTF_16BE_DATABASE_TEXT_ENCODING]
HUMAN_READABLE_DATABASE_TEXT_ENCODINGS = {UTF_8_DATABASE_TEXT_ENCODING: "UTF-8",
                                          UTF_16BE_DATABASE_TEXT_ENCODING: "UTF-16be",
                                          UTF_16LE_DATABASE_TEXT_ENCODING: "UTF-16le"}
RESERVED_FOR_EXPANSION_REGEX = "^0{40}$"

FREELIST_NEXT_TRUNK_PAGE_LENGTH = 4
FREELIST_LEAF_PAGE_POINTERS_LENGTH = 4
FREELIST_LEAF_PAGE_NUMBER_LENGTH = 4
FREELIST_HEADER_LENGTH = FREELIST_NEXT_TRUNK_PAGE_LENGTH + FREELIST_LEAF_PAGE_POINTERS_LENGTH  # ptr+num size
LEAF_PAGE_HEADER_LENGTH = 8
INTERIOR_PAGE_HEADER_LENGTH = 12
RIGHT_MOST_POINTER_OFFSET = 8
RIGHT_MOST_POINTER_LENGTH = 4
CELL_POINTER_BYTE_LENGTH = 2
LEFT_CHILD_POINTER_BYTE_LENGTH = 4
FREEBLOCK_HEADER_LENGTH = 4
NEXT_FREEBLOCK_OFFSET_LENGTH = 2
FREEBLOCK_BYTE_LENGTH = 2
PAGE_FRAGMENT_LIMIT = 60
FIRST_OVERFLOW_PAGE_NUMBER_LENGTH = 4
OVERFLOW_HEADER_LENGTH = 4  # This is the next overflow page number but we call it a header here
POINTER_MAP_ENTRY_LENGTH = 5

PAGE_HEADER_MODULE = "sqlite_dissect.file.database.header"
PAGE_MODULE = "sqlite_dissect.file.database.page"
CELL_MODULE = "sqlite_dissect.file.database.page"

INTERIOR_PAGE_HEADER_CLASS = "InteriorPageHeader"
LEAF_PAGE_HEADER_CLASS = "LeafPageHeader"

INDEX_INTERIOR_PAGE_CLASS = "IndexInteriorPage"
INDEX_LEAF_PAGE_CLASS = "IndexLeafPage"
TABLE_INTERIOR_PAGE_CLASS = "TableInteriorPage"
TABLE_LEAF_PAGE_CLASS = "TableLeafPage"
INDEX_INTERIOR_CELL_CLASS = "IndexInteriorCell"
INDEX_LEAF_CELL_CLASS = "IndexLeafCell"
TABLE_INTERIOR_CELL_CLASS = "TableInteriorCell"
TABLE_LEAF_CELL_CLASS = "TableLeafCell"

FIRST_OVERFLOW_PARENT_PAGE_NUMBER = 0
FIRST_OVERFLOW_PAGE_INDEX = 0
FIRST_FREELIST_TRUNK_PARENT_PAGE_NUMBER = 0
FIRST_FREELIST_TRUNK_PAGE_INDEX = 0

CELL_LOCATION = Enum({"ALLOCATED_SPACE": "Allocated Space",
                      "UNALLOCATED_SPACE": "Unallocated Space",
                      "FREEBLOCK": "Freeblock"})

CELL_SOURCE = Enum({"B_TREE": "B-Tree",
                    "DISPARATE_B_TREE": "Disparate B-Tree",
                    "FREELIST": "Freelist"})

BLOB_SIGNATURE_IDENTIFIER = -1
TEXT_SIGNATURE_IDENTIFIER = -2

ZERO_BYTE = b'\x00'
ALL_ZEROS_REGEX = "^0*$"

SQLITE_MASTER_SCHEMA_ROOT_PAGE = 1
MASTER_SCHEMA_COLUMN = Enum({"TYPE": 0, "NAME": 1, "TABLE_NAME": 2, "ROOT_PAGE": 3, "SQL": 4})
MASTER_SCHEMA_ROW_TYPE = Enum({"TABLE": "table", "INDEX": "index", "VIEW": "view", "TRIGGER": "trigger"})
MASTER_SCHEMA_NUMBER_OF_COLUMNS = 5

COLUMN_DEFINITION = Enum(["COLUMN_NAME", "DATA_TYPE_NAME", "COLUMN_CONSTRAINT"])
STORAGE_CLASS = Enum(["NULL", "INTEGER", "REAL", "TEXT", "BLOB"])
TYPE_AFFINITY = Enum(["TEXT", "NUMERIC", "INTEGER", "REAL", "BLOB"])
DATA_TYPE = Enum(["INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
                  "UNSIGNED_BIG_INT", "INT2", "INT8",
                  "CHARACTER_20", "VARCHAR_255", "VARYING_CHARACTER_255", "NCHAR_55",
                  "NATIVE_CHARACTER_70", "NVARCHAR_100", "TEXT", "CLOB",
                  "BLOB", "NOT_SPECIFIED",
                  "REAL", "DOUBLE", "DOUBLE_PRECISION", "FLOAT",
                  "NUMERIC", "DECIMAL_10_5", "BOOLEAN", "DATE", "DATETIME",
                  "INVALID"])

CREATE_TABLE_CLAUSE = "CREATE TABLE"
ORDINARY_TABLE_AS_CLAUSE = "AS"
CREATE_VIRTUAL_TABLE_CLAUSE = "CREATE VIRTUAL TABLE"
VIRTUAL_TABLE_USING_CLAUSE = "USING"

CREATE_INDEX_CLAUSE = "CREATE INDEX"
CREATE_UNIQUE_INDEX_CLAUSE = "CREATE UNIQUE INDEX"
INDEX_ON_COMMAND = "ON"
INDEX_WHERE_CLAUSE = "WHERE"

INTERNAL_SCHEMA_OBJECT_PREFIX = "sqlite_"
INTERNAL_SCHEMA_OBJECT_INDEX_PREFIX = "sqlite_autoindex_"

COLUMN_CONSTRAINT_TYPES = Enum(["PRIMARY_KEY", "NOT NULL", "UNIQUE", "CHECK", "DEFAULT",
                                "COLLATE", "FOREIGN_KEY"])

COLUMN_CONSTRAINT_PREFACES = ["CONSTRAINT", "PRIMARY", "NOT", "UNIQUE", "CHECK", "DEFAULT", "COLLATE", "REFERENCES"]
TABLE_CONSTRAINT_PREFACES = ["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"]

"""

Note:  For TABLE_CONSTRAINT_TYPE, the PRIMARY_KEY and UNIQUE should be handled the same in respect to this library.

"""

TABLE_CONSTRAINT_TYPES = Enum(["PRIMARY_KEY", "UNIQUE", "CHECK", "FOREIGN_KEY"])

POINTER_MAP_B_TREE_ROOT_PAGE_TYPE = b'\x01'
POINTER_MAP_FREELIST_PAGE_TYPE = b'\x02'
POINTER_MAP_OVERFLOW_FIRST_PAGE_TYPE = b'\x03'
POINTER_MAP_OVERFLOW_FOLLOWING_PAGE_TYPE = b'\x04'
POINTER_MAP_B_TREE_NON_ROOT_PAGE_TYPE = b'\x05'
POINTER_MAP_PAGE_TYPES = [POINTER_MAP_B_TREE_ROOT_PAGE_TYPE,
                          POINTER_MAP_FREELIST_PAGE_TYPE,
                          POINTER_MAP_OVERFLOW_FIRST_PAGE_TYPE,
                          POINTER_MAP_OVERFLOW_FOLLOWING_PAGE_TYPE,
                          POINTER_MAP_B_TREE_NON_ROOT_PAGE_TYPE]

WAL_FILE_POSTFIX = "-wal"
WAL_HEADER_LENGTH = 32
WAL_MAGIC_NUMBER_BIG_ENDIAN = 0x377F0683
WAL_MAGIC_NUMBER_LITTLE_ENDIAN = 0x377F0682
WAL_FILE_FORMAT_VERSION = 3007000
WAL_FRAME_HEADER_LENGTH = 24

WAL_INDEX_POSTFIX = "-shm"
WAL_INDEX_FILE_FORMAT_VERSION = 3007000
WAL_INDEX_NUMBER_OF_SUB_HEADERS = 2
WAL_INDEX_SUB_HEADER_LENGTH = 48
WAL_INDEX_CHECKPOINT_INFO_LENGTH = 24
WAL_INDEX_LOCK_RESERVED_LENGTH = 16
WAL_INDEX_HEADER_LENGTH = WAL_INDEX_NUMBER_OF_SUB_HEADERS * WAL_INDEX_SUB_HEADER_LENGTH + \
                          WAL_INDEX_CHECKPOINT_INFO_LENGTH + WAL_INDEX_LOCK_RESERVED_LENGTH
WAL_INDEX_NUMBER_OF_FRAMES_BACKFILLED_IN_DATABASE_LENGTH = 4

"""

Note:  The reader mark size is referred to as the Maximum xShmLock index (SQLITE_SHM_NLOCK) - 3 in the sqlite code.

"""
WAL_INDEX_READER_MARK_SIZE = 5
WAL_INDEX_READER_MARK_LENGTH = 4

ROLLBACK_JOURNAL_ALL_CONTENT_UNTIL_END_OF_FILE = -1
ROLLBACK_JOURNAL_POSTFIX = "-journal"
ROLLBACK_JOURNAL_HEADER_LENGTH = 28
ROLLBACK_JOURNAL_HEADER_HEX_STRING = 'd9d505f920a163d7'
ROLLBACK_JOURNAL_HEADER_ALL_CONTENT = 'ffffffff'

BASE_VERSION_NUMBER = 0
COMMIT_RECORD_BASE_VERSION_NUMBER = BASE_VERSION_NUMBER + 1

"""

The DATABASE_HEADER_VERSIONED_FIELDS covers all fields that may change from database header to database header
throughout the write ahead log.  This may not be a definitive list of fields that can change.

"""
DATABASE_HEADER_VERSIONED_FIELDS = Enum({"FILE_CHANGE_COUNTER": "file_change_counter",
                                         "VERSION_VALID_FOR_NUMBER": "version_valid_for_number",
                                         "DATABASE_SIZE_IN_PAGES": "database_size_in_pages",
                                         "FIRST_FREELIST_TRUNK_PAGE_NUMBER": "first_freelist_trunk_page_number",
                                         "NUMBER_OF_FREE_LIST_PAGES": "number_of_freelist_pages",
                                         "LARGEST_ROOT_B_TREE_PAGE_NUMBER": "largest_root_b_tree_page_number",
                                         "SCHEMA_COOKIE": "schema_cookie",
                                         "SCHEMA_FORMAT_NUMBER": "schema_format_number",
                                         "DATABASE_TEXT_ENCODING": "database_text_encoding",
                                         "USER_VERSION": "user_version",
                                         "MD5_HEX_DIGEST": "md5_hex_digest"})

"""

The types of output that are supported by this package.

"""
EXPORT_TYPES = Enum(["TEXT", "CSV", "SQLITE", "XLSX", "CASE"])

"""
Defines the list of common SQLite3 file extensions for initial identification of files to dissect for the bulk processing.
"""
SQLITE_FILE_EXTENSIONS = (".db", ".db3", ".sqlite", ".sqlite3")

"""
Below we instantiate and compile a regular expression to check xml illegal characters:
ILLEGAL_XML_CHARACTER_PATTERN.

"""

_illegal_xml_characters = [(0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F), (0x7F, 0x84), (0x86, 0x9F),
                           (0xD800, 0xDFFF), (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF)]

if maxunicode >= 0x10000:
    _illegal_xml_characters.extend([(0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF), (0x3FFFE, 0x3FFFF),
                                    (0x4FFFE, 0x4FFFF), (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF),
                                    (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF), (0x9FFFE, 0x9FFFF),
                                    (0xAFFFE, 0xAFFFF), (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
                                    (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF), (0xFFFFE, 0xFFFFF),
                                    (0x10FFFE, 0x10FFFF)])

_illegal_xml_ranges = ["%s-%s" % (unichr(low), unichr(high)) for (low, high) in _illegal_xml_characters]
ILLEGAL_XML_CHARACTER_PATTERN = compile(u'[%s]' % u''.join(_illegal_xml_ranges))
