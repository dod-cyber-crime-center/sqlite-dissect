import hashlib
import logging
from binascii import hexlify
from hashlib import md5
from logging import getLogger
from re import compile
from struct import pack
from struct import unpack
from os import walk, makedirs, path
from os.path import exists, isdir, join
from sqlite_dissect.constants import ALL_ZEROS_REGEX, SQLITE_DATABASE_HEADER_LENGTH, MAGIC_HEADER_STRING, \
    MAGIC_HEADER_STRING_ENCODING, SQLITE_FILE_EXTENSIONS
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import OVERFLOW_HEADER_LENGTH
from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER
from sqlite_dissect.constants import TEXT_SIGNATURE_IDENTIFIER
from sqlite_dissect.exception import InvalidVarIntError
from sqlite_dissect._version import __version__
from configargparse import ArgParser

"""

utilities.py

This script holds general utility functions for reference by the sqlite carving library.

The script holds the following class(es):
DotDict(dict)

This script holds the following function(s):
calculate_expected_overflow(overflow_byte_size, page_size)
decode_varint(byte_array, offset)
encode_varint(value)
get_class_instance(class_name)
get_md5_hash(string)
get_record_content(serial_type, record_body, offset=0)
get_serial_type_signature(serial_type)
has_content(byte_array)
is_sqlite_file(path)
get_sqlite_files(path)
create_directory(dir_path)
hash_file(file_path, hash_algo=hashlib.sha256())

"""


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def calculate_expected_overflow(overflow_byte_size, page_size):
    overflow_pages = 0
    last_overflow_page_content_size = overflow_byte_size

    if overflow_byte_size > 0:
        while overflow_byte_size > 0:
            overflow_pages += 1
            last_overflow_page_content_size = overflow_byte_size
            overflow_byte_size = overflow_byte_size - page_size + OVERFLOW_HEADER_LENGTH

    return overflow_pages, last_overflow_page_content_size


def decode_varint(byte_array, offset=0):
    unsigned_integer_value = 0
    varint_relative_offset = 0

    for x in xrange(1, 10):

        varint_byte = ord(byte_array[offset + varint_relative_offset:offset + varint_relative_offset + 1])
        varint_relative_offset += 1

        if x == 9:
            unsigned_integer_value <<= 1
            unsigned_integer_value |= varint_byte
        else:
            msb_set = varint_byte & 0x80
            varint_byte &= 0x7f
            unsigned_integer_value |= varint_byte
            if msb_set == 0:
                break
            else:
                unsigned_integer_value <<= 7

    signed_integer_value = unsigned_integer_value
    if signed_integer_value & 0x80000000 << 32:
        signed_integer_value -= 0x10000000000000000

    return signed_integer_value, varint_relative_offset


def encode_varint(value):
    max_allowed = 0x7fffffffffffffff
    min_allowed = (max_allowed + 1) - 0x10000000000000000
    if value > max_allowed or value < min_allowed:
        log_message = "The value: {} is not able to be cast into a 64 bit signed integer for encoding."
        log_message = log_message.format(value)
        getLogger(LOGGER_NAME).error(log_message)
        raise InvalidVarIntError(log_message)

    byte_array = bytearray()

    value += 1 << 64 if value < 0 else 0

    if value & 0xff000000 << 32:

        byte = value & 0xff
        byte_array.insert(0, pack("B", byte))
        value >>= 8

        for _ in xrange(8):
            byte_array.insert(0, pack("B", (value & 0x7f) | 0x80))
            value >>= 7

    else:

        while value:
            byte_array.insert(0, pack("B", (value & 0x7f) | 0x80))
            value >>= 7

            if len(byte_array) >= 9:
                log_message = "The value: {} produced a varint with a byte array of length: {} beyond the 9 bytes " \
                              "allowed for a varint."
                log_message = log_message.format(value, len(byte_array))
                getLogger(LOGGER_NAME).error(log_message)
                raise InvalidVarIntError(log_message)

        byte_array[-1] &= 0x7f

    return byte_array


def get_class_instance(class_name):
    if class_name.find(".") != -1:
        path_array = class_name.split(".")
        module = ".".join(path_array[:-1])
        instance = __import__(module)
        for section in path_array[1:]:
            instance = getattr(instance, section)
        return instance
    else:
        log_message = "Class name: {} did not specify needed modules in order to initialize correctly."
        log_message = log_message.format(log_message)
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)


def get_md5_hash(string):
    md5_hash = md5()
    md5_hash.update(string)
    return md5_hash.hexdigest().upper()


def get_record_content(serial_type, record_body, offset=0):
    # NULL
    if serial_type == 0:
        content_size = 0
        value = None

    # 8-bit twos-complement integer
    elif serial_type == 1:
        content_size = 1
        value = unpack(b">b", record_body[offset:offset + content_size])[0]

    # Big-endian 16-bit twos-complement integer
    elif serial_type == 2:
        content_size = 2
        value = unpack(b">h", record_body[offset:offset + content_size])[0]

    # Big-endian 24-bit twos-complement integer
    elif serial_type == 3:
        content_size = 3
        value_byte_array = '\0' + record_body[offset:offset + content_size]
        value = unpack(b">I", value_byte_array)[0]
        if value & 0x800000:
            value -= 0x1000000

    # Big-endian 32-bit twos-complement integer
    elif serial_type == 4:
        content_size = 4
        value = unpack(b">i", record_body[offset:offset + content_size])[0]

    # Big-endian 48-bit twos-complement integer
    elif serial_type == 5:
        content_size = 6
        value_byte_array = '\0' + '\0' + record_body[offset:offset + content_size]
        value = unpack(b">Q", value_byte_array)[0]
        if value & 0x800000000000:
            value -= 0x1000000000000

    # Big-endian 64-bit twos-complement integer
    elif serial_type == 6:
        content_size = 8
        value = unpack(b">q", record_body[offset:offset + content_size])[0]

    # Big-endian IEEE 754-2008 64-bit floating point number
    elif serial_type == 7:
        content_size = 8
        value = unpack(b">d", record_body[offset:offset + content_size])[0]

    # Integer constant 0 (schema format == 4)
    elif serial_type == 8:
        content_size = 0
        value = 0

    # Integer constant 1 (schema format == 4)
    elif serial_type == 9:
        content_size = 0
        value = 1

    # These values are not used/reserved and should not be found in sqlite files
    elif serial_type == 10 or serial_type == 11:
        raise ValueError("The serial type {} is not expected in SQLite files".format(serial_type))

    # A BLOB that is (N-12)/2 bytes in length
    elif serial_type >= 12 and serial_type % 2 == 0:
        content_size = (serial_type - 12) / 2
        value = record_body[offset:offset + content_size]

    # A string in the database encoding and is (N-13)/2 bytes in length.  The nul terminator is omitted
    elif serial_type >= 13 and serial_type % 2 == 1:
        content_size = (serial_type - 13) / 2
        value = record_body[offset:offset + content_size]

    else:
        log_message = "Invalid serial type: {} at offset: {} in record body: {}."
        log_message = log_message.format(serial_type, offset, hexlify(record_body))
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)

    return content_size, value


def get_serial_type_signature(serial_type):
    if serial_type >= 12:
        if serial_type % 2 == 0:
            return BLOB_SIGNATURE_IDENTIFIER
        elif serial_type % 2 == 1:
            return TEXT_SIGNATURE_IDENTIFIER
    return serial_type


def has_content(byte_array):
    pattern = compile(ALL_ZEROS_REGEX)
    if pattern.match(hexlify(byte_array)):
        return False
    return True


def is_sqlite_file(path):
    """
    Determines if the specified file contains the magic bytes to indicate it is a SQLite file. This is not meant to be a
    full validation of the file format, and that is asserted within the class at file/database/header.py.

    :param path: the string path to the file to be validated.
    :raises:
        IOError when the provided path does not exist in the filesystem.
        IOError when the file cannot be properly read for header comparison.
    """
    # Ensure the path exists
    if not exists(path):
        raise IOError("The specified path cannot be found: {}".format(path))

    # Attempt to open the file for reading
    try:
        with open(path, "rb") as sqlite:
            header = sqlite.read(SQLITE_DATABASE_HEADER_LENGTH)
            header_magic = header[0:16]
            magic = MAGIC_HEADER_STRING.decode(MAGIC_HEADER_STRING_ENCODING)
            return header_magic == magic
    except IOError as e:
        logging.error("Invalid SQLite file found: {}".format(e))


def get_sqlite_files(path):
    """
    Parses the path, validates it exists, and returns a list of all valid file(s) at the provided path. If the provided
    path is a file, it ensures it's a valid SQLite file and returns the path. If it's a directory, it validates all
    files within the directory and adds valid SQLite files to the path list.

    :param path: the string path to the file or directory in which SQLite files should be discovered.
    :raises:
        IOError when the provided path does not exist in the filesystem.
    """
    sqlite_files = []

    if exists(path):
        # Determine if it's a directory
        if isdir(path):
            for root, dirnames, filenames in walk(path):
                for filename in filenames:
                    if filename.endswith(SQLITE_FILE_EXTENSIONS):
                        # Ensure the SQLite file is valid
                        relative_path = join(root, filename)
                        if is_sqlite_file(relative_path):
                            sqlite_files.append(relative_path)
                        else:
                            logging.info("File was found but is not a SQLite file: {}".format(relative_path))
        else:
            if is_sqlite_file(path):
                sqlite_files.append(path)
            else:
                logging.info("File was found but is not a SQLite file: {}".format(path))
    else:
        raise IOError("The specified path cannot be found: {}".format(path))

    return sqlite_files


def create_directory(dir_path):
    """
    Creates a directory if it doesn't already exist.
    :param dir_path: The path of the directory to create
    :return: bool whether the directory was created correctly or if it already exists
    """
    if not exists(dir_path):
        try:
            makedirs(dir_path)
        except (OSError, IOError) as e:
            logging.error("Unable to create directory {} with error: {}".format(dir_path, e))
            return False

    # Ensure the directory was actually created, and it is actually a directory
    return exists(dir_path) and isdir(dir_path)


def hash_file(file_path, hash_algo=hashlib.sha256()):
    """
    Generates a hash of a file by chunking it and utilizing the Python hashlib library.
    """
    # Ensure the file path exists
    if not path.exists(file_path):
        raise IOError("The file path {} is not valid, the file does not exist".format(file_path))

    with open(file_path, 'rb') as f:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = f.read(hash_algo.block_size)
            if not chunk:
                break
            hash_algo.update(chunk)
    return hash_algo.hexdigest()


# Uses ArgumentParser from argparse to evaluate user arguments.
def parse_args(args=None):
    description = "SQLite Dissect is a SQLite parser with recovery abilities over SQLite databases " \
                  "and their accompanying journal files. If no options are set other than the file " \
                  "name, the default behaviour will be to check for any journal files and print to " \
                  "the console the output of the SQLite files.  The directory of the SQLite file " \
                  "specified will be searched through to find the associated journal files.  If " \
                  "they are not in the same directory as the specified file, they will not be found " \
                  "and their location will need to be specified in the command.  SQLite carving " \
                  "will not be done by default.  Please see the options below to enable carving."

    parser = ArgParser(description=description)

    # Define the argument for the configuration file that can optionally be passed
    parser.add_argument('--config', required=False, is_config_file=True, help='The path to the configuration file')

    parser.add_argument("sqlite_path",
                        metavar="SQLITE_PATH",
                        help="The path to the SQLite database file or directory containing multiple files",
                        )

    parser.add_argument("-v", "--version",
                        action="version",
                        version="version {version}".format(version=__version__),
                        help="display the version of SQLite Dissect")

    parser.add_argument("-d", "--directory",
                        metavar="OUTPUT_DIRECTORY",
                        env_var="SQLD_OUTPUT_DIRECTORY",
                        help="directory to write output to (must be specified for outputs other than console text)")

    parser.add_argument("-p", "--file-prefix",
                        default="",
                        metavar="FILE_PREFIX",
                        env_var="SQLD_FILE_PREFIX",
                        help="the file prefix to use on output files, default is the name of the SQLite "
                             "file (the directory for output must be specified)")

    parser.add_argument("-e", "--export",
                        nargs="*",
                        choices=["text", "csv", "sqlite", "xlsx", "case"],
                        default=["text"],
                        metavar="EXPORT_TYPE",
                        env_var="SQLD_EXPORT_TYPE",
                        help="the format to export to {text, csv, sqlite, xlsx, case} (text written to console if -d "
                             "is not specified)")

    journal_group = parser.add_mutually_exclusive_group()
    journal_group.add_argument("-n", "--no-journal",
                               action="store_true",
                               default=False,
                               env_var="SQLD_NO_JOURNAL",
                               help="turn off automatic detection of journal files")
    journal_group.add_argument("-w", "--wal",
                               env_var="SQLD_WAL",
                               help="the wal file to use instead of searching the SQLite file directory by default")
    journal_group.add_argument("-j", "--rollback-journal",
                               env_var="SQLD_ROLLBACK_JOURNAL",
                               help="the rollback journal file to use in carving instead of searching the SQLite file "
                                    "directory by default (under development, currently only outputs to csv, output "
                                    "directory needs to be specified)")

    parser.add_argument("-r", "--exempted-tables",
                        metavar="EXEMPTED_TABLES",
                        env_var="SQLD_EXEMPTED_TABLES",
                        help="comma-delimited string of tables [table1,table2,table3] to exempt (only implemented "
                             "and allowed for rollback journal parsing currently) ex.) table1,table2,table3")

    parser.add_argument("-s", "--schema",
                        action="store_true",
                        env_var="SQLD_SCHEMA",
                        help="output the schema to console, the initial schema found in the main database file")

    parser.add_argument("-t", "--schema-history",
                        action="store_true",
                        env_var="SQLD_SCHEMA_HISTORY",
                        help="output the schema history to console, prints the --schema information and "
                             "write-head log changes")

    parser.add_argument("-g", "--signatures",
                        action="store_true",
                        env_var="SQLD_SIGNATURES",
                        help="output the signatures generated to console")

    parser.add_argument("-c", "--carve",
                        action="store_true",
                        env_var="SQLD_CARVE",
                        default=False,
                        help="carves and recovers table data")

    parser.add_argument("-f", "--carve-freelists",
                        action="store_true",
                        env_var="SQLD_CARVE_FREELISTS",
                        default=False,
                        help="carves freelist pages (carving must be enabled, under development)")

    parser.add_argument("-b", "--tables",
                        metavar="TABLES",
                        env_var="SQLD_TABLES",
                        help="specified comma-delimited string of tables [table1,table2,table3] to carve "
                             "ex.) table1,table2,table3")

    parser.add_argument("-k", "--disable-strict-format-checking",
                        action="store_true",
                        env_var="SQLD_DISABLE_STRICT_FORMAT_CHECKING",
                        default=False,
                        help="disable strict format checks for SQLite databases "
                             "(this may result in improperly parsed SQLite files)")

    logging_group = parser.add_mutually_exclusive_group()
    logging_group.add_argument("-l", "--log-level",
                               default="off",
                               choices=["critical", "error", "warning", "info", "debug", "off"],
                               metavar="LOG_LEVEL",
                               env_var="SQLD_LOG_LEVEL",
                               help="level to log messages at {critical, error, warning, info, debug, off}")
    parser.add_argument("-i", "--log-file",
                        default=None,
                        metavar="LOG_FILE",
                        env_var="SQLD_LOG_FILE",
                        help="log file to write too, default is to write to console, ignored if log level set to off "
                             "(appends if file already exists)")

    parser.add_argument("--warnings",
                        action="store_true",
                        default=False,
                        help="enable runtime warnings")

    parser.add_argument("--header",
                        action="store_true",
                        default=False,
                        help="Print header information")

    return parser.parse_args(args)
