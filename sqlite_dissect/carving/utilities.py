from binascii import hexlify
from binascii import unhexlify
from logging import getLogger
from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import TEXT_SIGNATURE_IDENTIFIER
from sqlite_dissect.exception import CarvingError
from sqlite_dissect.exception import InvalidVarIntError
from sqlite_dissect.utilities import decode_varint

"""

utilities.py

This script holds carving utility functions for reference by the SQLite carving module.

This script holds the following function(s):
decode_varint_in_reverse(byte_array, offset)
calculate_body_content_size(serial_type_header)
calculate_serial_type_definition_content_length_min_max(simplified_serial_types, allowed_varint_length=5)
calculate_serial_type_varint_length_min_max(simplified_serial_types)
generate_regex_for_simplified_serial_type(simplified_serial_type)
generate_signature_regex(signature, skip_first_serial_type=False)
get_content_size(serial_type)

"""


def decode_varint_in_reverse(byte_array, offset, max_varint_length=9):

    """

    This function will move backwards through a byte array trying to decode a varint in reverse.  A InvalidVarIntError
    will be raised if a varint is not found by this algorithm used in this function.  The calling logic should check
    for this case in case it is encountered which is likely in the context of carving.

    Note:  This cannot determine if the field being parsed was originally a varint or not and may give false positives.
           Please keep this in mind when calling this function.

    Note:  If the array runs out of bytes while parsing in reverse, the currently determined varint will be returned.

    Note:  Since the parsing starts from the left of the offset specified, the resulting byte string that represents
           this varint can be determined by byte_array[varint_relative_offset:offset].  The length of the varint
           in bytes can be determined likewise either from the len() of the above or offset - varint_relative_offset.

    :param byte_array: bytearray  The byte array to parse for the varint in reverse.
    :param offset: int  The offset to move backwards from.  The offset specified is not included in the parsing and the
                        algorithm starts with the last byte of the varint at offset - 1.  If you want to start at the
                        end of the byte array then the offset should be the length of the byte array (where the offset
                        would refer to a non-existing index in the array).
    :param max_varint_length: int  The maximum number of varint bytes to go back in reverse.  The default is 9 since
                                   this is the maximum number of bytes a varint can be.

    :return:

    :raise: InvalidVarIntError:  If a varint is not determined while parsing the byte array in reverse using the
                                 algorithm in this function.  This error is not logged as an error but rather a
                                 debug statement since it is very likely to occur during carving and should be handled
                                 appropriately.

    """

    if offset > len(byte_array):
        log_message = "The offset: {} is greater than the size of the byte array: {} for the bytes: {}."
        log_message = log_message.format(offset, len(byte_array), hexlify(byte_array))
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)

    unsigned_integer_value = 0
    varint_inverted_relative_offset = 0

    varint_byte = ord(byte_array[offset - 1 - varint_inverted_relative_offset:offset - varint_inverted_relative_offset])
    varint_byte &= 0x7f
    unsigned_integer_value |= varint_byte
    varint_inverted_relative_offset += 1

    while offset - varint_inverted_relative_offset - 1 >= 0:

        if varint_inverted_relative_offset > max_varint_length:

            """

            Since this exception is not considered a important exception to log as an error, it will be logged
            as a debug statement.  There is a good chance of this use case occurring and is even expected during
            carving.

            """

            log_message = "A varint was not determined from byte array: {} starting at offset: {} in reverse."
            log_message = log_message.format(byte_array, offset)
            getLogger(LOGGER_NAME).debug(log_message)
            return InvalidVarIntError(log_message)

        varint_byte = ord(byte_array[offset - 1 - varint_inverted_relative_offset:
                                     offset - varint_inverted_relative_offset])
        msb_set = varint_byte & 0x80
        if msb_set:
            varint_byte &= 0x7f
            varint_byte <<= (7 * varint_inverted_relative_offset)
            unsigned_integer_value |= varint_byte
            varint_inverted_relative_offset += 1
        else:
            break

    varint_relative_offset = offset - varint_inverted_relative_offset

    return unsigned_integer_value, varint_relative_offset


def calculate_body_content_size(serial_type_header):
    body_content_size = 0
    start_offset = 0
    while start_offset < len(serial_type_header):
        serial_type, serial_type_varint_length = decode_varint(serial_type_header, start_offset)
        body_content_size += get_content_size(serial_type)
        start_offset += serial_type_varint_length
        if start_offset > len(serial_type_header):
            log_message = "Invalid start offset: {} retrieved from serial type header of length: {}: {}."
            log_message = log_message.format(start_offset, len(serial_type_header), hexlify(serial_type_header))
            getLogger(LOGGER_NAME).error(log_message)
            raise CarvingError(log_message)
    return body_content_size


def calculate_serial_type_definition_content_length_min_max(simplified_serial_types=None, allowed_varint_length=5):

    content_max_length = int('1111111' * allowed_varint_length, 2)

    if not simplified_serial_types:
        return 0, content_max_length

    serial_type_definition_content_length_min = content_max_length
    serial_type_definition_content_length_max = 0

    for simplified_serial_type in simplified_serial_types:
        if simplified_serial_type in [BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER]:
            serial_type_definition_content_length_min = min(serial_type_definition_content_length_min, 1)
            serial_type_definition_content_length_max = max(serial_type_definition_content_length_max,
                                                            content_max_length)
        else:
            serial_type_content_length = get_content_size(simplified_serial_type)
            serial_type_definition_content_length_min = min(serial_type_definition_content_length_min,
                                                            serial_type_content_length)
            serial_type_definition_content_length_max = max(serial_type_definition_content_length_max,
                                                            serial_type_content_length)

    return serial_type_definition_content_length_min, serial_type_definition_content_length_max


def calculate_serial_type_varint_length_min_max(simplified_serial_types):

    serial_type_varint_length_min = 5
    serial_type_varint_length_max = 1

    for simplified_serial_type in simplified_serial_types:

        if simplified_serial_type in [BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER]:
            serial_type_varint_length_min = min(serial_type_varint_length_min, 1)
            serial_type_varint_length_max = min(serial_type_varint_length_max, 5)
        else:
            serial_type_varint_length_min = min(serial_type_varint_length_min, 1)
            serial_type_varint_length_max = min(serial_type_varint_length_max, 1)

    return serial_type_varint_length_min, serial_type_varint_length_max


def generate_regex_for_simplified_serial_type(simplified_serial_type):

    """



    Note:  Right now 9 byte varints are not supported in the regular expressions generated for blob and text storage
           classes.

    :param simplified_serial_type:

    :return:

    """

    if simplified_serial_type == -2:
        return "(?:[\x0C-\x7F]|[\x80-\xFF]{1,7}[\x00-\x7F])"
    elif simplified_serial_type == -1:
        return "(?:[\x0D-\x7F]|[\x80-\xFF]{1,7}[\x00-\x7F])"
    elif 0 <= simplified_serial_type <= 9:
        return unhexlify("0{}".format(simplified_serial_type))
    else:
        log_message = "Unable to generate regular expression for simplified serial type: {}."
        log_message = log_message.format(simplified_serial_type)
        getLogger(LOGGER_NAME).error(log_message)
        raise CarvingError(log_message)


def generate_signature_regex(signature, skip_first_serial_type=False):

    """

    This function will generate the regular expression for a particular signature sent in derived from a Signature
    class.  For instance, the signature should be in list form as the simplified signature, simplified schema
    signature, etc.

    The skip first serial type field will omit the first serial type from the regular expression.  This is to better
    support carving of freeblocks since the first 4 bytes are overwritten of the entry and this could contain the first
    serial type byte in the header as the fourth byte.  Leaving this out will provide better accuracy for determining
    deleted entries in freeblocks.

    Note:  There may be issues if there is only one field either in the signature or left in the signature after the
           first serial type is skipped, if specified.

    Note:  There is also the case of the first serial type being a varint which needs to be addressed.

    :param signature:
    :param skip_first_serial_type:

    :return:

    """

    regex = ""

    if skip_first_serial_type:
        signature = signature[1:]

    for column_serial_type_array in signature:

        number_of_possible_serial_types = len(column_serial_type_array)

        if number_of_possible_serial_types == 1:

            serial_type = column_serial_type_array[0]
            serial_type_regex = generate_regex_for_simplified_serial_type(serial_type)
            regex += serial_type_regex

        elif 1 < number_of_possible_serial_types < 13:

            """

            The maximum number of possible serial types are in the range of 1 to 12.  Since the case of just
            a single serial type is handled above, this portion accounts for possible serial types of more than
            1 field up to 12.  These can be the following 12 serial type fields: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -2.

            """

            basic_serial_type_regex = ""
            blob_regex = ""
            text_regex = ""

            for column_serial_type in column_serial_type_array:
                if column_serial_type == -1:
                    blob_regex = generate_regex_for_simplified_serial_type(column_serial_type)
                elif column_serial_type == -2:
                    text_regex = generate_regex_for_simplified_serial_type(column_serial_type)
                else:
                    basic_serial_type_regex += generate_regex_for_simplified_serial_type(column_serial_type)

            if blob_regex or text_regex:

                if basic_serial_type_regex:
                    basic_serial_type_regex = "[{}]".format(basic_serial_type_regex)

                if blob_regex and not text_regex:

                    if not basic_serial_type_regex:
                        log_message = "No basic serial type regular expression found when multiple column serial " \
                                      "types were defined with a blob regular expression of: {} and no text regular " \
                                      "expression in the signature: {} where the skip first serial type was set to: {}."
                        log_message = log_message.format(blob_regex, signature, skip_first_serial_type)
                        getLogger(LOGGER_NAME).error(log_message)
                        raise CarvingError(log_message)

                    regex += "(?:{}|{})".format(basic_serial_type_regex, blob_regex)

                elif not blob_regex and text_regex:

                    if not basic_serial_type_regex:
                        log_message = "No basic serial type regular expression found when multiple column serial " \
                                      "types were defined with no blob regular expression and a text regular " \
                                      "expression of: {} in the signature: {} where the skip first serial type " \
                                      "was set to: {}."
                        log_message = log_message.format(text_regex, signature, skip_first_serial_type)
                        getLogger(LOGGER_NAME).error(log_message)
                        raise CarvingError(log_message)

                    regex += "(?:{}|{})".format(basic_serial_type_regex, text_regex)

                elif blob_regex and text_regex:

                    var_length_regex = blob_regex + "|" + text_regex
                    if basic_serial_type_regex:
                        regex += "(?:{}|{})".format(basic_serial_type_regex, var_length_regex)
                    else:
                        regex += "(?:{})".format(var_length_regex)

                else:
                    log_message = "No appropriate regular expressions were found for basic serial type, blob, or " \
                                  "text column signature types in the signature: {} where the skip first serial type " \
                                  "was set to: {}."
                    log_message = log_message.format(text_regex, signature, skip_first_serial_type)
                    getLogger(LOGGER_NAME).error(log_message)
                    raise CarvingError(log_message)

            else:

                """

                Since a blob or text regex was not found, the signatures must only be basic serial types (which are
                considered non-variable length serial types).

                """

                if not basic_serial_type_regex:
                    log_message = "No basic serial type regular expression found when no variable length serial " \
                                  "types were determined in the signature: {} where the skip first serial type was " \
                                  "set to: {}."
                    log_message = log_message.format(signature, skip_first_serial_type)
                    getLogger(LOGGER_NAME).error(log_message)
                    raise CarvingError(log_message)

                regex += "[{}]".format(basic_serial_type_regex)

        else:

            log_message = "Invalid number of columns in the signature: {} to generate a regular expression from " \
                          "where the skip first serial type was set to: {}."
            log_message = log_message.format(signature, skip_first_serial_type)
            getLogger(LOGGER_NAME).error(log_message)
            raise CarvingError(log_message)

    return regex


def get_content_size(serial_type):

    # NULL
    if serial_type == 0:
        return 0

    # 8-bit twos-complement integer
    elif serial_type == 1:
        return 1

    # Big-endian 16-bit twos-complement integer
    elif serial_type == 2:
        return 2

    # Big-endian 24-bit twos-complement integer
    elif serial_type == 3:
        return 3

    # Big-endian 32-bit twos-complement integer
    elif serial_type == 4:
        return 4

    # Big-endian 48-bit twos-complement integer
    elif serial_type == 5:
        return 6

    # Big-endian 64-bit twos-complement integer
    elif serial_type == 6:
        return 8

    # Big-endian IEEE 754-2008 64-bit floating point number
    elif serial_type == 7:
        return 8

    # Integer constant 0 (schema format == 4)
    elif serial_type == 8:
        return 0

    # Integer constant 1 (schema format == 4)
    elif serial_type == 9:
        return 0

    # A BLOB that is (N-12)/2 bytes in length
    elif serial_type >= 12 and serial_type % 2 == 0:
        return (serial_type - 12) / 2

    # A string in the database encoding and is (N-13)/2 bytes in length.  The nul terminator is omitted
    elif serial_type >= 13 and serial_type % 2 == 1:
        return (serial_type - 13) / 2

    else:
        log_message = "Invalid serial type: {}."
        log_message = log_message.format(serial_type)
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)
