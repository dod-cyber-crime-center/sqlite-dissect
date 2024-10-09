import pytest

from sqlite_dissect.carving.utilities import decode_varint_in_reverse, generate_regex_for_simplified_serial_type, \
    calculate_body_content_size, calculate_serial_type_definition_content_length_min_max, \
    calculate_serial_type_varint_length_min_max, get_content_size, generate_signature_regex

from sqlite_dissect.constants import BLOB_SIGNATURE_IDENTIFIER, TEXT_SIGNATURE_IDENTIFIER
from sqlite_dissect.exception import CarvingError
from sqlite_dissect.utilities import encode_varint


varint_tuples = [
    (0x10, encode_varint(0x10)),
    (0x1090, encode_varint(0x1090)),
    (0x109028, encode_varint(0x109028)),
    (0x10902873, encode_varint(0x10902873)),
    (0x1F09A8B298, encode_varint(0x1F09A8B298)),
    (0x1F09A8B298E9, encode_varint(0x1F09A8B298E9)),
    (0x1F09A8B298E912, encode_varint(0x1F09A8B298E912)),
    #TODO: (0x1F09A8B298E91092, encode_varint(0x1F09A8B298E91092)),
    #TODO: (0xFFFFFFFFFFFFFFFF, encode_varint(0x7FFFFFFFFFFFFFFF))
]


@pytest.mark.parametrize('value, encoded_value', varint_tuples)
def test_decode_varint_in_reverse(value, encoded_value):
    with pytest.raises(ValueError):
        decode_varint_in_reverse(bytearray(b'0'*9), 11)

    assert decode_varint_in_reverse(encoded_value, len(encoded_value))[0] == value


def test_get_content_size():
    # some random low ints
    assert get_content_size(0) == 0
    assert get_content_size(4) == 4
    assert get_content_size(7) == 8
    assert get_content_size(9) == 0

    # some even numbers >= 12
    assert get_content_size(28) == 8
    assert get_content_size(256) == 122

    # some odd ints >= 13
    assert get_content_size(33) == 10
    assert get_content_size(177) == 82

    # anything other than int >= 0 should error
    with pytest.raises(ValueError):
        get_content_size(-1)

    # string currently throws TypeError on attempted > comparison
    #with pytest.raises(ValueError):
    #    get_content_size("not an int")


def test_generate_regex_for_simplified_serial_type():
    # hardcoded values for -2 and -1
    # hex string for 0-9
    # CarvingError for anything else
    assert generate_regex_for_simplified_serial_type(4) == b"\x04"

    with pytest.raises(CarvingError):
        generate_regex_for_simplified_serial_type(-10)

    with pytest.raises(CarvingError):
        generate_regex_for_simplified_serial_type(10)


def test_calculate_body_content_size():
    # looks like it just decodes the passed in varint and then calls get_content_size
    assert calculate_body_content_size(encode_varint(5)) == get_content_size(5)
    assert calculate_body_content_size(encode_varint(30)) == get_content_size(30)
    assert calculate_body_content_size(encode_varint(47)) == get_content_size(47)
    assert calculate_body_content_size(bytearray([0x0])) == get_content_size(0)


def test_calculate_serial_type_definition_content_length_min_max():
    test_list = []
    content_max_length = int('1111111' * 9, 2)

    assert calculate_serial_type_definition_content_length_min_max(None, 9) == (0, content_max_length)
    assert calculate_serial_type_definition_content_length_min_max(test_list, 9) == (0, content_max_length)
    test_list = [BLOB_SIGNATURE_IDENTIFIER]
    assert calculate_serial_type_definition_content_length_min_max(test_list, 9) == (1, content_max_length)
    test_list = [TEXT_SIGNATURE_IDENTIFIER]
    assert calculate_serial_type_definition_content_length_min_max(test_list, 9) == (1, content_max_length)
    test_list = [1, 2, 3, 4]
    assert calculate_serial_type_definition_content_length_min_max(test_list, 9) == (1, 4)
    test_list = [6, 7, 5]
    assert calculate_serial_type_definition_content_length_min_max(test_list, 9) == (6, 8)
    test_list = [BLOB_SIGNATURE_IDENTIFIER, 5, 6]
    assert calculate_serial_type_definition_content_length_min_max(test_list, 9) == (1, content_max_length)


varint_length_min_max_params = [
    ([1, 2, 3, 4], 1, 1),  # no blob or string
    #TODO: ([1, 2, -1], 1, 5),  # blob present; max changes
    #TODO: ([1, 2, 3, -2], 1, 5)  # string present; max changes
]


@pytest.mark.parametrize('type_list, expected_min, expected_max', varint_length_min_max_params)
def test_calculate_serial_type_varint_length_min_max(type_list, expected_min, expected_max):
    assert calculate_serial_type_varint_length_min_max(type_list) == (expected_min, expected_max)


generate_signature_regex_params = [
    ([[]], False, -1),
    ([[1] * 20], False, -1),
    ([[-1, -1]], False, -1),
    ([[-2, -2]], False, -1),
    ([[-1, -2]], False, b'(?:(?:[\x0D-\x7F]|[\x80-\xFF]{1,7}[\x00-\x7F])|(?:[\x0C-\x7F]|[\x80-\xFF]{1,7}[\x00-\x7F]))'),
    ([[0, 1, -1]], False, b'(?:[\x00\x01]|(?:[\x0D-\x7F]|[\x80-\xFF]{1,7}[\x00-\x7F]))'),
    ([[0, 1, -2]], False, b'(?:[\x00\x01]|(?:[\x0C-\x7f]|[\x80-\xFF]{1,7}[\x00-\x7F]))'),
    ([[0, 1, -2], [1, -2]], True, b'(?:[\x01]|(?:[\x0C-\x7f]|[\x80-\xFF]{1,7}[\x00-\x7F]))'),
    ([[0, 1]], False, b'[\x00\x01]')
]


@pytest.mark.parametrize('column_list, skip_first, expected_value', generate_signature_regex_params)
def test_generate_signature_regex(column_list, skip_first, expected_value):
    if expected_value == -1:
        with pytest.raises(CarvingError):
            _ = generate_signature_regex(column_list, skip_first)

    else:
        assert generate_signature_regex(column_list, skip_first) == expected_value