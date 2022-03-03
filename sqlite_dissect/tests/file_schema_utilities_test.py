import pytest
from sqlite_dissect.file.schema.utilities import get_index_of_closing_parenthesis, parse_comment_from_sql_segment
from sqlite_dissect.exception import MasterSchemaParsingError

get_index_of_closing_parenthesis_params = [
    ('()', 0, 1),  # most basic case
    ('(()(()()))', 0, 9),  # adds intermediate parentheses
    ('(test text((more test text)))', 10, 27),  # non-zero offset with non-parenthesis characters
    ('no parentheses here', 0, -1),  # no parentheses found
    ('(sql statement --comment\nmore sql statement)', 0, 43),  # line comment
    ('(sql statement /*block comment*/)', 0, 32),  # block comment
    ('(sql statement --unterminated comment ))))', 0, -2),  # unterminated comment (shouldn't find pair)
    ('(no pair', 0, -2),  # no closing parenthesis
    ('(sql // bad comment)', 0, -2)  # bad comment indicator
]


@pytest.mark.parametrize('input_str, offset, expected_index', get_index_of_closing_parenthesis_params)
def test_get_index_of_closing_parenthesis(input_str, offset, expected_index):
    # expected_index of -1 will signal ValueError to test finding a non-parenthesis character at offset.
    if expected_index == -1:
        with pytest.raises(ValueError):
            get_index_of_closing_parenthesis(input_str, offset)

    # expected_index of -2 will signal MasterSchemaParsingError to test finding incomplete comment indicator/no
    # closing parenthesis
    elif expected_index == -2:
        with pytest.raises(MasterSchemaParsingError):
            get_index_of_closing_parenthesis(input_str, offset)

    else:
        assert expected_index == get_index_of_closing_parenthesis(input_str, offset)


# no test for an unterminated comment is carried out, function assumes a VALID comment has been found before parsing.
parse_comment_from_sql_segment_params = [
    ('--line comment\n', '--line comment\n'),
    ('/*block comment*/', '/*block comment*/'),
    ('--line1\nmore sql --line2\n', '--line1\n'),
    ('/*block1*/more sql/*block2*/', '/*block1*/'),
    ('no comments', -1),
]


@pytest.mark.parametrize('input_str, expected_return', parse_comment_from_sql_segment_params)
def test_parse_comment_from_sql_segment(input_str, expected_return):
    # expected_return of -1 will signal MasterSchemaParsingError to test finding a character that isn't a comment
    # identifier at beginning of string.
    if expected_return == -1:
        with pytest.raises(MasterSchemaParsingError):
            parse_comment_from_sql_segment(input_str)

    else:
        assert parse_comment_from_sql_segment(input_str)[0] == expected_return