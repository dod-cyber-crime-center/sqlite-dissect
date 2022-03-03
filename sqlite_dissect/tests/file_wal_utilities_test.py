import pytest
from sqlite_dissect.file.database.header import DatabaseHeader
from sqlite_dissect.file.wal.utilities import compare_database_headers
import os


def get_db_header(filepath):
    with open(filepath, 'rb') as db_file:
        return DatabaseHeader(db_file.read(100))


def find_header_changes(prev_header, current_header):
    changes = {}
    for prev_item, current_item in zip(prev_header.__dict__.items(), current_header.__dict__.items()):
        if prev_item != current_item:
            changes[prev_item[0]] = (prev_item[1], current_item[1])

    return changes


test_db_files = [
    get_db_header(os.path.join(os.path.dirname(__file__), 'test_db_files', 'compare_header_1.sqlite')),
    get_db_header(os.path.join(os.path.dirname(__file__), 'test_db_files', 'compare_header_2.sqlite'))
]

compare_database_header_params = [
    ('not a header', 'not a header', -1),
    ('not a header', test_db_files[0], -1),
    (test_db_files[0], 'not a header', -1),
    (test_db_files[0], test_db_files[1], find_header_changes(test_db_files[0], test_db_files[1])),
    (test_db_files[0], test_db_files[0], {})
]


@pytest.mark.parametrize('prev_header, current_header, expected_changes', compare_database_header_params)
def test_compare_database_headers(prev_header, current_header, expected_changes):
    # If expected_changes == -1, expect a ValueError to test type matching in function
    if expected_changes == -1:
        with pytest.raises(ValueError):
            compare_database_headers(prev_header, current_header)

    else:
        assert compare_database_headers(prev_header, current_header) == expected_changes
