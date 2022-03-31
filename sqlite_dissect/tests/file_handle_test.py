import pytest
import os
from sqlite_dissect.file.file_handle import FileHandle
from sqlite_dissect.constants import FILE_TYPE, UTF_8_DATABASE_TEXT_ENCODING, UTF_16LE_DATABASE_TEXT_ENCODING
from sqlite_dissect.exception import HeaderParsingError

SUCCESS = 1
IO_ERROR = -1
VALUE_ERROR = -2
NOT_IMPLEMENTED_ERROR = -3
HEADER_ERROR = -4
TYPE_ERROR = -5
RUNTIME_WARNING = -6
DB_FILES = os.path.dirname(os.path.abspath(__file__)).join('..', 'test_files')

# TODO: generate file_handle_construction_test.sqlite (valid arbitrary db)
# TODO: generate invalid_database.sqlite (empty file)
# TODO: generate invalid_wal.sqlite-wal (empty file)
# TODO: generate invalid_journal.sqlite-journal (empty file)
# TODO: generate invalid_wal_index.shm (empty file)
file_handle_construction_params = [
    (FILE_TYPE.DATABASE, DB_FILES, None, None, IO_ERROR),
    (FILE_TYPE.DATABASE, DB_FILES.join('does_not_exist.sqlite'), None, None, IO_ERROR),
    (FILE_TYPE.DATABASE, DB_FILES.join('inaccessible.sqlite'), None, None, IO_ERROR),
    (FILE_TYPE.DATABASE, DB_FILES.join('file_handle_construction_test.sqlite'), None, 1073741825, NOT_IMPLEMENTED_ERROR),
    (FILE_TYPE.DATABASE, DB_FILES.join('file_handle_construction_test.sqlite'), UTF_8_DATABASE_TEXT_ENCODING, None, VALUE_ERROR),
    (FILE_TYPE.DATABASE, DB_FILES.join('invalid_database.sqlite'), None, None, HEADER_ERROR),
    (FILE_TYPE.WAL, DB_FILES.join('invalid_wal.sqlite-wal'), None, None, HEADER_ERROR),
    (FILE_TYPE.ROLLBACK_JOURNAL, DB_FILES.join('invalid_journal.sqlite-journal'), None, None, HEADER_ERROR),
    (FILE_TYPE.WAL_INDEX, DB_FILES.join('invalid_wal_index.shm'), None, None, HEADER_ERROR),
    ('Invalid type', DB_FILES.join('file_handle_construction_test.sqlite'), None, None, VALUE_ERROR),
    (FILE_TYPE.DATABASE, DB_FILES.join('file_handle_construction_test.sqlite'), None, None, FileHandle(FILE_TYPE.DATABASE, DB_FILES.join('file_handle_construction_test.sqlite'), None, None)),
    (FILE_TYPE.WAL, DB_FILES.join('file_handle_construction_test.sqlite-wal'), None, None, FileHandle(FILE_TYPE.WAL, DB_FILES.join('file_handle_construction_test.sqlite-wal'), None, None)),
    (FILE_TYPE.WAL_INDEX, DB_FILES.join('file_handle_construction_test.shm'), None, None, FileHandle(FILE_TYPE.WAL_INDEX, DB_FILES.join('file_handle_construction_test.shm'), None, None)),
    (FILE_TYPE.ROLLBACK_JOURNAL, DB_FILES.join('file_handle_construction_test.sqlite-journal'), None, None, FileHandle(FILE_TYPE.WAL_INDEX, DB_FILES.join('file_handle_construction_test.sqlite-journal'), None, None))
]


@pytest.mark.parametrize('file_type, file_identifier, database_text_encoding, file_size, expected_value',
                         file_handle_construction_params)
def test_file_handle_construction(file_type, file_identifier, database_text_encoding, file_size, expected_value):
    if expected_value == IO_ERROR:
        with pytest.raises(IOError):
            file_handle = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    elif expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            file_handle = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    elif expected_value == NOT_IMPLEMENTED_ERROR:
        with pytest.raises(NotImplementedError):
            file_handle = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    elif expected_value == HEADER_ERROR:
        with pytest.raises(HeaderParsingError):
            file_handle = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    else:
        file_handle = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
        assert str(file_handle) == str(expected_value)


database_text_encoding_params = [
    (UTF_8_DATABASE_TEXT_ENCODING, SUCCESS),
    (UTF_16LE_DATABASE_TEXT_ENCODING, TYPE_ERROR),
    (5, VALUE_ERROR)
]
database_text_encoding_file_handle = FileHandle(FILE_TYPE.DATABASE, DB_FILES.join('file_handle_database_text_encoding_test.sqlite'), None, None)


@pytest.mark.parametrize('database_text_encoding, expected_value', database_text_encoding_params)
def test_database_text_encoding_setter(database_text_encoding, expected_value):
    if expected_value == TYPE_ERROR:
        with pytest.raises(TypeError):
            database_text_encoding_file_handle.database_text_encoding = database_text_encoding
    elif expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            database_text_encoding_file_handle.database_text_encoding = database_text_encoding
    else:
        database_text_encoding_file_handle.database_text_encoding = database_text_encoding
        assert True

close_params = [

]

@pytest.mark.parametrize('file_handle, expected_state', close_params)
def test_close(file_handle, expected_state):
    if expected_state == RUNTIME_WARNING:
        with pytest.warns(RuntimeWarning):
            file_handle.close()
    elif expected_state == IO_ERROR:
        with pytest.raises(IOError):
            file_handle.close()
    else:
        file_handle.close()
        with pytest.warns(None) as w:
            file_handle.close()
            if w:
                assert False
            else:
                assert True


