import pytest
import os
from sqlite_dissect.file.file_handle import FileHandle
from sqlite_dissect.constants import FILE_TYPE, UTF_8_DATABASE_TEXT_ENCODING, UTF_16LE_DATABASE_TEXT_ENCODING, UTF_8, UTF_16LE
from sqlite_dissect.exception import HeaderParsingError
from sqlite_dissect.tests.constants import *

# TODO: generate chinook.sqlite (chinook standard database)
# TODO: generate invalid_database.sqlite (empty file)
# TODO: generate invalid_wal.sqlite-wal (empty file)
# TODO: generate invalid_journal.sqlite-journal (empty file)
# TODO: generate invalid_wal_index.shm (empty file)
# TODO: generate inaccessible.sqlite (read-protected file)
file_handle_construction_params = [
    (FILE_TYPE.DATABASE, DB_FILES, None, None, IO_ERROR),
    (FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'does_not_exist.sqlite'), None, None, IO_ERROR),
    (FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'inaccessible.sqlite'), None, None, VALUE_ERROR),
    (FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, 1073741825, NOT_IMPLEMENTED_ERROR),
    (FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), UTF_8_DATABASE_TEXT_ENCODING, None, VALUE_ERROR),
    (FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'invalid_database.sqlite'), None, None, HEADER_ERROR),
    (FILE_TYPE.WAL, os.path.join(DB_FILES, 'invalid_wal.sqlite-wal'), None, None, VALUE_ERROR),
    (FILE_TYPE.ROLLBACK_JOURNAL, os.path.join(DB_FILES, 'invalid_journal.sqlite-journal'), None, None, VALUE_ERROR),
    (FILE_TYPE.WAL_INDEX, os.path.join(DB_FILES, 'invalid_wal_index.sqlite-shm'), None, None, VALUE_ERROR),
    ('Invalid type', os.path.join(DB_FILES, 'chinook.sqlite'), None, None, VALUE_ERROR),
    (FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, None, FileHandle(FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, None)),
    (FILE_TYPE.WAL, os.path.join(DB_FILES, 'chinook.sqlite-wal'), None, None, FileHandle(FILE_TYPE.WAL, os.path.join(DB_FILES, 'chinook.sqlite-wal'), None, None)),
    (FILE_TYPE.WAL_INDEX, os.path.join(DB_FILES, 'chinook.sqlite-shm'), None, None, FileHandle(FILE_TYPE.WAL_INDEX, os.path.join(DB_FILES, 'chinook.sqlite-shm'), None, None)),
    (FILE_TYPE.ROLLBACK_JOURNAL, os.path.join(DB_FILES, 'chinook.sqlite-journal'), None, None, FileHandle(FILE_TYPE.ROLLBACK_JOURNAL, os.path.join(DB_FILES, 'chinook.sqlite-journal'), None, None))
]


@pytest.mark.parametrize('file_type, file_identifier, database_text_encoding, file_size, expected_value',
                         file_handle_construction_params)
def test_file_handle_construction(file_type, file_identifier, database_text_encoding, file_size, expected_value):
    if expected_value == IO_ERROR:
        with pytest.raises(IOError):
            _ = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    elif expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    elif expected_value == NOT_IMPLEMENTED_ERROR:
        with pytest.raises(NotImplementedError):
            _ = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    elif expected_value == HEADER_ERROR:
        with pytest.raises(HeaderParsingError):
            _ = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
    else:
        file_handle = FileHandle(file_type, file_identifier, database_text_encoding, file_size)
        assert str(file_handle) == str(expected_value)


database_text_encoding_params = [
    (UTF_8, SUCCESS),
    (UTF_16LE, TYPE_ERROR),
    (5, TYPE_ERROR)
]


@pytest.mark.parametrize('database_text_encoding, expected_value', database_text_encoding_params)
def test_database_text_encoding_setter(database_text_encoding, expected_value):
    file_handle = FileHandle(FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, None)

    if expected_value == TYPE_ERROR:
        with pytest.raises(TypeError):
            file_handle.database_text_encoding = database_text_encoding
    elif expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            file_handle.database_text_encoding = database_text_encoding
    else:
        file_handle.database_text_encoding = database_text_encoding
        assert True


@pytest.fixture(params=[RUNTIME_WARNING, SUCCESS])
def test_close_file_handle(request):
    if request.param == RUNTIME_WARNING:
        temp_file = open(os.path.join(DB_FILES, 'chinook.sqlite'))
        yield FileHandle(FILE_TYPE.DATABASE, temp_file, None, None), RUNTIME_WARNING
        temp_file.close()
    elif request.param == IO_ERROR:
        temp_file = open(os.path.join(DB_FILES, 'chinook.sqlite'))
        temp_file.close()
        yield FileHandle(FILE_TYPE.DATABASE, temp_file, None, None), IO_ERROR
    else:
        yield FileHandle(FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, None), SUCCESS


def test_close(test_close_file_handle):
    if test_close_file_handle[1] == RUNTIME_WARNING:
        with pytest.warns(RuntimeWarning):
            test_close_file_handle[0].close()
    elif test_close_file_handle[1] == IO_ERROR:
        with pytest.raises(IOError):
            test_close_file_handle[0].close()
    else:
        with pytest.warns(None) as w:
            test_close_file_handle[0].close()
            if w:
                assert False
            else:
                assert True


test_read_data_params = [
    (FileHandle(FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, 5), 10, 10, EOF_ERROR),
    (FileHandle(FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, 5), 4, 10, EOF_ERROR),
    (FileHandle(FILE_TYPE.DATABASE, os.path.join(DB_FILES, 'chinook.sqlite'), None, None), 0, 1, SUCCESS),
]


@pytest.mark.parametrize('file_handle, offset, number_of_bytes, expected', test_read_data_params)
def test_read_data(file_handle, offset, number_of_bytes, expected):
    if expected == EOF_ERROR:
        with pytest.raises(EOFError):
            file_handle.read_data(offset, number_of_bytes)
    elif expected == VALUE_ERROR:
        with pytest.raises(ValueError):
            file_handle.read_data(offset, number_of_bytes)
    else:
        file_handle.read_data(offset, number_of_bytes)
        assert True

