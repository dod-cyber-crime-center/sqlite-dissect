import pytest
from sqlite_dissect.tests.constants import *
from sqlite_dissect.tests.utilities import *
from sqlite_dissect.file.wal_index.wal_index import *

test_wal_index_init_params = [
    (os.path.join(DB_FILES, "chinook.sqlite-shm"), None)
]


# wal_index parsing not fully implemented; will be used at a later date.
@pytest.mark.parametrize("file_name, file_size", test_wal_index_init_params)
def test_wal_index_init(file_size, file_name):
    wal_index = WriteAheadLogIndex(file_name, file_size)

    assert True
