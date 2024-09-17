import pytest
from struct import unpack
from sqlite_dissect.constants import *
from hashlib import md5
from sqlite_dissect.file.database.header import DatabaseHeader
import os
from sqlite_dissect.tests.constants import *
from sqlite_dissect.file.wal.header import WriteAheadLogHeader, WriteAheadLogFrameHeader
from sqlite_dissect.file.wal_index.header import WriteAheadLogIndexHeader, WriteAheadLogIndexSubHeader, WriteAheadLogIndexCheckpointInfo
from sqlite_dissect.exception import HeaderParsingError
from sqlite_dissect.utilities import get_md5_hash
from sqlite_dissect.file.journal.header import RollbackJournalHeader
from sqlite_dissect.tests.utilities import replace_bytes

SQLITE_DATABASE_HEADER_LENGTH = 100

# TODO: Split into multiple files to mimic project file structure.

test_db_header_init_params = [
    ("none", SUCCESS),
    ("invalid_header_small", VALUE_ERROR),
    ("invalid_header_large", VALUE_ERROR),
    ("invalid_magic_bytes", HEADER_ERROR),
    ("invalid_page_size_large", HEADER_ERROR),
    ("invalid_page_size_small", HEADER_ERROR),
    ("invalid_file_format_write", HEADER_ERROR),
    ("invalid_file_format_read", HEADER_ERROR),
    ("invalid_reserved_bytes", NOT_IMPLEMENTED_ERROR),
    ("invalid_maximum_embedded_payload_fraction", HEADER_ERROR),
    ("invalid_minimum_embedded_payload_fraction", HEADER_ERROR),
    ("invalid_leaf_payload_fraction", HEADER_ERROR),
    ("no_data", RUNTIME_WARNING),
    ("invalid_schema_format", HEADER_ERROR),
    ("invalid_database_text_encoding", HEADER_ERROR),
    ("invalid_vacuum_mode", HEADER_ERROR),
    ("invalid_reserve_space", HEADER_ERROR)
]


@pytest.fixture(params=test_db_header_init_params)
def permuted_database(request):
    with open(os.path.join(DB_FILES, "chinook.sqlite"), 'rb') as db_file:
        db_header_bytes = db_file.read(SQLITE_DATABASE_HEADER_LENGTH)

        if request.param[0] == "invalid_header_small":
            db_header_bytes = db_header_bytes[:90]
        elif request.param[0] == "invalid_header_large":
            db_header_bytes = db_header_bytes + 10 * b"\x00"
        elif request.param[0] == "invalid_magic_bytes":
            db_header_bytes = replace_bytes(db_header_bytes, 10*b"\x00", 0)
        elif request.param[0] == "invalid_page_size_large":
            db_header_bytes = replace_bytes(db_header_bytes, b"\xF0\x00", 16)
        elif request.param[0] == "invalid_page_size_small":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00", 16)
        elif request.param[0] == "invalid_file_format_write":
            db_header_bytes = replace_bytes(db_header_bytes, b"\xFF", 18)
        elif request.param[0] == "invalid_file_format_read":
            db_header_bytes = replace_bytes(db_header_bytes, b"\xFF", 19)
        elif request.param[0] == "invalid_reserved_bytes":
            db_header_bytes = replace_bytes(db_header_bytes, b"\xFF", 20)
        elif request.param[0] == "invalid_maximum_embedded_payload_fraction":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00", 21)
        elif request.param[0] == "invalid_minimum_embedded_payload_fraction":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00", 22)
        elif request.param[0] == "invalid_leaf_payload_fraction":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00", 23)
        elif request.param[0] == "no_data":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00\x00\x00", 44)
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00\x00\x00", 56)
        elif request.param[0] == "invalid_schema_format":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00\x00\xFF", 44)
        elif request.param[0] == "invalid_database_text_encoding":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00\x00\xFF", 56)
        elif request.param[0] == "invalid_vacuum_mode":
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00\x00\x00", 52)
            db_header_bytes = replace_bytes(db_header_bytes, b"\x00\x00\x00\x01", 64)
        elif request.param[0] == "invalid_reserve_space":
            db_header_bytes = replace_bytes(db_header_bytes, b"\xFF", 72)

        return {"byte_array": db_header_bytes, "expected_result": request.param[1]}


def test_db_header_init(permuted_database):
    if permuted_database["expected_result"] == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = DatabaseHeader(permuted_database["byte_array"])

    elif permuted_database["expected_result"] == TYPE_ERROR:
        with pytest.raises(TypeError):
            _ = DatabaseHeader(permuted_database["byte_array"])

    elif permuted_database["expected_result"] == NOT_IMPLEMENTED_ERROR:
        with pytest.raises(NotImplementedError):
            _ = DatabaseHeader(permuted_database["byte_array"])

    elif permuted_database["expected_result"] == RUNTIME_WARNING:
        with pytest.warns(RuntimeWarning):
            _ = DatabaseHeader(permuted_database["byte_array"])

    elif permuted_database["expected_result"] == HEADER_ERROR:
        with pytest.raises(HeaderParsingError):
            _ = DatabaseHeader(permuted_database["byte_array"])

    else:
        db_header = DatabaseHeader(permuted_database["byte_array"])

        assert db_header.magic_header_string == permuted_database["byte_array"][0:16]
        assert db_header.page_size == MAXIMUM_PAGE_SIZE \
            if unpack(b">H", permuted_database["byte_array"][16:18])[0] == MAXIMUM_PAGE_SIZE_INDICATOR \
            else unpack(b">H", permuted_database["byte_array"][16:18])[0]
        assert db_header.file_format_write_version == ord(permuted_database["byte_array"][18:19])
        assert db_header.file_format_read_version == ord(permuted_database["byte_array"][19:20])
        assert db_header.reserved_bytes_per_page == ord(permuted_database["byte_array"][20:21])
        assert db_header.maximum_embedded_payload_fraction == ord(permuted_database["byte_array"][21:22])
        assert db_header.minimum_embedded_payload_fraction == ord(permuted_database["byte_array"][22:23])
        assert db_header.leaf_payload_fraction == ord(permuted_database["byte_array"][23:24])
        assert db_header.file_change_counter == unpack(b">I", permuted_database["byte_array"][24:28])[0]
        assert db_header.database_size_in_pages == unpack(b">I", permuted_database["byte_array"][28:32])[0]
        assert db_header.first_freelist_trunk_page_number == \
               unpack(b">I", permuted_database["byte_array"][32:36])[0]
        assert db_header.number_of_freelist_pages == unpack(b">I", permuted_database["byte_array"][36:40])[0]
        assert db_header.schema_cookie == unpack(b">I", permuted_database["byte_array"][40:44])[0]
        assert db_header.schema_format_number == unpack(b">I", permuted_database["byte_array"][44:48])[0]
        assert db_header.default_page_cache_size == unpack(b">I", permuted_database["byte_array"][48:52])[0]
        assert db_header.largest_root_b_tree_page_number == \
               unpack(b">I", permuted_database["byte_array"][52:56])[0]
        assert db_header.database_text_encoding == unpack(b">I", permuted_database["byte_array"][56:60])[0]
        assert db_header.user_version == unpack(b">I", permuted_database["byte_array"][60:64])[0]
        assert db_header.incremental_vacuum_mode == unpack(b">I", permuted_database["byte_array"][64:68])[0]
        assert db_header.application_id == unpack(b">I", permuted_database["byte_array"][68:72])[0]
        assert db_header.reserved_for_expansion == permuted_database["byte_array"][72:92]
        assert db_header.version_valid_for_number == unpack(b">I", permuted_database["byte_array"][92:96])[0]
        assert db_header.sqlite_version_number == unpack(b">I", permuted_database["byte_array"][96:100])[0]
        assert db_header.md5_hex_digest == md5(permuted_database["byte_array"]).hexdigest().upper()


with open(os.path.join(DB_FILES, "chinook.sqlite-wal"), "rb") as wal_file:
    wal_byte_array = wal_file.read(WAL_HEADER_LENGTH)

test_write_ahead_log_header_params = [
    (wal_byte_array, SUCCESS),
    (wal_byte_array[:12], VALUE_ERROR),
    (replace_bytes(wal_byte_array, b"\x00\x00\x00\x00", 0), HEADER_ERROR),
    (replace_bytes(wal_byte_array, b"\x00\x00\x00\x00", 4), HEADER_ERROR),
    # TODO: Not warning like it should despite warning in log??? (replace_bytes(wal_byte_array, b"\x00\x00\x00\xFF", 12), RUNTIME_WARNING)
]


@pytest.mark.parametrize('byte_array, expected_value', test_write_ahead_log_header_params)
def test_write_ahead_log_header_init(byte_array, expected_value):
    if expected_value == HEADER_ERROR:
        with pytest.raises(HeaderParsingError):
            _ = WriteAheadLogHeader(byte_array)

    elif expected_value == RUNTIME_WARNING:
        with pytest.warns(RuntimeWarning):
            _ = WriteAheadLogHeader(byte_array)

    elif expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = WriteAheadLogHeader(byte_array)

    else:
        wal_header = WriteAheadLogHeader(byte_array)

        assert wal_header.magic_number == unpack(b">I", byte_array[0:4])[0]
        assert wal_header.file_format_version == unpack(b">I", byte_array[4:8])[0]
        assert wal_header.page_size == unpack(b">I", byte_array[8:12])[0]
        assert wal_header.checkpoint_sequence_number == unpack(b">I", byte_array[12:16])[0]
        assert wal_header.salt_1 == unpack(b">I", byte_array[16:20])[0]
        assert wal_header.salt_2 == unpack(b">I", byte_array[20:24])[0]
        assert wal_header.checksum_1 == unpack(b">I", byte_array[24:28])[0]
        assert wal_header.checksum_2 == unpack(b">I", byte_array[28:32])[0]
        assert wal_header.md5_hex_digest == get_md5_hash(byte_array)


with open(os.path.join(DB_FILES, "chinook.sqlite-shm"), 'rb') as wal_index_file:
    wal_index_byte_array = wal_index_file.read(WAL_INDEX_HEADER_LENGTH)

test_wal_index_header_params = [
    (wal_index_byte_array, SUCCESS),
    (wal_index_byte_array[:100], VALUE_ERROR)
]


@pytest.mark.parametrize("wal_index_header_byte_array, expected_value", test_wal_index_header_params)
def test_wal_index_header_init(wal_index_header_byte_array, expected_value):
    if expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = WriteAheadLogIndexHeader(wal_index_header_byte_array)

    else:
        checkpoint_start_offset = WAL_INDEX_NUMBER_OF_SUB_HEADERS * WAL_INDEX_SUB_HEADER_LENGTH
        checkpoint_end_offset = checkpoint_start_offset + WAL_INDEX_CHECKPOINT_INFO_LENGTH
        lock_reserved_start_offset = checkpoint_end_offset
        lock_reserved_end_offset = lock_reserved_start_offset + WAL_INDEX_LOCK_RESERVED_LENGTH

        wal_index_header = WriteAheadLogIndexHeader(wal_index_header_byte_array)

        assert wal_index_header.page_size == unpack(b"<H", wal_index_header_byte_array[14:16])[0]
        assert wal_index_header.endianness == ENDIANNESS.LITTLE_ENDIAN
        assert wal_index_header.lock_reserved == wal_index_header_byte_array[lock_reserved_start_offset:lock_reserved_end_offset]
        assert wal_index_header.md5_hex_digest == get_md5_hash(wal_index_header_byte_array)


with open(os.path.join(DB_FILES, "chinook.sqlite-shm"), 'rb') as wal_index_file:
    test_wal_index_sub_header_byte_array = wal_index_file.read(WAL_INDEX_SUB_HEADER_LENGTH)

test_wal_index_sub_header_params = [
    (test_wal_index_sub_header_byte_array, 0, SUCCESS),
    (test_wal_index_sub_header_byte_array[:30], 0, VALUE_ERROR),
    (test_wal_index_sub_header_byte_array, 10, VALUE_ERROR),
    (test_wal_index_sub_header_byte_array, -3, VALUE_ERROR),
    (replace_bytes(test_wal_index_sub_header_byte_array, b"\x00\x00\x00\x00", 0), 0, HEADER_ERROR),
    (replace_bytes(test_wal_index_sub_header_byte_array, b"\x00\x2D\xE2\x18", 0), 0, NOT_IMPLEMENTED_ERROR)
]


@pytest.mark.parametrize("wal_index_sub_header_byte_array, index, expected_value", test_wal_index_sub_header_params)
def test_wal_index_sub_header_init(wal_index_sub_header_byte_array, index, expected_value):
    if expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = WriteAheadLogIndexSubHeader(index, wal_index_sub_header_byte_array)

    elif expected_value == HEADER_ERROR:
        with pytest.raises(HeaderParsingError):
            _ = WriteAheadLogIndexSubHeader(index, wal_index_sub_header_byte_array)

    elif expected_value == NOT_IMPLEMENTED_ERROR:
        with pytest.raises(NotImplementedError):
            _ = WriteAheadLogIndexSubHeader(index, wal_index_sub_header_byte_array)

    else:
        wal_index_sub_header = WriteAheadLogIndexSubHeader(index, wal_index_sub_header_byte_array)

        assert wal_index_sub_header.index == index
        assert wal_index_sub_header.endianness == ENDIANNESS.LITTLE_ENDIAN
        assert wal_index_sub_header.file_format_version == unpack(b"<I", wal_index_sub_header_byte_array[0:4])[0]
        assert wal_index_sub_header.unused_padding_field == unpack(b"<I", wal_index_sub_header_byte_array[4:8])[0]
        assert wal_index_sub_header.change_counter == unpack(b"<I", wal_index_sub_header_byte_array[8:12])[0]
        assert wal_index_sub_header.initialized == ord(wal_index_sub_header_byte_array[12:13])
        assert wal_index_sub_header.checksums_in_big_endian == ord(wal_index_sub_header_byte_array[13:14])
        assert wal_index_sub_header.page_size == unpack(b"<H", wal_index_sub_header_byte_array[14:16])[0]
        assert wal_index_sub_header.last_valid_frame_index == unpack(b"<I", wal_index_sub_header_byte_array[16:20])[0]
        assert wal_index_sub_header.database_size_in_pages == unpack(b"<I", wal_index_sub_header_byte_array[20:24])[0]
        assert wal_index_sub_header.frame_checksum_1 == unpack(b"<I", wal_index_sub_header_byte_array[24:28])[0]
        assert wal_index_sub_header.frame_checksum_2 == unpack(b"<I", wal_index_sub_header_byte_array[28:32])[0]
        assert wal_index_sub_header.salt_1 == unpack(b"<I", wal_index_sub_header_byte_array[32:36])[0]
        assert wal_index_sub_header.salt_2 == unpack(b"<I", wal_index_sub_header_byte_array[36:40])[0]
        assert wal_index_sub_header.checksum_1 == unpack(b"<I", wal_index_sub_header_byte_array[40:44])[0]
        assert wal_index_sub_header.checksum_2 == unpack(b"<I", wal_index_sub_header_byte_array[44:48])[0]

        # TODO: Replace utilities get_md5_hash with independent implementation
        assert wal_index_sub_header.md5_hex_digest == get_md5_hash(wal_index_sub_header_byte_array)


checkpoint_info_start_offset = WAL_INDEX_NUMBER_OF_SUB_HEADERS * WAL_INDEX_SUB_HEADER_LENGTH
checkpoint_info_end_offset = checkpoint_info_start_offset + WAL_INDEX_CHECKPOINT_INFO_LENGTH
test_wal_index_checkpoint_info_byte_array = wal_index_byte_array[checkpoint_info_start_offset:checkpoint_info_end_offset]

test_wal_index_checkpoint_info_params = [
    (test_wal_index_checkpoint_info_byte_array, ENDIANNESS.LITTLE_ENDIAN, SUCCESS),
    (test_wal_index_checkpoint_info_byte_array[:10], ENDIANNESS.LITTLE_ENDIAN, VALUE_ERROR)
]


@pytest.mark.parametrize("wal_index_checkpoint_info_byte_array, endianness, expected_value", test_wal_index_checkpoint_info_params)
def test_wal_index_checkpoint_info_init(wal_index_checkpoint_info_byte_array, endianness, expected_value):
    if expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = WriteAheadLogIndexCheckpointInfo(wal_index_checkpoint_info_byte_array, endianness)

    else:
        wal_index_checkpoint_info = WriteAheadLogIndexCheckpointInfo(wal_index_checkpoint_info_byte_array, endianness)

        assert wal_index_checkpoint_info.endianness == endianness
        assert wal_index_checkpoint_info.number_of_frames_backfilled_in_database == unpack(b"<I", wal_index_checkpoint_info_byte_array[0:4])[0]
        assert len(wal_index_checkpoint_info.reader_marks) == WAL_INDEX_READER_MARK_SIZE

        for index in range(WAL_INDEX_READER_MARK_SIZE):
            start_offset = index * WAL_INDEX_READER_MARK_LENGTH
            start_offset += WAL_INDEX_NUMBER_OF_FRAMES_BACKFILLED_IN_DATABASE_LENGTH
            end_offset = start_offset + WAL_INDEX_READER_MARK_LENGTH

            assert wal_index_checkpoint_info.reader_marks[index] == unpack(b"<I", wal_index_checkpoint_info_byte_array[start_offset:end_offset])[0]

        assert wal_index_checkpoint_info.md5_hex_digest == get_md5_hash(wal_index_checkpoint_info_byte_array)


with open(os.path.join(DB_FILES, "chinook.sqlite-journal"), 'rb') as journal_file:
    journal_header_byte_array = journal_file.read(ROLLBACK_JOURNAL_HEADER_LENGTH)

test_journal_header_params = [
    (journal_header_byte_array, SUCCESS),
    (journal_header_byte_array[:10], VALUE_ERROR),
    # Not warning like it should despite warning in log??? (replace_bytes(journal_header_byte_array, 2*b"\x00\xFF\x00\xFF", 0), RUNTIME_WARNING)
]


@pytest.mark.parametrize("rollback_journal_header_byte_array, expected_value", test_journal_header_params)
def test_journal_header_init(rollback_journal_header_byte_array, expected_value):
    if expected_value == VALUE_ERROR:
        with pytest.raises(ValueError):
            _ = RollbackJournalHeader(rollback_journal_header_byte_array)

    elif expected_value == RUNTIME_WARNING:
        with pytest.warns(RuntimeWarning):
            _ = RollbackJournalHeader(rollback_journal_header_byte_array)

    else:
        journal_header = RollbackJournalHeader(rollback_journal_header_byte_array)

        assert journal_header.header_string == rollback_journal_header_byte_array[0:8]
        
        assert journal_header.page_count == (
            ROLLBACK_JOURNAL_ALL_CONTENT_UNTIL_END_OF_FILE
            if rollback_journal_header_byte_array[8:12] == ROLLBACK_JOURNAL_HEADER_ALL_CONTENT
            else unpack(b">I", rollback_journal_header_byte_array[8:12])[0]
        )

        assert journal_header.random_nonce_for_checksum == unpack(b">I", rollback_journal_header_byte_array[12:16])[0]
        assert journal_header.initial_size_of_database_in_pages == unpack(b">I", rollback_journal_header_byte_array[16:20])[0]
        assert journal_header.disk_sector_size == unpack(b">I", rollback_journal_header_byte_array[20:24])[0]
        assert journal_header.page_size == journal_header.size_of_pages_in_journal == unpack(b">I", rollback_journal_header_byte_array[24:28])[0]
        assert journal_header.md5_hex_digest == get_md5_hash(rollback_journal_header_byte_array)
