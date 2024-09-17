from sqlite_dissect.file.database.payload import *
from sqlite_dissect.constants import *
from sqlite_dissect.exception import *
from sqlite_dissect.tests.utilities import *
from sqlite_dissect.tests.constants import *
from sqlite_dissect.constants import SQLITE_DATABASE_HEADER_LENGTH
from struct import unpack
import pytest

with open(os.path.join(DB_FILES, "chinook.sqlite"), 'rb') as db_file:
    db_file.seek(16384)
    page = db_file.read(4096)
    first_cell_offset = unpack(b">H", page[8:10])[0]
    payload_byte_size, payload_bytes_length = decode_varint(page, first_cell_offset)
    _, rowid_length = decode_varint(page, first_cell_offset + payload_bytes_length)
    payload_offset = first_cell_offset + payload_bytes_length + rowid_length

test_record_params = [
    (page, payload_offset, payload_byte_size, payload_byte_size, b"", SUCCESS),
    (page, payload_offset, payload_byte_size, None, b"Arbitrary data", RECORD_ERROR),
    (page, payload_offset, payload_byte_size, payload_byte_size - 10, b"", RECORD_ERROR),
    (page, payload_offset, payload_byte_size, payload_byte_size + 10, b"", RECORD_ERROR),
    (page, payload_offset, payload_byte_size, payload_byte_size, b"Arbitrary data", RECORD_ERROR),
    (page, payload_offset, payload_byte_size, payload_byte_size + 100, b"Arbitrary data", RECORD_ERROR)
]


@pytest.mark.parametrize(
    "page, payload_offset, payload_byte_size, bytes_on_first_page, overflow, expected_value",
    test_record_params
)
def test_record_init(page, payload_offset, payload_byte_size, bytes_on_first_page, overflow, expected_value):
    if expected_value == RECORD_ERROR:
        with pytest.raises(RecordParsingError):
            _ = Record(page, payload_offset, payload_byte_size, bytes_on_first_page, overflow)

    else:
        record = Record(page, payload_offset, payload_byte_size, bytes_on_first_page, overflow)

        assert record.start_offset == payload_offset
        assert record.byte_size == payload_byte_size
        assert record.end_offset == payload_offset + bytes_on_first_page == record.body_end_offset
        assert record.has_overflow == (False if not overflow else True)
        assert record.bytes_on_first_page == bytes_on_first_page
        assert record.header_byte_size == decode_varint(page, payload_offset)[0]
        assert record.header_byte_size_varint_length == decode_varint(page, payload_offset)[1]
        assert record.header_start_offset == payload_offset
        assert record.header_end_offset == payload_offset + decode_varint(page, payload_offset)[0] \
               == record.body_start_offset

        total_record_content = page[payload_offset:payload_offset + bytes_on_first_page] + overflow
        assert record.md5_hex_digest == get_md5_hash(total_record_content)

        current_header_offset = decode_varint(page, payload_offset)[1]
        num_columns = 0
        serial_type_signature = ""
        while current_header_offset < decode_varint(page, payload_offset)[0]:
            serial_type, serial_type_varint_length = decode_varint(total_record_content, current_header_offset)
            serial_type_signature += str(get_serial_type_signature(serial_type))
            num_columns += 1
            current_header_offset += serial_type_varint_length

        assert record.serial_type_signature == serial_type_signature
        assert len(record.record_columns) == num_columns
