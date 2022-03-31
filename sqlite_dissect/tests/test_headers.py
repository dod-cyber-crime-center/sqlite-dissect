import pytest
from struct import unpack
from sqlite_dissect.constants import MAXIMUM_PAGE_SIZE, MAXIMUM_PAGE_SIZE_INDICATOR
from hashlib import md5
from sqlite_dissect.file.database.header import DatabaseHeader
import os

SQLITE_DATABASE_HEADER_LENGTH = 100


@pytest.mark.parametrize('filepath', [os.path.join(os.path.dirname(__file__), 'sample_db')])
class TestDatabaseHeader:
    # assumes that the file contains a valid header and parses header data for consumption by tests.
    def __init__(self, filepath):
        for f in os.listdir(filepath):
            with open(f, 'rb') as db_file:
                self.database_header_byte_array = db_file.read(100)

            self.magic_header_string = self.database_header_byte_array[0:16]
            self.page_size = unpack(b">H", self.database_header_byte_array[16:18])[0]

            if self.page_size == MAXIMUM_PAGE_SIZE_INDICATOR:
                self.page_size = MAXIMUM_PAGE_SIZE

            self.file_format_write_version = ord(self.database_header_byte_array[18:19])
            self.file_format_read_version = ord(self.database_header_byte_array[19:20])
            self.reserved_bytes_per_page = ord(self.database_header_byte_array[20:21])
            self.maximum_embedded_payload_fraction = ord(self.database_header_byte_array[21:22])
            self.minimum_embedded_payload_fraction = ord(self.database_header_byte_array[22:23])
            self.leaf_payload_fraction = ord(self.database_header_byte_array[23:24])
            self.file_change_counter = unpack(b">I", self.database_header_byte_array[24:28])[0]
            self.database_size_in_pages = unpack(b">I", self.database_header_byte_array[28:32])[0]
            self.first_freelist_trunk_page_number = unpack(b">I", self.database_header_byte_array[32:36])[0]
            self.number_of_freelist_pages = unpack(b">I", self.database_header_byte_array[36:40])[0]
            self.schema_cookie = unpack(b">I", self.database_header_byte_array[40:44])[0]
            self.schema_format_number = unpack(b">I", self.database_header_byte_array[44:48])[0]
            self.default_page_cache_size = unpack(b">I", self.database_header_byte_array[48:52])[0]
            self.largest_root_b_tree_page_number = unpack(b">I", self.database_header_byte_array[52:56])[0]
            self.database_text_encoding = unpack(b">I", self.database_header_byte_array[56:60])[0]
            self.user_version = unpack(b">I", self.database_header_byte_array[60:64])[0]
            self.incremental_vacuum_mode = unpack(b">I", self.database_header_byte_array[64:68])[0]
            self.application_id = unpack(b">I", self.database_header_byte_array[68:72])[0]
            self.reserved_for_expansion = self.database_header_byte_array[72:92]
            self.version_valid_for_number = unpack(b">I", self.database_header_byte_array[92:96])[0]
            self.sqlite_version_number = unpack(b">I", self.database_header_byte_array[96:100])[0]
            self.md5_hex_digest = md5(self.database_header_byte_array).hexdigest().upper()

    # checks for correct parsing of header values in accordance with sqlite3 docs.
    def test_db_header_parsing(self):
        db_header = DatabaseHeader(self.database_header_byte_array)
        assert db_header.magic_header_string == self.magic_header_string
        assert db_header.page_size == self.page_size
        assert db_header.file_format_write_version == self.file_format_write_version
        assert db_header.file_format_read_version == self.file_format_read_version
        assert db_header.reserved_bytes_per_page == self.reserved_bytes_per_page
        assert db_header.maximum_embedded_payload_fraction == self.maximum_embedded_payload_fraction
        assert db_header.minimum_embedded_payload_fraction == self.minimum_embedded_payload_fraction
        assert db_header.leaf_payload_fraction == self.leaf_payload_fraction
        assert db_header.file_change_counter == self.file_change_counter
        assert db_header.database_size_in_pages == self.database_size_in_pages
        assert db_header.first_freelist_trunk_page_number == self.first_freelist_trunk_page_number
        assert db_header.number_of_freelist_pages == self.number_of_freelist_pages
        assert db_header.schema_cookie == self.schema_cookie
        assert db_header.schema_format_number == self.schema_format_number
        assert db_header.default_page_cache_size == self.default_page_cache_size
        assert db_header.largest_root_b_tree_page_number == self.largest_root_b_tree_page_number
        assert db_header.database_text_encoding == self.database_text_encoding
        assert db_header.user_version == self.user_version
        assert db_header.incremental_vacuum_mode == self.incremental_vacuum_mode
        assert db_header.application_id == self.application_id
        assert db_header.reserved_for_expansion == self.reserved_for_expansion
        assert db_header.version_valid_for_number == self.version_valid_for_number
        assert db_header.sqlite_version_number == self.sqlite_version_number
        assert db_header.md5_hex_digest == self.md5_hex_digest



