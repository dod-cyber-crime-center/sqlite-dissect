import io
import sqlite3
import sys
from contextlib import redirect_stdout
from hashlib import md5
from io import StringIO

import pytest

import sqlite_dissect.tests.nist_assertions as nist_assertions
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.entrypoint import main
from sqlite_dissect.tests import nist_assertions
from sqlite_dissect.tests.constants import LOG_FILES
from sqlite_dissect.tests.utilities import db_file, parse_csv
from sqlite_dissect.utilities import get_sqlite_files, parse_args


def get_md5_hash(filepath):
    with open(filepath, "rb") as file_obj:
        return md5(file_obj.read()).digest()


def fetch_pragma(db_cursor, pragma_func):
    db_cursor.execute("PRAGMA %s" % pragma_func)
    return db_cursor.fetchone()[0]


# SFT-01
def test_header_reporting(db_file):
    if "SFT-01" not in db_file[0].name:
        pytest.skip("Skipping SFT-03 db's")

    db_filepath = str(db_file[0].resolve())
    hash_before_parsing = get_md5_hash(db_filepath)

    args = parse_args([db_filepath, "--header"])
    sqlite_files = get_sqlite_files(args.sqlite_path)

    with redirect_stdout(io.StringIO()) as output:
        main(args, sqlite_files[0], len(sqlite_files) > 1)

        reported_page_size = None
        reported_journal_mode_read = None
        reported_journal_mode_write = None
        reported_num_pages = None
        reported_encoding = None

        for line in output.getvalue().splitlines():
            if "FILE FORMAT WRITE VERSION" in line.upper():
                reported_journal_mode_write = line.split(": ")[1].strip()
            elif "FILE FORMAT READ VERSION" in line.upper():
                reported_journal_mode_read = line.split(": ")[1].strip()
            elif "PAGE SIZE" in line.upper():
                reported_page_size = int(line.split(": ")[1].strip())
            elif "DATABASE SIZE IN PAGES" in line.upper():
                reported_num_pages = int(line.split(": ")[1].strip())
            elif "DATABASE TEXT ENCODING" in line.upper():
                reported_encoding = line.split(": ")[1].strip()

        actual_database = sqlite3.connect(db_filepath)
        db_cursor = actual_database.cursor()

        actual_page_size = fetch_pragma(db_cursor, "page_size")
        actual_journal_mode = fetch_pragma(db_cursor, "journal_mode")
        actual_num_pages = fetch_pragma(db_cursor, "page_count")
        actual_encoding = fetch_pragma(db_cursor, "encoding")

        hash_after_parsing = get_md5_hash(db_filepath)

        nist_assertions.assert_md5_equals(
            hash_before_parsing, hash_after_parsing, db_file[0].name
        )
        nist_assertions.assert_file_exists(db_filepath)
        nist_assertions.assert_correct_page_size(reported_page_size, actual_page_size)
        nist_assertions.assert_correct_journal_mode(
            reported_journal_mode_read, actual_journal_mode, "r"
        )
        nist_assertions.assert_correct_journal_mode(
            reported_journal_mode_write, actual_journal_mode, "w"
        )
        nist_assertions.assert_correct_num_pages(reported_num_pages, actual_num_pages)
        nist_assertions.assert_correct_encoding(reported_encoding, actual_encoding)
