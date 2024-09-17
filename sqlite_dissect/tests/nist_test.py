import sqlite3
from sqlite_dissect.tests.constants import LOG_FILES
import sqlite_dissect.tests.nist_assertions as nist_assertions
from hashlib import md5
from sqlite_dissect.entrypoint import main
import io
import sys
import pytest

from contextlib import redirect_stdout
from hashlib import md5
from io import StringIO
from sqlite_dissect.entrypoint import main
from sqlite_dissect.constants import FILE_TYPE
from sqlite_dissect.tests import nist_assertions
from sqlite_dissect.tests.utilities import db_file, parse_csv
from sqlite_dissect.utilities import get_sqlite_files, parse_args


def get_md5_hash(filepath):
    with open(filepath, 'rb') as file_obj:
        return md5(file_obj.read()).digest()


def fetch_pragma(db_cursor, pragma_func):
    db_cursor.execute('PRAGMA %s' % pragma_func)
    return db_cursor.fetchone()[0]


# SFT-01
def test_header_reporting(db_file):
    if 'SFT-01' not in db_file[0].name:
        pytest.skip("Skipping SFT-03 db's")

    db_filepath = str(db_file[0].resolve())
    hash_before_parsing = get_md5_hash(db_filepath)

    args = parse_args([db_filepath, '--header'])
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
                reported_journal_mode_write = line.split(': ')[1].strip()
            elif "FILE FORMAT READ VERSION" in line.upper():
                reported_journal_mode_read = line.split(': ')[1].strip()
            elif "PAGE SIZE" in line.upper():
                reported_page_size = int(line.split(': ')[1].strip())
            elif "DATABASE SIZE IN PAGES" in line.upper():
                reported_num_pages = int(line.split(': ')[1].strip())
            elif "DATABASE TEXT ENCODING" in line.upper():
                reported_encoding = line.split(': ')[1].strip()

        actual_database = sqlite3.connect(db_filepath)
        db_cursor = actual_database.cursor()

        actual_page_size = fetch_pragma(db_cursor, 'page_size')
        actual_journal_mode = fetch_pragma(db_cursor, 'journal_mode')
        actual_num_pages = fetch_pragma(db_cursor, 'page_count')
        actual_encoding = fetch_pragma(db_cursor, 'encoding')

        hash_after_parsing = get_md5_hash(db_filepath)

        nist_assertions.assert_md5_equals(hash_before_parsing, hash_after_parsing, db_file[0].name)
        nist_assertions.assert_file_exists(db_filepath)
        nist_assertions.assert_correct_page_size(reported_page_size, actual_page_size)
        nist_assertions.assert_correct_journal_mode(reported_journal_mode_read, actual_journal_mode, 'r')
        nist_assertions.assert_correct_journal_mode(reported_journal_mode_write, actual_journal_mode, 'w')
        nist_assertions.assert_correct_num_pages(reported_num_pages, actual_num_pages)
        nist_assertions.assert_correct_encoding(reported_encoding, actual_encoding)


# SFT-02
def test_schema_reporting(db_file, tmp_path):
    if 'SFT-01' not in db_file[0].name:
        pytest.skip("Skipping SFT-03 db's")

    db_filepath = str(db_file[0].resolve())
    hash_before_parsing = get_md5_hash(db_filepath)

    args = parse_args([db_filepath])
    sqlite_files = get_sqlite_files(args.sqlite_path)
    log_file = tmp_path / "log.txt"

    with open(str(log_file), 'w') as stdout:
        sys.stdout = stdout
        main(args, sqlite_files[0], len(sqlite_files) > 1)

    reported_tables = []
    reported_columns = {}
    reported_num_rows = {}
    current_table = None
    row_count = 0
    with open(str(log_file), 'r') as stdout:
        for line in stdout:
            line = line.strip()
            if "Master schema entry: " in line and "row type: table" in line:
                current_table = line[line.find("Master schema entry: "):line.find("row type: ")].split(': ')[1].strip()
                reported_tables.append(current_table)
                reported_columns[current_table] = []

                create_statement = line[line.find("sql: "):].split(': ')[1].strip()
                columns = create_statement[create_statement.find("(") + 1:create_statement.find(")")].split(',')
                for column in columns:
                    reported_columns[current_table].append(column.strip().split()[0])

            elif "File Type: " in line and current_table:
                row_count += 1

            elif line == '-' * 15:
                reported_num_rows[current_table] = row_count
                current_table = None
                row_count = 0

        actual_database = sqlite3.connect(db_filepath)
        db_cursor = actual_database.cursor()
        db_cursor.execute("SELECT tbl_name, sql FROM sqlite_master WHERE type='table'")

        actual_tables = []
        actual_columns = {}
        actual_num_rows = {}
        for table in db_cursor.fetchall():
            actual_tables.append(table[0])
            actual_columns[table[0]] = []

            columns = table[1][table[1].find("(")+1:table[1].find(")")]
            for column in columns.split(","):
                actual_columns[table[0]].append(column.strip().split()[0])

            db_cursor.execute("SELECT COUNT(*) FROM %s" % table[0])
            actual_num_rows[table[0]] = int(db_cursor.fetchone()[0])

        hash_after_parsing = get_md5_hash(db_filepath)

        nist_assertions.assert_md5_equals(hash_before_parsing, hash_after_parsing, db_file[0].name)
        nist_assertions.assert_file_exists(db_filepath)
        nist_assertions.assert_correct_tables(reported_tables, actual_tables)

        for table in reported_columns:
            nist_assertions.assert_correct_columns(reported_columns[table], actual_columns[table], table)
            nist_assertions.assert_correct_num_pages(reported_num_rows[table], actual_num_rows[table])
