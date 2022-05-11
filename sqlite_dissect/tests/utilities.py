import hashlib
import os
import pytest
import sqlite3
import random
import string
import re
from collections import OrderedDict
import uuid

def strip_one(string, pattern):
    return re.sub(pattern + '$', "", re.sub('^' + pattern, "", string))

def find_breakpoints(input_string, quote_chars = ["'", '"'], delim = ','):
    breakpoints = []

    in_quotes = None
    is_encapsulated = False
    last_char = None
    for index, character in enumerate(input_string):
        if in_quotes:
            if character == in_quotes:
                in_quotes = None

        elif is_encapsulated:
            if character == ']':
                is_encapsulated = False

        else:
            if character in quote_chars:
                in_quotes = character

            elif character == '[':
                is_encapsulated = True

            elif character == delim:
                breakpoints.append(index)

    return breakpoints

def parse_rows(row_string):
    commas = find_breakpoints(row_string)

    row_dict = {}
    row_list = [row_string[i:j].strip() for i,j in zip([0] + [index + 1 for index in commas], commas + [None])]

    for row in row_list:
        spaces = find_breakpoints(row, delim=' ')
        row_dict[strip_one(row[ : spaces[0]], '[\'"]').lstrip('[ ').rstrip('] ')] = row[spaces[0] : ].strip()

    return row_dict

def get_index_of_closing_parenthesis(string, opening_parenthesis_offset=0):
    in_quotes = None
    in_block_comment = False
    in_line_comment = False

    quote_chars = ['"', "'"]
    block_comment_chars = '/*'
    block_comment_term = '*/'
    line_comment_chars = '--'
    line_comment_term = '\n'

    for index, character in enumerate(string[opening_parenthesis_offset : ]):
        if in_quotes and character == in_quotes:
            in_quotes = None

        elif in_block_comment and character == block_comment_term[0] and string[index : index + 2] == block_comment_term:
            in_block_comment = False

        elif in_line_comment and character == line_comment_term:
            in_line_comment = False

        elif not in_quotes and not in_block_comment and not in_line_comment:
            if character in quote_chars:
                in_quotes = character

            elif character == block_comment_chars[0] and string[index : index + 2] == block_comment_chars:
                in_block_comment = True

            elif character == line_comment_chars[0] and string[index : index + 2] == line_comment_chars:
                in_line_comment = True

            elif character == ')':
                return index + opening_parenthesis_offset


def parse_schema(stdout):
    tables = {}

    while stdout:
        # Find the next table entry
        stdout = stdout[stdout.find("Type: table") : ]
        table_name = stdout[stdout.find("Table Name:") + 11 : stdout.find("SQL:")].strip()

        if table_name:
            stdout = stdout[stdout.find("SQL:") + 4 : ]
            
            closing_parenthesis_found = False
            in_quotes = False
            index = 0
            while not closing_parenthesis_found and stdout:
                if stdout[index] == "'":
                    in_quotes = not in_quotes
        
                elif stdout[index] == '(' and not in_quotes:
                    next_parenthesis = index
                    closing_parenthesis = get_index_of_closing_parenthesis(stdout, next_parenthesis)
                    closing_parenthesis_found = True

                index += 1

            # Fetches lines with columns in them
            schema_statement = stdout[next_parenthesis + 1 : closing_parenthesis].strip()
            tables[table_name] = parse_rows(schema_statement)

        stdout = stdout[closing_parenthesis + 1 : ]

    return tables

def get_md5_hash(string):
    return hashlib.md5(string).hexdigest().upper()


def replace_bytes(byte_array, replacement, index):
    return byte_array[:index] + replacement + byte_array[index + len(replacement):]


def decode_varint(byte_array, offset=0):
    unsigned_integer_value = 0
    varint_relative_offset = 0

    for x in xrange(1, 10):

        varint_byte = ord(byte_array[offset + varint_relative_offset:offset + varint_relative_offset + 1])
        varint_relative_offset += 1

        if x == 9:
            unsigned_integer_value <<= 1
            unsigned_integer_value |= varint_byte
        else:
            msb_set = varint_byte & 0x80
            varint_byte &= 0x7f
            unsigned_integer_value |= varint_byte
            if msb_set == 0:
                break
            else:
                unsigned_integer_value <<= 7

    signed_integer_value = unsigned_integer_value
    if signed_integer_value & 0x80000000 << 32:
        signed_integer_value -= 0x10000000000000000

    return signed_integer_value, varint_relative_offset

default_columns = OrderedDict(
    [
        ('name', 'TEXT NOT NULL'),
        ('data1', 'INT NOT NULL'),
        ('data2', 'INT NOT NULL'),
        ('data3', 'INT NOT NULL'),
        ('data4', 'TEXT NOT NULL')
    ]
)

db_params = [
    {
        'name': 'SFT-01-UTF8-WAL',
        'journal_mode': 'WAL',
        'encoding': 'UTF-8',
        'page_size': 4096,
        'create': 100,
        'delete': 0,
        'modify': 0,
        'columns': default_columns,
        'table_name': 'testing'
    },
    {
        'name': 'SFT-01-UTF16BE-PERSIST',
        'journal_mode': 'PERSIST',
        'encoding': 'UTF-16be',
        'page_size': 1024,
        'create': 100,
        'delete': 0,
        'modify': 0,
        'columns': default_columns,
        'table_name': 'testing'
    },
    {
        'name': 'SFT-01-UTF16LE-OFF',
        'journal_mode': 'OFF',
        'encoding': 'UTF-16le',
        'page_size': 8192,
        'create': 100,
        'delete': 0,
        'modify': 0,
        'columns': default_columns,
        'table_name': 'testing'
    },
    {
        'name': 'SFT-03-PERSIST',
        'journal_mode': 'PERSIST',
        'encoding': 'UTF-8',
        'page_size': 4096,
        'create': 2000,
        'delete': 100,
        'modify': 100,
        'columns': default_columns,
        'table_name': 'testing'
    },
    {
        'name': 'SFT-03-WAL',
        'journal_mode': 'WAL',
        'encoding': 'UTF-8',
        'page_size': 4096,
        'create': 2000,
        'delete': 100,
        'modify': 100,
        'columns': default_columns,
        'table_name': 'testing'
    }
]


def generate_int():
    return random.randint(0, 999999999)


def generate_string():
    return ''.join([random.choice(string.ascii_letters) for _ in range(random.randint(1, 15))])


def generate_float():
    return random.random() * 999999999.00


row_gen_dict = OrderedDict({
    'INT NOT NULL': generate_int,
    'REAL NOT NULL': generate_float,
    'TEXT NOT NULL': generate_string
})


# Uses columns dict in request.param objects
def generate_rows(num_rows, columns):
    row_spec = []
    for field_type in columns.values():
        row_spec.append(row_gen_dict[field_type])

    row_list = []
    for _ in range(num_rows):
        row = [uuid.uuid4().hex]
        for generator in row_spec:
            row.append(generator())

        row_list.append(row)

    return row_list


# Assumes columns doesn't include the id field (rowid alias). Uses columns dict in request.param objects
def generate_create_statement(table_name, columns):
    create_statement = "CREATE TABLE " + table_name + " (id TEXT PRIMARY KEY"
    for column_name, attributes in columns.items():
        create_statement += ', ' + column_name + ' ' + attributes

    create_statement += ')'
    return create_statement


# Generates a statement for use with sqlite3 library (uses qmark substitution).
def generate_insert_statement(table_name, num_values):
    return "INSERT INTO " + table_name + " VALUES (" + num_values * "?, " + "?)"


def generate_update_statement(table_name, columns):
    string_fields = ["%s=?" % field for field in columns.keys()]
    return "UPDATE " + table_name + " SET " + ', '.join(string_fields) + " WHERE id=?"


@pytest.fixture(params=db_params)
def db_file(request, tmp_path):
    basename = request.param['name'] + '.sqlite'
    db_filepath = tmp_path / basename
    modified_rows = []
    deleted_rows = []

    with sqlite3.connect(str(db_filepath.resolve())) as db:
        cursor = db.cursor()

        cursor.execute("PRAGMA journal_mode = %s" % (request.param['journal_mode']))
        cursor.execute("PRAGMA encoding = '%s'" % (request.param['encoding']))
        cursor.execute("PRAGMA page_size = %s" % (request.param['page_size']))

        cursor.execute(generate_create_statement(request.param['table_name'], request.param['columns']))

        row_list = []
        if request.param['create'] > 0:
            row_list = generate_rows(request.param['create'], request.param['columns'])

            insert_statement = generate_insert_statement(request.param['table_name'], len(request.param['columns']))
            cursor.executemany(insert_statement, row_list)
            db.commit()
            cursor.execute("PRAGMA wal_checkpoint")

        id_list = [row[0] for row in row_list]
        id_for_mod = random.sample(id_list, request.param['modify'])
        id_for_del = random.sample([row_id for row_id in id_list if row_id not in id_for_mod], request.param['delete'])

        if request.param['modify'] > 0:
            row_values = [row[1:] for row in generate_rows(request.param['modify'], request.param['columns'])]
            map(lambda row_values, id_for_mod: row_values.append(id_for_mod), row_values, id_for_mod)
            for row_id in id_for_mod:
                cursor.execute("SELECT * FROM testing WHERE id=?", (row_id, ))
                modified_rows.append(cursor.fetchone())

            update_statement = generate_update_statement(request.param['table_name'], request.param['columns'])
            cursor.executemany(update_statement, row_values)
            db.commit()

        if request.param['delete'] > 0:
            for row_id in id_for_del:
                cursor.execute("SELECT * FROM testing WHERE id=?", (row_id, ))
                deleted_rows.append(cursor.fetchone())

            cursor.executemany("DELETE FROM testing WHERE id=?", [[row_id] for row_id in id_for_del])
            db.commit()

    if 'SFT-01' in request.param['name'] or request.param['journal_mode'] != 'WAL':
        db.close()
    yield db_filepath, modified_rows + deleted_rows

# Parses CSV file returned by sqlite_dissect operations and returns rows found that match the given operations.
def parse_csv(filepath, operations, first_key = 'id'):
    accepted_sources = ["ROLLBACK_JOURNAL", "DATABASE", "WAL"]

    with open(filepath, 'r') as csv_file:
        key_line = csv_file.readline().strip()
        commas = find_breakpoints(key_line)
        keys = [strip_one(key_line[i:j], "['\"]") for i,j in zip([0] + [index + 1 for index in commas], commas + [None])]
        op_index = keys.index("Operation")
        first_index = keys.index(first_key)
        rows = []

        for line in csv_file:
            line_list = map(lambda data: data.strip('"'), line.strip().split(','))
            
            if line_list[0] in accepted_sources and line_list[op_index] in operations:
                rows.append(tuple(line_list[first_index:]))

    return tuple(rows)

        
