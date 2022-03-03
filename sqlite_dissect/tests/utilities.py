import os
import pytest
import sqlite3
import random
import string
from collections import OrderedDict
import uuid

default_columns = OrderedDict(
    [
        ('name', 'TEXT NOT NULL'),
        ('data1', 'INT NOT NULL'),
        ('data2', 'TEXT NOT NULL'),
        ('data3', 'REAL NOT NULL'),
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


