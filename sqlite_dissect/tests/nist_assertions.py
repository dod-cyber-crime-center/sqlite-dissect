from os.path import exists, basename
from hashlib import md5


def row_equals(row1, row2):
    for attr1, attr2 in zip(row1, row2):
        if str(attr1) != str(attr2):
            return False

    return True


def row_exists(target, row_list):
    for row in row_list:
        if row_equals(row, target):
            return True

    return False


def get_list_diff(reported_list, correct_list):
    missed_items = [item for item in correct_list if item not in reported_list]
    extra_items = [item for item in reported_list if item not in correct_list]

    return missed_items, extra_items


# NIST SFT-CA:

# SFT-CA-01
def assert_md5_equals(initial_hash, final_hash, filename):
    assert initial_hash == final_hash, "MD5 Hash value of file %s does not match. File has been altered!\n" \
                                       "Initial hash: %s\nFinal hash: %s" % (filename, initial_hash, final_hash)


# SFT-CA-02
def assert_file_exists(file_path):
    assert exists(file_path), "The file %s doesn't exist. File has been deleted!" % (basename(file_path))


# SFT-CA-03
def assert_correct_page_size(reported_size, correct_size):
    assert reported_size == correct_size, "The program reports an incorrect page size!\nCorrect page size: %d\n" \
                                          "Reported page size: %d" % (correct_size, reported_size)


# SFT-CA-04
# SFT-CA-05
def assert_correct_journal_mode(reported_mode, correct_mode, version):
    version_dict = {'r': 'read version',
                    'w': 'write version'}

    journal_dict = {'PERSIST': 'JOURNAL',
                    'DELETE': 'JOURNAL',
                    'MEMORY': 'JOURNAL',
                    'OFF': 'JOURNAL',
                    'TRUNCATE': 'JOURNAL',
                    'WAL': 'WAL'}

    assert reported_mode.upper() == journal_dict[
        correct_mode.upper()], "The program reports an incorrect journal mode (%s)!\nCorrect mode: %s\n" \
                               "Reported mode: %s" % (version_dict[version], correct_mode, reported_mode)


# SFT-CA-06
def assert_correct_num_pages(reported_num, correct_num):
    assert reported_num == correct_num, "The program reports an incorrect number of pages!\n" \
                                        "Correct number of pages: %s\nReported number of pages: %s" \
                                        % (correct_num, reported_num)


# SFT-CA-07
def assert_correct_encoding(reported_enc, correct_enc):
    assert reported_enc.upper() == correct_enc.upper(), "The program reports and incorrect database text encoding!\n" \
                                                        "Correct encoding: %s\nReported encoding: %s" % (
                                                        correct_enc, reported_enc)


# SFT-CA-08
def assert_correct_tables(reported_names, correct_names):
    missed_tables, extra_tables = get_list_diff(reported_names, correct_names)

    assert not missed_tables, "The program failed to report all tables!\nTables missing: %s" % (
        ", ".join(missed_tables))
    assert not extra_tables, "The program reported extra tables!\nTables added: %s" % (", ".join(extra_tables))


# SFT-CA-09
def assert_correct_columns(reported_names, correct_names, table):
    missed_columns, extra_columns = get_list_diff(reported_names, correct_names)

    assert not missed_columns, "The program failed to report all columns in table %s!\n" \
                               "Columns missing: %s" % (table, ", ".join(missed_columns))
    assert not extra_columns, "The program reported extra columns in table %s!\n" \
                              "Columns added: %s" % (table, ", ".join(extra_columns))


# SFT-CA-10
def assert_correct_num_rows(reported_num, correct_num, table):
    assert reported_num == correct_num, "The program reports an incorrect number of rows for table %s!\n" \
                                        "Correct number of rows: %d\nReported number of rows: %d" \
                                        % (table, correct_num, reported_num)


# SFT-CA-11
# SFT-CA-12
def assert_correct_rows(reported_rows, correct_rows):
    missed_rows = []
    for row in correct_rows:
        if not row_exists(row, reported_rows):
            missed_row = '('
            first_attribute = True
            for attribute in row:
                if not first_attribute:
                    missed_row += ', '
                else:
                    first_attribute = False
                missed_row += str(attribute)
            missed_row += ')'

            missed_rows.append(missed_row)

    assert not missed_rows, "The program failed to report all recoverable rows (missed %i out of %i)!\n" \
                            "Rows missing: \n%s" % (len(missed_rows), len(correct_rows), "\n".join(missed_rows))


# SFT-CA-13
def assert_correct_source(reported_source, accepted_sources, element):
    assert reported_source in accepted_sources, "The program reports an invalid file source!\n Element: %s\n" \
                                              "Reported source: %s" % (element, reported_source)


# NIST SFT-AO:


# SFT-AO-01
def assert_correct_statement(reported_statement, correct_statement, table):
    assert reported_statement == correct_statement, "The program reports an incorrect CREATE TABLE statement for " \
                                                    "table %s!\nCorrect statement:\n%s\nReported statement:\n%s" \
                                                    % (table, correct_statement, reported_statement)


# SFT-AO-02
def assert_correct_data_type(reported_type, correct_type, column, table):
    assert reported_type == correct_type, "The program reports an incorrect data type for column %s in table %s!\n" \
                                          "Correct type: %s\nReported type: %s" \
                                          % (column, table, correct_type, reported_type)


# SFT-AO-03
def assert_correct_key(reported_key, correct_key, table):
    assert reported_key == correct_key, "The program reports an incorrect primary key for table %s!\nCorrect key: %s\n" \
                                        "Reported key: %s" % (table, correct_key, reported_key)


# SFT-AO-04
# SFT-AO-05
def assert_correct_recovery_cause(reported_cause, correct_cause, row_key, table):
    cause_dict = {
        'd': "A deletion",
        'u': "An update"
    }

    assert reported_cause == correct_cause, "The program reports an incorrect cause of recovery for the row with " \
                                            "primary key %s in table %s!\nCorrect cause: %s within the database.\n" \
                                            "Reported cause: %s within the database." \
                                            % (row_key, table, correct_cause, reported_cause)


# SFT-AO-06
def assert_correct_file_offset(reported_offset, correct_offset, element):
    assert reported_offset == correct_offset, "The program reports an incorrect file offset for data element %s!\n" \
                                              "Correct offset: %d\nReported offset: %d" \
                                              % (element, correct_offset, reported_offset)


# SFT-AO-07
def assert_correct_table_name(reported_name, correct_name, elmnt):
    assert reported_name == correct_name, "The program reports an incorrect table for data element %s!\n" \
                                          "Correct table: %s\nReported table: %s" % (elmnt, correct_name, reported_name)


# SFT-AO-08
def assert_correct_transactions(reported_transactions, correct_transactions, wal):
    missing_transactions, extra_transactions = get_list_diff(reported_transactions, correct_transactions)

    assert missing_transactions, "The program fails to report all transactions in WAL file %s!\n" \
                                 "Missing transactions:\n%s" % (wal, "\n".join(missing_transactions))
    assert extra_transactions, "The program reports more transactions than expected in WAL file %s!\n" \
                               "Extra transactions:\n%s" % (wal, "\n".join(extra_transactions))
