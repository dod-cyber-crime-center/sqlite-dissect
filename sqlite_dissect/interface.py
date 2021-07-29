from os.path import basename
from os.path import normpath
from sqlite_dissect.carving.carver import SignatureCarver
from sqlite_dissect.carving.signature import Signature
from sqlite_dissect.constants import BASE_VERSION_NUMBER
from sqlite_dissect.constants import CELL_SOURCE
from sqlite_dissect.constants import MASTER_SCHEMA_ROW_TYPE
from sqlite_dissect.file.database.database import Database
from sqlite_dissect.file.database.page import BTreePage
from sqlite_dissect.file.database.utilities import aggregate_leaf_cells
from sqlite_dissect.file.database.utilities import get_pages_from_b_tree_page
from sqlite_dissect.file.schema.master import OrdinaryTableRow
from sqlite_dissect.file.schema.master import VirtualTableRow
from sqlite_dissect.export.csv_export import CommitCsvExporter
from sqlite_dissect.export.sqlite_export import CommitSqliteExporter
from sqlite_dissect.file.wal.wal import WriteAheadLog
from sqlite_dissect.version_history import VersionHistory
from sqlite_dissect.version_history import VersionHistoryParser

"""

interface.py

This script acts as a simplified interface for common operations for the sqlite carving library.

This script holds the following function(s):
create_database(file_identifier, store_in_memory=False, strict_format_checking=True)
create_write_ahead_log(file_name, file_object=None)
create_version_history(database, write_ahead_log=None)
get_table_names(database)
get_index_names(database)
select_all_from_table(table_name, version)
select_all_from_index(index_name, version)
create_table_signature(table_name, version, version_history=None)
carve_table(table_name, signature, version)
get_version_history_iterator(table_or_index_name, version_history, signature=None)
export_table_or_index_version_history_to_csv(export_directory, version_history,
                                             table_or_index_name, signature=None, carve_freelist_pages=False)
export_version_history_to_csv(export_directory, version_history, signatures=None, carve_freelist_pages=False)
export_table_or_index_version_history_to_sqlite(export_directory, sqlite_file_name, version_history,
                                                table_or_index_name, signature=None, carve_freelist_pages=False):
export_version_history_to_sqlite(export_directory, sqlite_file_name, version_history,
                                 signatures=None, carve_freelist_pages=False):

"""


def create_database(file_identifier, store_in_memory=False, strict_format_checking=True):
    return Database(file_identifier, store_in_memory, strict_format_checking)


def create_write_ahead_log(file_identifier):
    return WriteAheadLog(file_identifier)


def create_version_history(database, write_ahead_log=None):
    return VersionHistory(database, write_ahead_log)


def get_table_names(database):
    return [master_schema_entry.name
            for master_schema_entry in database.master_schema.master_schema_entries
            if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.TABLE]


def get_index_names(database):
    return [master_schema_entry.name
            for master_schema_entry in database.master_schema.master_schema_entries
            if master_schema_entry.row_type == MASTER_SCHEMA_ROW_TYPE.INDEX]


def get_master_schema_entry(master_schema_entry_name, version_history):
    master_schema_entries = version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries
    for master_schema_entry in master_schema_entries:
        if master_schema_entry.name == master_schema_entry_name:
            return master_schema_entry
    raise Exception("Master schema entry not found for master schema entry name: %s." % master_schema_entry_name)


def get_column_index(column_name, master_schema_entry_name, version_history):
    master_schema_entry = get_master_schema_entry(master_schema_entry_name, version_history)
    for column_definition in master_schema_entry.column_definitions:
        if column_definition.column_name == column_name:
            return column_definition.index
    raise Exception("Column definition not found for column name: %s and master schema entry name: %s." %
                    (column_name, master_schema_entry_name))


def select_all_from_table(table_name, version):
    master_schema_entries = {master_schema_entry.name: master_schema_entry
                             for master_schema_entry in version.master_schema.master_schema_entries}
    master_schema_entry = master_schema_entries[table_name]
    number_of_cells, cells = aggregate_leaf_cells(version.get_b_tree_root_page(master_schema_entry.root_page_number))
    if master_schema_entry.without_row_id:
        return cells.values()
    else:
        return sorted(cells.values(), key=lambda cell: cell.row_id)


def select_all_from_index(index_name, version):
    master_schema_entries = {master_schema_entry.name: master_schema_entry
                             for master_schema_entry in version.master_schema.master_schema_entries}
    master_schema_entry = master_schema_entries[index_name]
    number_of_cells, cells = aggregate_leaf_cells(version.get_b_tree_root_page(master_schema_entry.root_page_number))
    return cells.values()


def create_table_signature(table_name, version, version_history=None):
    if not version_history:
        version_history = VersionHistory(version)
    master_schema_entries = {master_schema_entry.name: master_schema_entry
                             for master_schema_entry in version.master_schema.master_schema_entries}
    master_schema_entry = master_schema_entries[table_name]
    # Signatures are not currently generated/supported for "without rowid" tables and virtual tables.
    if isinstance(master_schema_entry, OrdinaryTableRow) and master_schema_entry.without_row_id:
        return None
    elif isinstance(master_schema_entry, VirtualTableRow):
        return None
    else:
        return Signature(version_history, master_schema_entry)


def carve_table(table_name, signature, version):
    master_schema_entries = {master_schema_entry.name: master_schema_entry
                             for master_schema_entry in version.master_schema.master_schema_entries}
    master_schema_entry = master_schema_entries[table_name]
    # Do not carve if the table is a "without rowid" table since they are not currently supported
    if master_schema_entry.without_row_id:
        return []
    b_tree_pages = get_pages_from_b_tree_page(version.get_b_tree_root_page(master_schema_entry.root_page_number))
    b_tree_pages = {b_tree_page.number: b_tree_page for b_tree_page in b_tree_pages}
    carved_cells = []
    for page_number, page in b_tree_pages.iteritems():
        #  For carving freeblocks make sure the page is a b-tree page and not overflow
        if isinstance(page, BTreePage):
            carvings = SignatureCarver.carve_freeblocks(version, CELL_SOURCE.B_TREE, page.freeblocks, signature)
            carved_cells.extend(carvings)
        carvings = SignatureCarver.carve_unallocated_space(version, CELL_SOURCE.B_TREE, page_number,
                                                           page.unallocated_space_start_offset,
                                                           page.unallocated_space, signature)
        carved_cells.extend(carvings)
    return carved_cells


def get_version_history_iterator(table_or_index_name, version_history, signature=None, carve_freelist_pages=False):
    # Currently master schema entries are taken from the base version
    master_schema_entries = {master_schema_entry.name: master_schema_entry
                             for master_schema_entry in
                             version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries}
    return VersionHistoryParser(version_history, master_schema_entries[table_or_index_name], None, None,
                                signature, carve_freelist_pages)


def export_table_or_index_version_history_to_csv(csv_file_name, export_directory, version_history,
                                                 table_or_index_name, signature=None, carve_freelist_pages=False):
    # Currently the file name prefix is taken from the base version name
    csv_prefix_file_name = basename(normpath(csv_file_name))
    commit_csv_exporter = CommitCsvExporter(export_directory, csv_prefix_file_name)
    # Currently master schema entries are taken from the base version

    master_schema_entries = {master_schema_entry.name: master_schema_entry
                             for master_schema_entry in
                             version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries}
    master_schema_entry = master_schema_entries[table_or_index_name]
    version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                  signature, carve_freelist_pages)
    for commit in version_history_parser:
        commit_csv_exporter.write_commit(master_schema_entry, commit)


def export_version_history_to_csv(csv_file_name, export_directory, version_history,
                                  signatures=None, carve_freelist_pages=False):
    # Currently the file name prefix is taken from the base version name
    csv_prefix_file_name = basename(normpath(csv_file_name))
    commit_csv_exporter = CommitCsvExporter(export_directory, csv_prefix_file_name)
    signatures = {signature.name: signature for signature in signatures} if signatures else None
    # Currently master schema entries are taken from the base version
    for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:
        if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:
            signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures else None
            carve_freelist_pages = carve_freelist_pages if signature else False
            version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                          signature, carve_freelist_pages)
            for commit in version_history_parser:
                commit_csv_exporter.write_commit(master_schema_entry, commit)


def export_table_or_index_version_history_to_sqlite(export_directory, sqlite_file_name, version_history,
                                                    table_or_index_name, signature=None, carve_freelist_pages=False):
    with CommitSqliteExporter(export_directory, sqlite_file_name) as commit_sqlite_exporter:
        # Currently master schema entries are taken from the base version
        master_schema_entries = {master_schema_entry.name: master_schema_entry
                                 for master_schema_entry in
                                 version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries}
        master_schema_entry = master_schema_entries[table_or_index_name]
        version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                      signature, carve_freelist_pages)
        for commit in version_history_parser:
            commit_sqlite_exporter.write_commit(master_schema_entry, commit)


def export_version_history_to_sqlite(export_directory, sqlite_file_name, version_history,
                                     signatures=None, carve_freelist_pages=False):
    signatures = {signature.name: signature for signature in signatures} if signatures else None
    with CommitSqliteExporter(export_directory, sqlite_file_name) as commit_sqlite_exporter:
        # Currently master schema entries are taken from the base version
        for master_schema_entry in version_history.versions[BASE_VERSION_NUMBER].master_schema.master_schema_entries:
            if master_schema_entry.row_type in [MASTER_SCHEMA_ROW_TYPE.INDEX, MASTER_SCHEMA_ROW_TYPE.TABLE]:
                signature = signatures[master_schema_entry.name] if master_schema_entry.name in signatures else None
                carve_freelist_pages = carve_freelist_pages if signature else False
                version_history_parser = VersionHistoryParser(version_history, master_schema_entry, None, None,
                                                              signature, carve_freelist_pages)
                for commit in version_history_parser:
                    commit_sqlite_exporter.write_commit(master_schema_entry, commit)
