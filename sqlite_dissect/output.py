from binascii import hexlify
from logging import getLogger
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.constants import UTF_8
from sqlite_dissect.file.database.page import BTreePage
from sqlite_dissect.file.database.page import IndexInteriorPage
from sqlite_dissect.file.database.page import IndexLeafPage
from sqlite_dissect.file.database.page import TableInteriorPage
from sqlite_dissect.file.database.page import TableLeafPage
from sqlite_dissect.exception import OutputError
from sqlite_dissect.utilities import has_content

"""

output.py

This script holds general output functions used for debugging, logging, and general output for the
sqlite carving library.

This script holds the following function(s):

get_page_breakdown(pages)
get_pointer_map_entries_breakdown(version)
stringify_b_tree(version_interface, b_tree_root_page, padding="")
stringify_cell_record(cell, database_text_encoding, page_type)
stringify_cell_records(cells, database_text_encoding, page_type)
stringify_master_schema_version(version)
stringify_master_schema_versions(version_history)
stringify_page_history(version_history, padding="")
stringify_page_information(version, padding="")
stringify_page_structure(version, padding="")
stringify_unallocated_space(version, padding="", include_whitespace=True, whitespace_threshold=0)
stringify_version_pages(version, padding="")
"""


def get_page_breakdown(pages):
    page_breakdown = {}
    for page_type in PAGE_TYPE:
        page_breakdown[page_type] = []
    for page_number, page in pages.iteritems():
        page_breakdown[page.page_type].append(page_number)
    return page_breakdown


def get_pointer_map_entries_breakdown(version):

    pointer_map_entries_breakdown = []

    if not version.pointer_map_pages:
        return pointer_map_entries_breakdown

    for pointer_map_page in version.pointer_map_pages:

        if not pointer_map_page.pointer_map_entries:
            log_message = "No pointer map entries found for pointer map page: {} with page version: {} in version: {}."
            log_message = log_message.format(pointer_map_page.number, pointer_map_page.page_version_number,
                                             version.number)
            getLogger(LOGGER_NAME).error(log_message)
            raise OutputError(log_message)

        last_type_seen = pointer_map_page.pointer_map_entries[0].page_type
        last_page_number = pointer_map_page.number + 1
        last_entry = None
        for entry in pointer_map_page.pointer_map_entries:
            if hexlify(last_type_seen) != hexlify(entry.page_type):
                pages = entry.page_number - last_page_number
                breakdown = (pointer_map_page.number, last_page_number, entry.page_number - 1,
                             pages, hexlify(last_entry.page_type))
                pointer_map_entries_breakdown.append(breakdown)
                last_page_number = entry.page_number
            last_type_seen = entry.page_type
            last_entry = entry
        pages = last_entry.page_number - last_page_number + 1
        breakdown = (pointer_map_page.number, last_page_number, last_entry.page_number,
                     pages, hexlify(last_entry.page_type))
        pointer_map_entries_breakdown.append(breakdown)

    return pointer_map_entries_breakdown


def stringify_b_tree(version_interface, b_tree_root_page, padding=""):

    string = ""

    if isinstance(b_tree_root_page, TableLeafPage):
        string += "\n" + padding + "B-Tree Table Leaf Page -> {}: page version {} at offset {} with {} cells"
        string = string.format(b_tree_root_page.number, version_interface.get_page_version(b_tree_root_page.number),
                               b_tree_root_page.offset, len(b_tree_root_page.cells))
    elif isinstance(b_tree_root_page, IndexLeafPage):
        string += "\n" + padding + "B-Tree Index Leaf Page -> {}: page version {} at offset {} with {} cells"
        string = string.format(b_tree_root_page.number, version_interface.get_page_version(b_tree_root_page.number),
                               b_tree_root_page.offset, len(b_tree_root_page.cells))
    elif isinstance(b_tree_root_page, TableInteriorPage):
        string += "\n" + padding + "B-Tree Table Interior Page -> {}: page version {} at offset {} with {} cells"
        string = string.format(b_tree_root_page.number, version_interface.get_page_version(b_tree_root_page.number),
                               b_tree_root_page.offset, len(b_tree_root_page.cells))
        string += stringify_b_tree(version_interface, b_tree_root_page.right_most_page, padding + "\t")
        for b_tree_interior_cell in b_tree_root_page.cells:
            string += stringify_b_tree(version_interface, b_tree_interior_cell.left_child_page, padding + "\t")
    elif isinstance(b_tree_root_page, IndexInteriorPage):
        string += "\n" + padding + "B-Tree Index Interior Page -> {}: page version {} at offset {} with {} cells"
        string = string.format(b_tree_root_page.number, version_interface.get_page_version(b_tree_root_page.number),
                               b_tree_root_page.offset, len(b_tree_root_page.cells))
        string += stringify_b_tree(version_interface, b_tree_root_page.right_most_page, padding + "\t")
        for b_tree_interior_cell in b_tree_root_page.cells:
            string += stringify_b_tree(version_interface, b_tree_interior_cell.left_child_page, padding + "\t")
    else:
        log_message = "The b-tree root page is not a b-tree root page type but instead: {} in version: {}."
        log_message = log_message.format(b_tree_root_page.page_type, version_interface.number)
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)

    if not isinstance(b_tree_root_page, TableInteriorPage):
        for cell in b_tree_root_page.cells:
            if cell.has_overflow:
                overflow_padding = padding
                overflow_page = cell.overflow_pages[cell.overflow_page_number]
                overflow_padding += "\t"
                string += "\n" + overflow_padding + "Overflow Page -> {}: page version {} at offset {}"
                string = string.format(overflow_page.number, version_interface.get_page_version(overflow_page.number),
                                       overflow_page.offset)
                while overflow_page.next_overflow_page_number:
                    overflow_page = cell.overflow_pages[overflow_page.next_overflow_page_number]
                    overflow_padding += "\t"
                    string += "\n" + overflow_padding + "Overflow Page -> {}: page version {} at offset {}"
                    string = string.format(overflow_page.number,
                                           version_interface.get_page_version(overflow_page.number),
                                           overflow_page.offset)

    return string


def stringify_cell_record(cell, database_text_encoding, page_type):
    if page_type == PAGE_TYPE.B_TREE_TABLE_LEAF:

        column_values = []
        for record_column in cell.payload.record_columns:
            text_affinity = True if record_column.serial_type >= 13 and record_column.serial_type % 2 == 1 else False
            value = record_column.value
            if record_column.value:
                if text_affinity:
                    column_values.append(value.decode(database_text_encoding, "replace").encode(UTF_8))
                else:
                    column_values.append(str(value))
            else:
                column_values.append("NULL")
        content = "(" + ", ".join(column_values) + ")"
        return "#{}: {}".format(cell.row_id, content)

    elif page_type == PAGE_TYPE.B_TREE_INDEX_LEAF:

        column_values = []
        for record_column in cell.payload.record_columns:
            text_affinity = True if record_column.serial_type >= 13 and record_column.serial_type % 2 == 1 else False
            value = record_column.value
            if record_column.value:
                if text_affinity:
                    column_values.append(value.decode(database_text_encoding, "replace").encode(UTF_8))
                else:
                    column_values.append(str(value))
            else:
                column_values.append("NULL")
        content = "(" + ", ".join(column_values) + ")"
        return content

    else:
        log_message = "Invalid page type specified for stringify cell record: {}.  Page type should " \
                      "be either {} or {}."
        log_message = log_message.format(page_type, PAGE_TYPE.B_TREE_TABLE_LEAF, PAGE_TYPE.B_TREE_INDEX_LEAF)
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)


def stringify_cell_records(cells, database_text_encoding, page_type):
    cell_records = set()
    for cell in cells:
        cell_records.add(stringify_cell_record(cell, database_text_encoding, page_type))
    return cell_records


def stringify_master_schema_version(version):

    string = ""

    for master_schema_entry in version.master_schema.master_schema_entries:

        entry_string = "Version: {} Added Master Schema Entry: Root Page Number: {} Type: {} Name: {} " \
                       "Table Name: {} SQL: {}.\n"
        entry_string = entry_string.format(version.version_number, master_schema_entry.root_page_number,
                                           master_schema_entry.row_type, master_schema_entry.name,
                                           master_schema_entry.table_name, master_schema_entry.sql)
        string += entry_string

    return string


def stringify_master_schema_versions(version_history):

    string = ""

    master_schema_entries = {}

    for version_number, version in version_history.versions.iteritems():

        if version.master_schema_modified:

            modified_master_schema_entries = dict(map(lambda x: [x.md5_hash_identifier, x],
                                                      version.master_schema.master_schema_entries))

            for md5_hash_identifier, master_schema_entry in modified_master_schema_entries.iteritems():

                if md5_hash_identifier not in master_schema_entries:

                    added_string = "Version: {} Added Master Schema Entry: Root Page Number: {} Type: {} Name: {} " \
                                   "Table Name: {} SQL: {}.\n"
                    added_string = added_string.format(version_number, master_schema_entry.root_page_number,
                                                       master_schema_entry.row_type, master_schema_entry.name,
                                                       master_schema_entry.table_name, master_schema_entry.sql)
                    string += added_string

                    master_schema_entries[md5_hash_identifier] = master_schema_entry

                elif master_schema_entry.root_page_number != master_schema_entries[
                                                                                md5_hash_identifier].root_page_number:

                    previous_root_page_number = master_schema_entries[md5_hash_identifier].root_page_number

                    updated_string = "Version: {} Updated Master Schema Entry: Root Page Number From: {} To: {} " \
                                     "Type: {} Name: {} Table Name: {} SQL: {}.\n"
                    updated_string = updated_string.format(version_number, previous_root_page_number,
                                                           master_schema_entry.root_page_number,
                                                           master_schema_entry.row_type, master_schema_entry.name,
                                                           master_schema_entry.table_name, master_schema_entry.sql)
                    string += updated_string

                    master_schema_entries[md5_hash_identifier] = master_schema_entry

            for md5_hash_identifier, master_schema_entry in master_schema_entries.iteritems():

                if md5_hash_identifier not in modified_master_schema_entries:

                    removed_string = "Version: {} Removed Master Schema Entry: Root Page Number: {} Type: {} " \
                                     "Name: {} Table Name: {} SQL: {}.\n"
                    removed_string = removed_string.format(version_number, master_schema_entry.root_page_number,
                                                           master_schema_entry.row_type, master_schema_entry.name,
                                                           master_schema_entry.table_name, master_schema_entry.sql)
                    string += removed_string

    return string


def stringify_page_history(version_history, padding=""):
    string = ""
    for version_number in version_history.versions:
        string += "\n" if string else ""
        string += stringify_version_pages(version_history.versions[version_number], padding)
    return string


def stringify_page_information(version, padding=""):
    string = padding + "Page Breakdown:"
    for page_type, page_array in get_page_breakdown(version.pages).iteritems():
        page_array_length = len(page_array)
        string += "\n" + padding + "\t" + "{}: {} Page Numbers: {}"
        string = string.format(page_type, page_array_length, page_array)
    string += "\n" + padding + "Page Structure:\n{}".format(stringify_page_structure(version, padding + "\t"))
    if version.pointer_map_pages:
        string += "\n" + padding + "Pointer Map Entry Breakdown across {} Pages:".format(version.database_size_in_pages)
        for pointer_map_entry_breakdown in get_pointer_map_entries_breakdown(version):
            string += "\n" + padding + "\t" + "Pointer Map Page {}: Page {} -> {} ({}) had Pointer Page Type (Hex) {}"
            string = string.format(pointer_map_entry_breakdown[0], pointer_map_entry_breakdown[1],
                                   pointer_map_entry_breakdown[2], pointer_map_entry_breakdown[3],
                                   pointer_map_entry_breakdown[4])
    return string


def stringify_page_structure(version, padding=""):

    string = padding + "{} Pages of {} bytes".format(version.database_size_in_pages, version.page_size)

    string += "\n" + padding + "Database Root Page:"
    string += stringify_b_tree(version, version.root_page, padding + "\t")

    pointer_map_pages = version.pointer_map_pages
    if pointer_map_pages:
        for pointer_map_page in pointer_map_pages:
            string += "\n" + padding + "Pointer Map Page -> {}".format(pointer_map_page.number)

    freelist_trunk_page = version.first_freelist_trunk_page
    if freelist_trunk_page:
        string += "\n" + padding + "Freelist Trunk Page -> {}".format(freelist_trunk_page.number)
        freelist_padding = padding + "\t"
        for freelist_leaf_page in freelist_trunk_page.freelist_leaf_pages:
            string += "\n" + freelist_padding + "Freelist Leaf Page -> {}".format(freelist_leaf_page.number)
        while freelist_trunk_page.next_freelist_trunk_page:
            freelist_trunk_page = freelist_trunk_page.next_freelist_trunk_page
            string += "\n" + freelist_padding + "Freelist Trunk Page -> {}".format(freelist_trunk_page.number)
            freelist_padding += "\t"
            for freelist_leaf_page in freelist_trunk_page.freelist_leaf_pages:
                string += "\n" + freelist_padding + "Freelist Leaf Page -> {}".format(freelist_leaf_page.number)

    if version.master_schema:
        string += "\n" + padding + "Master Schema Root Pages:"
        for master_schema_root_page_number in version.master_schema.master_schema_b_tree_root_page_numbers:
            master_schema_root_page = version.get_b_tree_root_page(master_schema_root_page_number)
            string += stringify_b_tree(version, master_schema_root_page, padding + "\t")

    return string


def stringify_unallocated_space(version, padding="", include_empty_space=True):
    string = ""
    calculated_total_fragmented_bytes = 0
    for page_number, page in version.pages.iteritems():

        unallocated_content = page.unallocated_content
        if len(unallocated_content):
            if (not include_empty_space and has_content(unallocated_content)) or include_empty_space:
                string += "\n" if string else ""
                string += padding + "Page #{}: {} Page Unallocated Space Start Offset: {} " \
                                    "End Offset: {} Size: {} Hex: [{}]"
                string = string.format(page_number, page.page_type, page.unallocated_space_start_offset,
                                       page.unallocated_space_end_offset, page.unallocated_space_length,
                                       hexlify(page.unallocated_content))

        if isinstance(page, BTreePage):
            for freeblock in page.freeblocks:
                freeblock_content = freeblock.content
                if len(freeblock_content) and has_content(freeblock_content):
                    string += "\n" if string else ""
                    string += padding + "Page #{}: {} Page Freeblock #{}: Unallocated Space Start Offset: {} " \
                                        "End Offset: {} Size: {} Hex: [{}]"
                    string = string.format(page_number, page.page_type, freeblock.index, freeblock.start_offset,
                                           freeblock.end_offset, freeblock.content_length,
                                           hexlify(freeblock_content))

            for fragment in page.fragments:
                fragment_content = fragment.content
                if fragment_content and has_content(fragment_content):
                    string += "\n" if string else ""
                    string += padding + "Page #{}: {} Page Fragment #{}: Unallocated Space Start Offset: {} " \
                                        "End Offset: {} Size: {} Hex: [{}]"
                    string = string.format(page_number, page.page_type, fragment.index, fragment.start_offset,
                                           fragment.end_offset, fragment.byte_size, hexlify(fragment_content))
                calculated_total_fragmented_bytes += page.header.number_of_fragmented_free_bytes

    string += "\n" if string else ""
    string += padding + "Calculated Total Fragmented Bytes: {}".format(calculated_total_fragmented_bytes)
    return string


def stringify_version_pages(version, padding=""):
    string = padding + "Version {} with {} of {} Pages: {}".format(version.version_number,
                                                                   len(version.updated_page_numbers),
                                                                   version.database_size_in_pages,
                                                                   version.updated_page_numbers)

    page_versions = {}
    for page_number, page_version_number in version.page_version_index.iteritems():
        if page_version_number in page_versions:
            page_versions[page_version_number] = page_versions[page_version_number] + ", " + str(page_number)
        else:
            page_versions[page_version_number] = str(page_number)

    for version_number in reversed(range(version.version_number + 1)):
        page_version_string = "\n" + padding + "\t" + "Version: {} has Pages: {}"
        if version_number in page_versions:
            string += page_version_string.format(version_number, page_versions[version_number])
        else:
            string += page_version_string.format(version_number, str())
    return string
