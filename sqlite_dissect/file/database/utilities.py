from logging import getLogger
from math import floor
from sqlite_dissect.constants import LOGGER_NAME
from sqlite_dissect.constants import PAGE_TYPE
from sqlite_dissect.constants import POINTER_MAP_ENTRY_LENGTH
from sqlite_dissect.exception import ParsingError
from sqlite_dissect.file.database.page import IndexInteriorPage
from sqlite_dissect.file.database.page import IndexLeafPage
from sqlite_dissect.file.database.page import PointerMapPage
from sqlite_dissect.file.database.page import TableInteriorPage
from sqlite_dissect.file.database.page import TableLeafPage

"""

utilities.py

This script holds utility functions for dealing with database specific objects such as pages rather than more general
utility methods.

This script holds the following function(s):
aggregate_leaf_cells(b_tree_page, accounted_for_cell_md5s=None, records_only=False)
create_pointer_map_pages(version, database_size_in_pages, page_size)
get_maximum_pointer_map_entries_per_page(page_size)
get_page_numbers_and_types_from_b_tree_page(b_tree_page)
get_pages_from_b_tree_page(b_tree_page)

"""


def aggregate_leaf_cells(b_tree_page, accounted_for_cell_md5s=None, payloads_only=False):

    """

    This function will parse through all records across all leaf pages in a b-tree recursively and return a total
    number of cells found along with a dictionary of cells where the dictionary is in the form of:
    cells[CELL_MD5_HEX_DIGEST] = cell.  Therefore, without the accounted for cell md5s specified,
    the number of cells will match the length of the records dictionary.

    If the accounted for cell md5s field is set with entries, then those entries will be ignored from the dictionary
    but the number of cells will include the number of accounted for cell md5s in it.  Therefore, with the accounted
    for cell md5s specified, the number of cells will match the length of the records dictionary + the number of
    accounted for cell md5s found.

    If the payloads only flag is specified, the dictionary will only contain payloads (ie. records) and not the cells:
    cells[CELL_MD5_HEX_DIGEST] = cell.payload.

    Note:  As this function name implies, this only parses through the leaf pages of table and index b-tree pages.
           Cells of interior pages will be not be handled by this function.

    :param b_tree_page:
    :param accounted_for_cell_md5s:
    :param payloads_only:

    :return: tuple(number_of_records, records)

    :raise:

    """

    accounted_for_cell_md5s = set() if accounted_for_cell_md5s is None else accounted_for_cell_md5s

    number_of_cells = 0
    cells = {}

    if isinstance(b_tree_page, TableLeafPage) or isinstance(b_tree_page, IndexLeafPage):

        number_of_cells += len(b_tree_page.cells)

        if payloads_only:
            for cell in b_tree_page.cells:
                if cell.md5_hex_digest not in accounted_for_cell_md5s:
                    accounted_for_cell_md5s.add(cell.md5_hex_digest)
                    cells[cell.md5_hex_digest] = cell.payload
        else:
            for cell in b_tree_page.cells:
                if cell.md5_hex_digest not in accounted_for_cell_md5s:
                    accounted_for_cell_md5s.add(cell.md5_hex_digest)
                    cells[cell.md5_hex_digest] = cell

    elif isinstance(b_tree_page, TableInteriorPage) or isinstance(b_tree_page, IndexInteriorPage):

        right_most_page_number_of_records, right_most_page_records = aggregate_leaf_cells(b_tree_page.right_most_page,
                                                                                          accounted_for_cell_md5s,
                                                                                          payloads_only)
        number_of_cells += right_most_page_number_of_records
        cells.update(right_most_page_records)

        for cell in b_tree_page.cells:

            left_child_page_number_of_records, left_child_page_records = aggregate_leaf_cells(cell.left_child_page,
                                                                                              accounted_for_cell_md5s,
                                                                                              payloads_only)
            number_of_cells += left_child_page_number_of_records
            cells.update(left_child_page_records)

    else:

        log_message = "Invalid page type found: {} to aggregate cells on.".format(type(b_tree_page))
        getLogger(LOGGER_NAME).error(log_message)
        raise ValueError(log_message)

    return number_of_cells, cells


def create_pointer_map_pages(version, database_size_in_pages, page_size):

    """



    Note:  When calling this function, the caller should have already determined if pointer map pages exist in the file
           they are parsing or not.  This can be done by checking the largest root b-tree page number exists in the
           database header.  If it does not exist, then pointer map pages are not enabled.  This function does not
           have any way nor need to check that field and solely computes what the pointer map pages would be off of
           the database size in pages and page size.

    :param version:
    :param database_size_in_pages:
    :param page_size:

    :return:

    """

    logger = getLogger(LOGGER_NAME)

    maximum_entries_per_page = get_maximum_pointer_map_entries_per_page(page_size)

    number_of_pointer_map_pages = 1
    if database_size_in_pages - 2 > maximum_entries_per_page:
        database_pages_left = database_size_in_pages - 2 - maximum_entries_per_page
        while database_pages_left > 0:
            database_pages_left -= maximum_entries_per_page - 1
            number_of_pointer_map_pages += 1

    pointer_map_pages = []
    pointer_map_page_number = 2
    number_of_pointer_map_pages = 0
    while pointer_map_page_number < database_size_in_pages:

        number_of_pointer_map_pages += 1
        entries = number_of_pointer_map_pages * maximum_entries_per_page
        next_pointer_map_page_number = entries + 2 + number_of_pointer_map_pages

        number_of_entries = maximum_entries_per_page
        if next_pointer_map_page_number > database_size_in_pages:
            previous_entries = ((number_of_pointer_map_pages - 1) * maximum_entries_per_page)
            number_of_entries = database_size_in_pages - previous_entries - number_of_pointer_map_pages - 1

        pointer_map_pages.append(PointerMapPage(version, pointer_map_page_number, number_of_entries))
        pointer_map_page_number = next_pointer_map_page_number

        if pointer_map_page_number == database_size_in_pages:
            log_message = "The next pointer map page number: {} is equal to the database size in pages: {} " \
                          "for version: {} resulting in erroneous pointer map pages."
            log_message = log_message.format(pointer_map_page_number, database_size_in_pages, version.version_number)
            logger.error(log_message)
            raise ParsingError(log_message)

    """

    Iterate through the pointer map pages that were created and tally up all the pointer map pages along with their
    pointer map entries.  This total should match the total number of pages in the database.

    Note:  The first pointer map page in the database is page 2 and therefore the root page always appears before the
           first pointer map page at page 2.  Below the calculated database pages starts at one to account for the root
           database page.

    """

    calculated_database_pages = 1
    for pointer_map_page in pointer_map_pages:
        calculated_database_pages += 1
        calculated_database_pages += pointer_map_page.number_of_entries

    if calculated_database_pages != database_size_in_pages:
        log_message = "The calculated number of database pages from the pointer map pages: {} does not equal the " \
                      "database size in pages: {} for version: {}."
        log_message = log_message.format(calculated_database_pages, database_size_in_pages, version.version_number)
        logger.error(log_message)
        raise ParsingError(log_message)

    return pointer_map_pages


def get_maximum_pointer_map_entries_per_page(page_size):
    return int(floor(float(page_size)/POINTER_MAP_ENTRY_LENGTH))


def get_page_numbers_and_types_from_b_tree_page(b_tree_page):

    logger = getLogger(LOGGER_NAME)

    b_tree_page_numbers = {}

    if isinstance(b_tree_page, TableLeafPage):
        b_tree_page_numbers[b_tree_page.number] = PAGE_TYPE.B_TREE_TABLE_LEAF
    elif isinstance(b_tree_page, IndexLeafPage):
        b_tree_page_numbers[b_tree_page.number] = PAGE_TYPE.B_TREE_INDEX_LEAF
    elif isinstance(b_tree_page, TableInteriorPage):
        b_tree_page_numbers[b_tree_page.number] = PAGE_TYPE.B_TREE_TABLE_INTERIOR
        b_tree_page_numbers.update(get_page_numbers_and_types_from_b_tree_page(b_tree_page.right_most_page))
        for b_tree_cell in b_tree_page.cells:
            b_tree_page_numbers.update(get_page_numbers_and_types_from_b_tree_page(b_tree_cell.left_child_page))
    elif isinstance(b_tree_page, IndexInteriorPage):
        b_tree_page_numbers[b_tree_page.number] = PAGE_TYPE.B_TREE_INDEX_INTERIOR
        b_tree_page_numbers.update(get_page_numbers_and_types_from_b_tree_page(b_tree_page.right_most_page))
        for b_tree_cell in b_tree_page.cells:
            b_tree_page_numbers.update(get_page_numbers_and_types_from_b_tree_page(b_tree_cell.left_child_page))
    else:
        log_message = "The b-tree page is not a BTreePage object but has a type of: {}."
        log_message = log_message.format(type(b_tree_page))
        logger.error(log_message)
        raise ValueError(log_message)

    if not isinstance(b_tree_page, TableInteriorPage):
        for cell in b_tree_page.cells:
            if cell.has_overflow:
                overflow_page = cell.overflow_pages[cell.overflow_page_number]
                b_tree_page_numbers[overflow_page.number] = PAGE_TYPE.OVERFLOW
                while overflow_page.next_overflow_page_number:
                    overflow_page = cell.overflow_pages[overflow_page.next_overflow_page_number]
                    b_tree_page_numbers[overflow_page.number] = PAGE_TYPE.OVERFLOW

    return b_tree_page_numbers


def get_pages_from_b_tree_page(b_tree_page):

    """



    Note:  The b-tree page sent in is included in the return result.

    :param b_tree_page:

    :return:

    """

    logger = getLogger(LOGGER_NAME)

    b_tree_pages = []

    if isinstance(b_tree_page, TableLeafPage) or isinstance(b_tree_page, IndexLeafPage):
        b_tree_pages.append(b_tree_page)
    elif isinstance(b_tree_page, TableInteriorPage) or isinstance(b_tree_page, IndexInteriorPage):
        b_tree_pages.append(b_tree_page)
        b_tree_pages.extend(get_pages_from_b_tree_page(b_tree_page.right_most_page))
        for b_tree_cell in b_tree_page.cells:
            b_tree_pages.extend(get_pages_from_b_tree_page(b_tree_cell.left_child_page))
    else:
        log_message = "The b-tree page is not a BTreePage object but has a type of: {}."
        log_message = log_message.format(type(b_tree_page))
        logger.error(log_message)
        raise ValueError(log_message)

    if not isinstance(b_tree_page, TableInteriorPage):
        for cell in b_tree_page.cells:
            if cell.has_overflow:
                overflow_page = cell.overflow_pages[cell.overflow_page_number]
                b_tree_pages.append(overflow_page)
                while overflow_page.next_overflow_page_number:
                    overflow_page = cell.overflow_pages[overflow_page.next_overflow_page_number]
                    b_tree_pages.append(overflow_page)

    return b_tree_pages
