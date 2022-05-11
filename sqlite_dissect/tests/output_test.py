from ctypes import pointer
import pytest
from sqlite_dissect.output import get_page_breakdown, get_pointer_map_entries_breakdown
from sqlite_dissect.exception import OutputError
from sqlite_dissect.constants import PAGE_TYPE

class MockPage:
    def __init__(self, page_type, page_number, page_bytes=None):
        self.page_type = page_type
        self.page_number = page_number
        self.page_bytes = page_bytes

class MockVersion:
    def __init__(self, pointer_map_pages, number):
        self.pointer_map_pages = pointer_map_pages
        self.number = number

class MockPointerMapPage:
    def __init__(self, pointer_map_entries, number, page_version_number):
        self.pointer_map_entries = pointer_map_entries
        self.number = number
        self.page_version_number = page_version_number

def test_get_page_breakdown():
    # if pages contains no values, returned value should be empty.
    assert get_page_breakdown({}) == {
        "LOCK_BYTE": [], 
        "FREELIST_TRUNK": [], 
        "FREELIST_LEAF": [], 
        "B_TREE_TABLE_INTERIOR": [], 
        "B_TREE_TABLE_LEAF": [],
        "B_TREE_INDEX_INTERIOR": [], 
        "B_TREE_INDEX_LEAF": [], 
        "OVERFLOW": [], 
        "POINTER_MAP": []
    }

    # if pages contains a page with an unrecognized page type, the function will crash.
    with pytest.raises(Exception):
        _ = get_page_breakdown({0: MockPage('unrecognized', 0)})

    # if pages contains all valid pages, returned value should be a list of page types.
    assert get_page_breakdown({
        0: MockPage('LOCK_BYTE', 0), 
        1: MockPage('FREELIST_TRUNK', 1), 
        89: MockPage('LOCK_BYTE', 89)
    }) == {
        "LOCK_BYTE": [0, 89], 
        "FREELIST_TRUNK": [1], 
        "FREELIST_LEAF": [], 
        "B_TREE_TABLE_INTERIOR": [], 
        "B_TREE_TABLE_LEAF": [],
        "B_TREE_INDEX_INTERIOR": [], 
        "B_TREE_INDEX_LEAF": [], 
        "OVERFLOW": [], 
        "POINTER_MAP": []
    }

def test_get_pointer_map_entries_breakdown():
    # if version.pointer_map_pages is empty, then return value is empty
    assert get_pointer_map_entries_breakdown(MockVersion([], 0)) == []

    # if version.pointer_map_pages contains a pointer_map_page without any entries, OutputError is raised
    with pytest.raises(OutputError):
        _ = get_pointer_map_entries_breakdown(MockVersion([MockPointerMapPage([], 0, 0)], 0))

    # if version is valid, return value is a list of tuples containing map page info
    assert get_pointer_map_entries_breakdown(MockVersion([
        MockPointerMapPage([MockPage("LOCK_BYTE", 0), MockPage("FREELIST_TRUNK", 1), MockPage("FREELIST_LEAF", 2)], 0, 0), 
        MockPointerMapPage([MockPage("LOCK_BYTE", 3), MockPage("FREELIST_TRUNK", 4), MockPage("FREELIST_LEAF", 5)], 1, 0)
    ], 0)) == [
        (0, 1, 0, 0, '4c4f434b5f42595445'), 
        (0, 1, 1, 1, '465245454c4953545f5452554e4b'), 
        (0, 2, 2, 1, '465245454c4953545f4c454146'), 
        (1, 2, 3, 2, '4c4f434b5f42595445'), 
        (1, 4, 4, 1, '465245454c4953545f5452554e4b'), 
        (1, 5, 5, 1, '465245454c4953545f4c454146')
    ]