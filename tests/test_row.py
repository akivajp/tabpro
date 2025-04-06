import pytest
from tabpro.core.classes.row import Row
from tabpro.core.io.loader import Loader

from icecream import ic
from collections import OrderedDict

def test_row():
    # Enable debug output using icecream
    ic.enable()
    
    # Test empty row initialization
    row = Row()
    ic(row)
    ic(len(row))
    assert len(row) == 0
    
    # Test get method with default value for non-existent key
    assert row.get('x', 'default') == 'default'
    
    # Test KeyError when accessing non-existent key
    with pytest.raises(KeyError):
        ic(row['x'])
    
    # Test setting and getting a simple key-value pair
    row['x'] = 100
    ic(row)
    ic(row.get('x', 'default'))
    
    # Test setting and getting a nested key
    row['a.b.c'] = 123
    ic(row)
    
    # Test setting and getting a complex nested structure
    row['d1.d2'] = {
        'x': 100,
        'y': 200,
        'd3': {
            'xxx': 111,
            'yyy': 222,
        }
    }
    ic(row)
    
    # Test setting and accessing array values
    row['d1.a'] = [3, 5, 7]
    ic(row)
    
    # Test array indexing with nested keys
    ic(row['d1.a.1'])
    ic(row.get('d1.a.2'))
    ic(row.get('d1.a.3'))
    
    # Test row iteration
    ic(list(row))

def test_row_initialization():
    # Test basic initialization and empty state
    row = Row()
    assert len(row) == 0
    assert not row
    assert 'x' not in row

def test_row_basic_operations():
    # Test basic get/set operations and existence checks
    row = Row()
    row['x'] = 100
    assert row['x'] == 100
    assert row.get('x') == 100
    assert row.get('y', 'default') == 'default'
    assert 'x' in row
    assert 'y' not in row

def test_row_nested_operations():
    # Test operations with nested keys
    row = Row()
    row['a.b.c'] = 123
    assert row['a.b.c'] == 123
    assert 'a.b.c' in row
    
    # Test complex nested structure
    row['d1.d2'] = {
        'x': 100,
        'y': 200,
        'd3': {
            'xxx': 111,
            'yyy': 222,
        }
    }
    assert row['d1.d2.x'] == 100
    assert row['d1.d2.d3.xxx'] == 111

def test_row_iteration():
    # Test iteration methods (__iter__, keys, items)
    row = Row()
    row['a'] = 1
    row['b'] = 2
    row['c.d'] = 3
    
    assert list(row) == ['a', 'b', 'c.d']
    assert list(row.keys()) == ['a', 'b', 'c.d']
    assert list(row.items()) == [('a', 1), ('b', 2), ('c.d', 3)]

def test_row_clone():
    # Test cloning functionality and independence
    row = Row()
    row['a'] = 1
    row['b.c'] = 2
    
    cloned = row.clone()
    assert cloned['a'] == 1
    assert cloned['b.c'] == 2
    
    # Verify that clone is independent
    cloned['a'] = 3
    assert row['a'] == 1
    assert cloned['a'] == 3

def test_row_from_dict():
    # Test creation from dictionary
    data = {
        'a': 1,
        'b': {'c': 2},
        'd.e': 3
    }
    row = Row.from_dict(data)
    assert row['a'] == 1
    assert row['b.c'] == 2
    assert row['d.e'] == 3

def test_row_staging():
    # Test staging functionality
    row = Row()
    row['a'] = 1
    
    staging = row.staging
    staging['b'] = 2
    
    assert 'b' in staging
    assert 'b' not in row
    
    # Verify that staging changes don't affect original
    staging['a'] = 3
    assert row['a'] == 1
    assert staging['a'] == 3

def test_row_search():
    # Test search functionality
    row = Row()
    row['a.b.c'] = 1
    row['a.b.d'] = 2
    row['x.y.z'] = 3
    
    results, prefix = row.search('a.b')
    assert isinstance(results, OrderedDict)
    assert results['c'] == 1
    assert results['d'] == 2
    assert prefix == 'a.b'
    assert 'x.y.z' not in results

def test_row_error_cases():
    # Test error handling
    row = Row()
    with pytest.raises(KeyError):
        _ = row['nonexistent']
    
    with pytest.raises(KeyError):
        _ = row['a.b.c']  # Non-existent nested key
