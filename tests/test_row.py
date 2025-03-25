import pytest
from tabpro.core.classes.row import Row
from tabpro.core.io.loader import Loader

from icecream import ic

def test_row():
    ic.enable()
    row = Row()
    ic(row)
    ic(len(row))
    assert len(row) == 0
    assert row.get('x', 'default') == 'default'
    with pytest.raises(KeyError):
        ic(row['x'])
    row['x'] = 100
    ic(row)
    ic(row.get('x', 'default'))
    row['a.b.c'] = 123
    ic(row)
    row['d1.d2'] = {
        'x': 100,
        'y': 200,
        'd3': {
            'xxx': 111,
            'yyy': 222,
        }
    }
    ic(row)
    row['d1.a'] = [3, 5, 7]
    ic(row)
    ic(row['d1.a.1'])
    ic(row.get('d1.a.2'))
    ic(row.get('d1.a.3'))
    ic(list(row))
