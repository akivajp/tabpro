'''
Set a value in a flat row dictionary with a nested key.
'''

from collections import OrderedDict
from typing import Any

def set_flat_field_value(
    flat_row: OrderedDict,
    target: str,
    value: Any,
    depth: int = 0,
):
    if depth > 10:
        raise ValueError(
            'Depth too high'
        )
    if isinstance(value, dict):
        for key in value.keys():
            set_flat_field_value(flat_row, f'{target}.{key}', value[key], depth + 1)
    else:
        flat_row[target] = value
    return flat_row
