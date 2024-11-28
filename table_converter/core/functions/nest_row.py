'''
This function is used to nest a row. It is used to nest a row that has been unnested.
'''

import math

from collections import OrderedDict
from typing import Mapping

from .set_nested_field_value import set_nested_field_value

def nest_row(
    row: Mapping,
    remove_nan: bool = True,
):
    new_row = OrderedDict()
    for key, value in row.items():
        if isinstance(value, OrderedDict):
            value = nest_row(value)
        if isinstance(value, float):
            if math.isnan(value):
                if remove_nan:
                    continue
        set_nested_field_value(new_row, key, value)
    return new_row
