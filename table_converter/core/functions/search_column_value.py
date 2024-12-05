'''
This function is used to search for a column value in a row. It will first search in the '__debug__' field, then in the '__debug__.__original__' field, and finally in the row itself. If the value is found, it will be set in the row and returned.
'''

from .. constants import (
    INPUT_FIELD,
    STAGING_FIELD,
)

from collections import OrderedDict

from . get_nested_field_value import get_nested_field_value

def search_column_value(
    row: OrderedDict,
    column: str,
):
    for key in [
        f'{STAGING_FIELD}.{column}',
        column,
        f'{STAGING_FIELD}.{INPUT_FIELD}.{column}',
    ]:
        value, found = get_nested_field_value(row, key)
        if found:
            return value, key
    return None, None
