'''
This function is used to search for a column value in a row. It will first search in the '__debug__' field, then in the '__debug__.__original__' field, and finally in the row itself. If the value is found, it will be set in the row and returned.
'''

from .. constants import STAGING_FIELD

from collections import OrderedDict

from . get_field_value import get_field_value
from . set_field_value import set_field_value

def search_column_value(
    row: OrderedDict,
    column: str,
):
    if STAGING_FIELD in row:
        value, found = get_field_value(row[STAGING_FIELD], column)
        if found:
            return value, True
    value, found = get_field_value(row[STAGING_FIELD], column)
    original, found = get_field_value(row, f'{STAGING_FIELD}.__original__')
    if found:
        value, found = get_field_value(original, column)
        if found:
            return value, True
    value, found = get_field_value(row, column)
    if found:
        set_field_value(row, column, value)
        return value, True
    return None, False
