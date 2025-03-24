'''
Set a value in a row, both in the flat and nested representations.
'''

from typing import Any

from .. constants import (
    STAGING_FIELD,
)

from . set_flat_field_value import set_flat_field_value
from . set_nested_field_value import set_nested_field_value

from .. types import (
    Row,
)

def set_row_value(
    row: Row,
    target: str,
    value: Any,
):
    set_flat_field_value(row.flat, target, value)
    set_nested_field_value(row.nested, target, value)
    return row

def set_row_staging_value(
    row: Row,
    target: str,
    value: Any,
):
    set_row_value(row, f'{STAGING_FIELD}.{target}', value)
    return row
