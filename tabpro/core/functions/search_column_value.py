'''
行の中から列の値を探す。

優先順位は次のとおり。

1. staging (__staging__.<column>) — アクションが書き込んだ値
2. row 本体 (<column>)
3. staging の入力値記録 (__staging__.__input__.<column>) — 変換前の値
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
