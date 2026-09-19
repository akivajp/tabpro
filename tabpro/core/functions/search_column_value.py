'''
行の中から列の値を探す。

優先順位は次のとおり。

1. staging (__staging__.<column>) — アクションが書き込んだ値
2. row 本体 (<column>)
3. staging の入力値記録 (__staging__.__input__.<column>) — 変換前の値

NOTE:
    戻り値は (値, 見つかった位置の完全なキー文字列) である。
    2番目の要素は真偽値ではなく、見つからなければ None、
    見つかれば '__staging__.x' / 'x' のような非空のキー文字列。
    呼び出し側は truthiness (見つかれば必ず真) で found 扱いしてよい。
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
