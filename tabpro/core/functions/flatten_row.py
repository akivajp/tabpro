'''
This module contains the function to flatten a nested dictionary.
'''

from collections import OrderedDict
from typing import (
    Any,
    Mapping,
    TypeAlias,
)

# NOTE:
#   PEP 695 の `type` 文は Python 3.12 以降でしか使えないため、
#   3.10 以降をサポートするために TypeAlias による定義を用いる。
#   `type` 文は右辺が遅延評価されるが、通常の代入は即時評価となるため、
#   右辺は実行時に評価できる形でなければならない点に注意。
FlatFieldMap: TypeAlias = Mapping[str, Any]
# NOTE:
#   自己参照する型エイリアスは即時評価できないため、文字列の前方参照とする。
FieldMap: TypeAlias = "Mapping[str, str | FieldMap]"

def flatten_row(
    mapping: FieldMap,
    parent_key: str = '',
    new_mapping: FlatFieldMap | None = None,
) -> FlatFieldMap:
    '''
    ネストした辞書を、キーをドットで連結したフラットな辞書に変換する。

    Args:
        mapping: 変換対象のネストした辞書。
        parent_key: 再帰呼び出し時に引き継ぐ親キー。
        new_mapping: 再帰呼び出し時に引き継ぐ変換先の辞書。

    Returns:
        ドット区切りのキーを持つフラットな辞書。
    '''
    if new_mapping is None:
        new_mapping = OrderedDict()
    for key, mapped in mapping.items():
        new_key = f'{parent_key}.{key}' if parent_key else key
        if isinstance(mapped, Mapping):
            flatten_row(mapped, new_key, new_mapping)
        else:
            new_mapping[new_key] = mapped
    return new_mapping
