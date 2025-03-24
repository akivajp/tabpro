'''
This module contains the function to flatten a nested dictionary.
'''

from collections import OrderedDict
from typing import Mapping

type FlatFieldMap = Mapping[str]
type FieldMap = Mapping[str, str|FieldMap]

def flatten_row(
    mapping: FieldMap,
    parent_key: str = '',
    new_mapping: FlatFieldMap | None = None,
) -> FlatFieldMap:
    if new_mapping is None:
        new_mapping = OrderedDict()
    for key, mapped in mapping.items():
        new_key = f'{parent_key}.{key}' if parent_key else key
        if isinstance(mapped, Mapping):
            flatten_row(mapped, new_key, new_mapping)
        else:
            new_mapping[new_key] = mapped
    return new_mapping
