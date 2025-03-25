'''
Row class
'''

from collections import (
    OrderedDict
)

from typing import (
    Mapping,
)

from .. functions.get_nested_field_value import get_nested_field_value
from .. functions.set_nested_field_value import set_nested_field_value
from .. functions.set_flat_field_value import set_flat_field_value


class Row(Mapping):
    def __init__(self):
        self.flat = OrderedDict()
        self.nested = OrderedDict()

    def clone(self):
        return self.__class__.from_dict(self.flat)

    def get(self, key, default=None):
        value, found = get_nested_field_value(self.nested, key)
        if not found:
            return default
        return value

    def __getitem__(self, key):
        value, found = get_nested_field_value(self.nested, key)
        if not found:
            raise KeyError(f'Key not found: {key}')
        return value

    def __setitem__(self, key, value):
        set_nested_field_value(self.nested, key, value)
        set_flat_field_value(self.flat, key, value)

    def __contains__(self, key):
        _, found = get_nested_field_value(self.nested, key)
        return found

    def __iter__(self):
        return iter(self.flat)

    def __len__(self):
        return len(self.flat)

    def __repr__(self):
        return f'Row(flat={self.flat}, nested={self.nested})'
    
    @staticmethod
    def from_dict(data: dict):
        row = Row()
        for key, value in data.items():
            row[key] = value
        return row
