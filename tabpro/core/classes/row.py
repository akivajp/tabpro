'''
Row class
'''

from collections import (
    OrderedDict
)

from typing import (
    Any,
    Mapping,
)

from ..constants import (
    STAGING_FIELD,
)

from ..functions.get_nested_field_value import get_nested_field_value
from ..functions.search_column_value import search_column_value
from ..functions.set_nested_field_value import set_nested_field_value
from ..functions.set_flat_field_value import set_flat_field_value


class Row(Mapping):
    def __init__(
            self,
        ):
        self.flat = OrderedDict()
        self.nested = OrderedDict()
        self._prefix: str | None = None
        self._staging: Row | None = None

    @property
    def staging(self):
        if self._staging is None:
            row = Row()
            row.flat = self.flat
            row.nested = self.nested
            row._prefix = STAGING_FIELD
            self._staging = row
        return self._staging

    def clone(self):
        cloned = self.__class__.from_dict(self.flat)
        cloned._prefix = self._prefix
        return cloned

    def get(self, key, default=None):
        if self._prefix:
            key = f'{self._prefix}.{key}'
        value, found = get_nested_field_value(self.nested, key)
        if not found:
            return default
        return value
    
    def iter(
        self,
        include_staging: bool = False,
    ):
        for key in self.flat:
            if not include_staging:
                if isinstance(key, str):
                    if key == STAGING_FIELD or key.startswith(STAGING_FIELD + '.'):
                        continue
            yield key

    def items(
        self,
        include_staging: bool = False,
    ):
        for key, value in self.flat.items():
            if not include_staging:
                if isinstance(key, str):
                    if key == STAGING_FIELD or key.startswith(STAGING_FIELD + '.'):
                        continue
            yield key, value

    def keys(
        self,
        include_staging: bool = False,
    ):
        return self.iter(include_staging=include_staging)
    
    def pop(
        self,
        key: str,
        default: Any = None,
    ):
        last_nested = self.nested
        keys = key.split('.')
        for key in keys[:-1]:
            if key not in last_nested:
                return default, False
            last_nested = last_nested[key]
        # delete flat keys
        prefix = key
        for flat_key in list(self.flat.keys()):
            if flat_key == prefix or flat_key.startswith(prefix):
                del self.flat[flat_key]
        return last_nested.pop(keys[-1], default), True
    
    def pop_staging(self):
        return self.pop(STAGING_FIELD)

    def search(
        self,
        field: str,
    ):
        return search_column_value(self.nested, field)

    def __getitem__(self, key):
        if self._prefix:
            key = f'{self._prefix}.{key}'
        value, found = get_nested_field_value(self.nested, key)
        if not found:
            raise KeyError(f'key not found: {key}')
        return value

    def __setitem__(self, key, value):
        if self._prefix:
            key = f'{self._prefix}.{key}'
        set_nested_field_value(self.nested, key, value)
        set_flat_field_value(self.flat, key, value)

    def __contains__(self, key):
        if self._prefix:
            key = f'{self._prefix}.{key}'
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
