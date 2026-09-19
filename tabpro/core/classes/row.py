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
        # NOTE:
        #   staging ビュー (プレフィックス __staging__ を持つ Row) では、
        #   staged キーをプレフィックスを外して返す。
        #   以前は共有する flat をそのまま反復していたため、
        #   staging.keys() が staged 値ではなくデータ列を返す不整合があった。
        if self._prefix is not None:
            for key in self.flat:
                if isinstance(key, str) and key.startswith(self._prefix + '.'):
                    yield key[len(self._prefix) + 1:]
            return
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
        # NOTE:
        #   staging ビューでは staged キーと値の組を返す (iter と同じ規約)。
        if self._prefix is not None:
            for key, value in self.flat.items():
                if isinstance(key, str) and key.startswith(self._prefix + '.'):
                    yield key[len(self._prefix) + 1:], value
            return
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
        '''
        指定したキーを取り除き、その値を返す。

        NOTE:
            Row は Mapping を継承しているため、pop は Mapping の契約
            (取り除いた値そのものを返す) に従う。
            見つかったかどうかも確認したい場合は pop_found を用いる。

            以前はフラット表現の削除条件が壊れていた。
            ループ変数を使い回していたため対象がパスの手前の要素になり、
            さらに startswith による前方一致だったため、
            omit:x が xx や xyz まで巻き込んで削除していた
            (フラット表現を使う CSV / TSV / Excel の出力でのみ現れる)。

        Args:
            key: 取り除くキー。ドット区切りでネストを辿る。
            default: 見つからなかった場合に返す値。

        Returns:
            取り除いた値。見つからなかった場合は default。
        '''
        value, found = self.pop_found(key)
        if not found:
            return default
        return value

    def pop_found(
        self,
        key: str,
    ):
        '''
        指定したキーを取り除き、(値, 見つかったか) の組を返す。

        Args:
            key: 取り除くキー。ドット区切りでネストを辿る。

        Returns:
            (取り除いた値, 見つかったか) の組。
            見つからなかった場合は (None, False)。
        '''
        last_nested = self.nested
        keys = key.split('.')
        for part in keys[:-1]:
            if part not in last_nested:
                return None, False
            last_nested = last_nested[part]
        # NOTE:
        #   フラット表現からは、そのキー自身とその子孫だけを削除する。
        #   子孫の判定には区切り文字まで含めて一致を見る。
        prefix = f'{key}.'
        for flat_key in list(self.flat.keys()):
            if flat_key == key or (
                isinstance(flat_key, str) and flat_key.startswith(prefix)
            ):
                del self.flat[flat_key]
        # NOTE:
        #   葉のキーが存在しない場合も found=True を返していたため、
        #   見つからなかった場合の判定をここで行う。
        if keys[-1] not in last_nested:
            return None, False
        return last_nested.pop(keys[-1]), True

    def pop_staging(self):
        return self.pop_found(STAGING_FIELD)

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
        # NOTE:
        #   keys() / items() と同じく staging を除外する。
        #   以前はフラット表現そのものを反復していたため、
        #   iter と keys で結果が一致しない状態だった。
        return self.iter(include_staging=False)

    def __len__(self):
        # NOTE:
        #   Mapping の契約 (len と iter の一致) に揃えるため、
        #   staging を除外した件数を返す。
        return sum(1 for _ in self.iter())

    def __repr__(self):
        return f'Row(flat={self.flat}, nested={self.nested})'
    
    @staticmethod
    def from_dict(data: dict):
        row = Row()
        for key, value in data.items():
            row[key] = value
        return row
