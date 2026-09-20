from rich.pretty import pretty_repr

from ..classes.row import Row

from ...logging import logger


def get_primary_key(
    row: Row,
    keys: list[str],
):
    list_keys = []
    for key in keys:
        value, found = row.search(key)
        if not found:
            logger.debug('row: ')
            logger.debug(pretty_repr(row.flat))
            existing_first20 = list(row.keys())[:20]
            raise KeyError(f'Column not found: {key}, existing columns: {existing_first20}')
        list_keys.append(value)
    primary_key = tuple(list_keys)
    return primary_key

def make_match_key(
    primary_key: tuple,
):
    '''
    行を突き合わせるための照合キーを返す。

    入力形式によってはキーの型が混ざる (SQLite 由来は int、
    Excel 由来は str など) ため、str() で正準化して照合する。
    文字列同士の区別は保たれる ('01' と '1' は異なる値) ので、
    同型同士の照合の厳密さは従来どおりで、型を跨ぐ一致だけが緩くなる。

    Args:
        primary_key: get_primary_key() が返したキーの組。

    Returns:
        正準化された照合キー。
    '''
    return tuple(str(value) for value in primary_key)
