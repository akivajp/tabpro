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
            #progress = Progress()
            #progress.console.print(Panel(
            #    row.nested,
            #))
            logger.debug('row: ')
            logger.debug(pretty_repr(row.flat))
            existing_first20 = list(row.keys())[:20]
            raise KeyError(f'Column not found: {key}, existing columns: {existing_first20}')
        list_keys.append(value)
    primary_key = tuple(list_keys)
    return primary_key
