# -*- coding: utf-8 -*-

import sys

from collections import OrderedDict

from typing import (
    Any,
)

# 3-rd party modules

from icecream import ic

# local

from ..logging import logger

from .constants import (
    FILE_FIELD,
    ROW_INDEX_FIELD,
    FILE_ROW_INDEX_FIELD,
)

from .functions.search_column_value import search_column_value

from .io import (
    check_writer,
    get_loader,
    get_writer,
    save,
)

from .classes.row import Row

from .console.views import (
    Panel,
)

from .progress import (
    Progress,
)

from .functions.get_primary_key import get_primary_key

def set_diff(
    row: Row,
    field: str,
    value: Any,
    added: bool = True,
):
    if '.' in field:
        head, last = field.rsplit('.', 1)
        if added:
            row[f'diff.{head}.+{last}'] = value
        else:
            row[f'diff.{head}.-{last}'] = value
    else:
        if added:
            row[f'diff.+{field}'] = value
        else:
            row[f'diff.-{field}'] = value
    return row

def compare(
    path1: str,
    path2: str,
    output_path: str,
    query_keys: list[str],
    compare_keys: list[str] | None = None,
    verbose: bool = False,
):
    progress = Progress(
        #redirect_stdout = False,
        #transient=True,
    )
    progress.start()
    console = progress.console
    #console.log('previous files: ', previous_files)
    #console.log('modification files: ', modification_files)
    console.log('file1: ', path1)
    console.log('file2: ', path2)
    console.log('query keys: ', query_keys)
    console.log('compare keys: ', compare_keys)
    list_dict_key_to_row: list[dict[Any, Row]] = [{},{}]
    set_query_values = set()
    num_modified = 0
    if output_path:
        check_writer(output_path)
    loaders = [get_loader(path) for path in [path1, path2]]
    for loader_index, loader in enumerate(loaders):
        console.log('loading file: ', [path1, path2][loader_index])
        console.log('# rows: ', len(loader))
        dict_key_to_row = list_dict_key_to_row[loader_index] = {}
        for row_index, row in enumerate(loader):
            query_value = get_primary_key(row, query_keys)
            if query_value in dict_key_to_row:
                raise ValueError(
                    f'Key {query_value} already exists in file: {path1 if loader_index == 0 else path2}'
                )
            dict_key_to_row[query_value] = row
            set_query_values.add(query_value)
    diff_rows: list[Row] = []
    for query_value in sorted(set_query_values):
        row1 = list_dict_key_to_row[0].get(query_value)
        row2 = list_dict_key_to_row[1].get(query_value)
        diff_row = Row()
        list_compare_keys = []
        if compare_keys is not None:
            list_compare_keys = compare_keys
        else:
            for row in [row1, row2]:
                if row is not None:
                    for key in row.keys():
                        if key not in list_compare_keys:
                            list_compare_keys.append(key)
        if len(query_value) == 1:
            key_field = 'key'
            key_value = query_value[0]
        else:
            key_field = 'keys'
            key_value = query_value
        if row2 is None:
            assert row1 is not None
            diff_row[f'-{key_field}'] = f'{key_value}'
            for key in list_compare_keys:
                if key in row1:
                    value = row1[key]
                    set_diff(diff_row, key, value, added=False)
        elif row1 is None:
            diff_row[f'+{key_field}'] = f'{key_value}'
            for key in list_compare_keys:
                if key in row2:
                    value = row2[key]
                    set_diff(diff_row, key, value, added=True)
        else:
            diff_row[key_field] = key_value
            for key in list_compare_keys:
                if key in row1:
                    if key in row2:
                        if row1[key] != row2[key]:
                            set_diff(diff_row, key, row1[key], added=False)
                            set_diff(diff_row, key, row2[key], added=True)
                    else:
                        set_diff(diff_row, key, row1[key], added=False)
                elif key in row2:
                    set_diff(diff_row, key, row2[key], added=True)
        if len(diff_row) > 1:
            diff_rows.append(diff_row)
    console.log('# diff rows: ', len(diff_rows))
    if output_path is None:
        if sys.stdout.isatty():
            if len(diff_rows) > 0:
                console.print(Panel(
                    diff_rows[0],
                    title='first diff row',
                    title_justify='left',
                    border_style='yellow',
                ))
            else:
                console.print(Panel(
                    'no diff rows',
                    title='diff',
                    title_justify='left',
                    border_style='green',
                ))
    else:
        save(
            diff_rows,
            output_path,
            progress=progress,
        )
    progress.stop()
