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

import difflib

def compare(
    path1: str,
    path2: str,
    output_path: str,
    primary_keys: list[str],
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
    console.log('keys: ', primary_keys)
    list_dict_key_to_row: list[dict[Any, Row]] = [{},{}]
    set_primary_keys = set()
    #set_modified_keys = set()
    #all_base_rows = []
    #all_modified_rows = []
    #list_ignored_keys = []
    num_modified = 0
    if output_path:
        check_writer(output_path)
    loaders = [get_loader(path) for path in [path1, path2]]
    for loader_index, loader in enumerate(loaders):
        console.log('loading file: ', [path1, path2][loader_index])
        console.log('# rows: ', len(loader))
        dict_key_to_row = list_dict_key_to_row[loader_index] = {}
        for row_index, row in enumerate(loader):
            primary_key = get_primary_key(row, primary_keys)
            if primary_key in dict_key_to_row:
                raise ValueError(
                    f'Key {primary_key} already exists in file: {path1 if loader_index == 0 else path2}'
                )
            dict_key_to_row[primary_key] = row
            set_primary_keys.add(primary_key)
    diff_rows: list[Row] = []
    for primary_key in sorted(set_primary_keys):
        row1 = list_dict_key_to_row[0].get(primary_key)
        row2 = list_dict_key_to_row[1].get(primary_key)
        diff_row = Row()
        if len(primary_key) == 1:
            key_field = 'key'
            key_value = primary_key[0]
        else:
            key_field = 'keys'
            key_value = primary_key
        if row2 is None:
            diff_row[f'-{key_field}'] = f'{key_value}'
            for key, value in row1.items():
                diff_row[f'diff.-{key}'] = value
        elif row1 is None:
            diff_row[f'+{key_field}'] = f'{key_value}'
            for key, value in row2.items():
                diff_row[f'diff.+{key}'] = value
        else:
            diff_row[key_field] = key_value
            set_compare_keys = set()
            if compare_keys is not None:
                set_compare_keys = set(compare_keys)
            else:
                for row in [row1, row2]:
                    for key in row.keys():
                        if key not in set_compare_keys:
                            set_compare_keys.add(key)
            for key in set_compare_keys:
                if key not in row1:
                    diff_row[f'diff.-{key}'] = row1[key]
                elif key not in row2:
                    diff_row[f'diff.+{key}'] = row2[key]
                elif row1[key] != row2[key]:
                    diff_row[f'diff.-{key}'] = row1[key]
                    diff_row[f'diff.+{key}'] = row2[key]
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
