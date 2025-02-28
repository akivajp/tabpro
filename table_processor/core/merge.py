# -*- coding: utf-8 -*-

import json
import math
import os

from collections import OrderedDict

from typing import (
    Mapping,
)

# 3-rd party modules

from icecream import ic
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# local

from . functions.flatten_row import flatten_row
from . functions.get_nested_field_value import get_nested_field_value
from . functions.get_nested_field_value import get_nested_field_value
from . functions.nest_row import nest_row as nest
from . functions.search_column_value import search_column_value
from . functions.set_nested_field_value import set_nested_field_value
from . functions.set_row_value import (
    set_row_value,
    set_row_staging_value,
)

from . actions import (
    prepare_row,
)

from . convert import (
    load,
    save,
)

def merge(
    previous_files: list[str],
    modification_files: list[str],
    keys: list[str],
    allow_duplicate_keys: bool = False,
    ignore_not_found: bool = False,
    output_file: str | None = None,
):
    ic.enable()
    ic()
    ic(previous_files)
    ic(modification_files)
    ic(keys)
    dict_key_to_row = {}
    all_rows = []
    list_ignored_keys = []
    num_modified = 0
    for previous_file in previous_files:
        if not os.path.exists(previous_file):
            raise FileNotFoundError(f'File not found: {previous_file}')
        df = load(previous_file)
        # NOTE: NaN を None に変換しておかないと厄介
        df = df.replace([np.nan], [None])
        #ic(df)
        ic(len(df))
        #ic(df.columns)
        #ic(df.iloc[0])
        for index, flat_row in tqdm(
            df.iterrows(),
            desc=f'Loading: {previous_file}',
            total=len(df),
        ):
            row = prepare_row(flat_row)
            list_keys = []
            for key in keys:
                value, found = search_column_value(row.nested, key)
                if not found:
                    raise KeyError(f'Column not found: {key}, existing columns: {row.flat.keys()}')
                list_keys.append(value)
            primary_key = tuple(list_keys)
            #ic(key)
            if not allow_duplicate_keys:
                if primary_key in dict_key_to_row:
                    ic(index)
                    raise ValueError(f'Duplicate key: {key}')
            dict_key_to_row[primary_key] = row
            all_rows.append(row)
    for modification_file in modification_files:
        if not os.path.exists(modification_file):
            raise FileNotFoundError(f'File not found: {modification_file}')
        df = load(modification_file)
        # NOTE: NaN を None に変換しておかないと厄介
        df = df.replace([np.nan], [None])
        #ic(df)
        ic(len(df))
        #ic(df.columns)
        #ic(df.iloc[0])
        for index, flat_row in tqdm(
            df.iterrows(),
            desc=f'Processing: {modification_file}',
            total=len(df),
        ):
            row = prepare_row(flat_row)
            list_keys = []
            for key in keys:
                value, found = search_column_value(row.nested, key)
                if not found:
                    raise KeyError(f'Column not found: {key}, existing columns: {row.flat.keys()}')
                list_keys.append(value)
            primary_key = tuple(list_keys)
            #ic(key)
            #if key not in dict_key_to_row:
            #    dict_key_to_row[key] = row
            #else:
            #    dict_key_to_row[key].flat.update(row.flat)
            if primary_key not in dict_key_to_row:
                if ignore_not_found:
                    ic(primary_key)
                    ic(row.flat['__staging__.__file_row_index__'])
                    list_ignored_keys.append(primary_key)
                    continue
                ic(index)
                raise ValueError(f'Key not found: {primary_key}')
            previous_row = dict_key_to_row[primary_key]
            #ic(previous_row)
            #ic(previous_row.flat)
            for key, value in row.flat.items():
                if key.startswith('__staging__.'):
                    continue
                #ic(key)
                #ic(key, value)
                set_row_value(previous_row, key, value)
                #set_row_value(previous_row, '指示追従性？', 'test')
                #set_row_value(previous_row, 'modified', True)
            #ic(previous_row)
            #ic(previous_row.flat)
            #raise
            num_modified += 1
    ic(num_modified)
    if ignore_not_found:
        ic(len(list_ignored_keys))
        ic(list_ignored_keys)
    if output_file:
        #all_df = pd.DataFrame(all_rows)
        all_df = pd.DataFrame([row.flat for row in all_rows])
        ic('Saving to: ', output_file)
        save(all_df, output_file)
