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

from . io import (
    get_loader,
    #get_saver,
    get_writer,
    #load,
    save,
)

def get_primary_key(
    row: Mapping,
    keys: list[str],
):
    list_keys = []
    for key in keys:
        value, found = search_column_value(row, key)
        if not found:
            raise KeyError(f'Column not found: {key}, existing columns: {row.keys()}')
        list_keys.append(value)
    primary_key = tuple(list_keys)
    return primary_key

def merge(
    previous_files: list[str],
    modification_files: list[str],
    keys: list[str],
    allow_duplicate_keys: bool = False,
    ignore_not_found: bool = False,
    output_base_data_file: str | None = None,
    output_modified_data_file: str | None = None,
    output_remaining_data_file: str | None = None,
    merge_fields: list[str] | None = None,
):
    ic.enable()
    ic()
    ic(previous_files)
    ic(modification_files)
    ic(keys)
    #dict_key_to_row = {}
    dict_key_to_row = OrderedDict()
    set_modified_keys = set()
    all_base_rows = []
    all_modified_rows = []
    list_ignored_keys = []
    num_modified = 0
    for output_path in [
        output_base_data_file,
        output_modified_data_file,
        output_remaining_data_file,
    ]:
        if output_path:
            get_writer(output_path)
    for previous_file in previous_files:
        if not os.path.exists(previous_file):
            raise FileNotFoundError(f'File not found: {previous_file}')
        loader = get_loader(previous_file)
        ic(len(loader))
        for index, row in enumerate(tqdm(
            loader,
            desc=f'Loading: {previous_file}',
            total=len(loader),
        )):
            primary_key = get_primary_key(row.flat, keys)
            if not allow_duplicate_keys:
                if primary_key in dict_key_to_row:
                    ic(index)
                    raise ValueError(f'Duplicate key: {primary_key}')
            dict_key_to_row[primary_key] = row
            all_base_rows.append(row)
    for modification_file in modification_files:
        if not os.path.exists(modification_file):
            raise FileNotFoundError(f'File not found: {modification_file}')
        loader = get_loader(modification_file)
        ic(len(loader))
        for index, row in enumerate(tqdm(
            loader,
            desc=f'Processing: {modification_file}',
            total=len(loader),
        )):
            primary_key = get_primary_key(row.flat, keys)
            if primary_key not in dict_key_to_row:
                if ignore_not_found:
                    ic(primary_key)
                    ic(row.flat['__staging__.__file_row_index__'])
                    list_ignored_keys.append(primary_key)
                    continue
                ic(index)
                raise ValueError(f'Key not found: {primary_key}')
            previous_row = dict_key_to_row[primary_key]
            all_modified_rows.append(previous_row)
            if merge_fields is None:
                merge_fields = []
                for field in row.flat.keys():
                    if field.startswith('__staging__.'):
                        continue
                    merge_fields.append(field)
            for field in merge_fields:
                value, found = search_column_value(row.flat, field)
                if found:
                    set_row_value(previous_row, field, value)
            set_modified_keys.add(primary_key)
            num_modified += 1
    ic(num_modified)
    if ignore_not_found:
        ic(len(list_ignored_keys))
        ic(list_ignored_keys)
    if output_base_data_file:
        ic('Saving to: ', output_base_data_file)
        save(all_base_rows, output_base_data_file)
    if output_modified_data_file:
        ic('Saving to: ', output_modified_data_file)
        save(all_modified_rows, output_modified_data_file)
    if output_remaining_data_file:
        remaining_rows = []
        for key, row in dict_key_to_row.items():
            if key not in set_modified_keys:
                remaining_rows.append(row)
        ic(len(remaining_rows))
        ic('Saving to: ', output_remaining_data_file)
        save(remaining_rows, output_remaining_data_file)
