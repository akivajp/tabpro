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

from . config import (
    AssignArrayConfig,
    PushConfig,
    setup_config,
    setup_pick_with_args,
)
from . constants import (
    FILE_FIELD,
    ROW_INDEX_FIELD,
    FILE_ROW_INDEX_FIELD,
    INPUT_FIELD,
    STAGING_FIELD,
)
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
    do_actions,
    pop_row_staging,
    prepare_row,
    remap_columns,
    setup_actions_with_args,
)

from . types import (
    GlobalStatus,
)

from . convert import (
    load,
    save,
)

from . functions.assign_id import get_id

def convert(
    input_files: list[str],
    output_file: str | None = None,
    output_file_filtered_out: str | None = None,
    config_path: str | None = None,
    output_debug: bool = False,
    list_actions: list[str] | None = None,
    list_pick_columns: list[str] | None = None,
    action_delimiter: str = ':',
    verbose: bool = False,
    ignore_file_rows: list[str] | None = None,
):
    ic.enable()
    ic()
    ic(input_files)
    df_list = []
    row_list_filtered_out = []
    set_ignore_file_rows = set()
    global_status = GlobalStatus()
    config = setup_config(config_path)
    ic(config)
    if ignore_file_rows:
        set_ignore_file_rows = set(ignore_file_rows)
    if list_pick_columns:
        setup_pick_with_args(config, list_pick_columns)
    if list_actions:
        setup_actions_with_args(
            config,
            list_actions,
            action_delimiter=action_delimiter
        )
    if output_file:
        ext = os.path.splitext(output_file)[1]
        if ext not in dict_savers:
            raise ValueError(f'Unsupported file type: {ext}')
        saver = dict_savers[ext]
    ic(config)
    #return # debug return
    for input_file in input_files:
        ic(input_file)
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        base_name = os.path.basename(input_file)
        ext = os.path.splitext(input_file)[1]
        ic(ext)
        if ext not in dict_loaders:
            raise ValueError(f'Unsupported file type: {ext}')
        df = dict_loaders[ext](input_file)
        # NOTE: NaN を None に変換しておかないと厄介
        df = df.replace([np.nan], [None])
        #ic(df)
        #ic(len(df))
        #ic(df.columns)
        #ic(df.iloc[0])
        #new_rows = []
        new_flat_rows = []
        for index, flat_row in df.iterrows():
            file_row_index = f'{input_file}:{index}'
            if file_row_index in set_ignore_file_rows:
                continue
            short_file_row_index = f'{base_name}:{index}'
            if short_file_row_index in set_ignore_file_rows:
                continue
            #if flat_row.empty:
            #    continue
            orig_row = prepare_row(flat_row)
            row = prepare_row(flat_row)
            if STAGING_FIELD not in orig_row.nested:
                set_row_staging_value(row, FILE_FIELD, input_file)
                set_row_staging_value(row, FILE_ROW_INDEX_FIELD, file_row_index)
                set_row_staging_value(row, ROW_INDEX_FIELD, index)
                set_row_staging_value(row, INPUT_FIELD, orig_row.nested)
            if config.process.assign_array:
                row.flat= assign_array(row.flat, config.process.assign_array)
            if config.process.push:
                row.flat = push_fields(row.flat, config.process.push)
            if config.process.assign_length:
                row.flat = assign_length(row.flat, config.process.assign_length)
            if config.actions:
                try:
                    new_row = do_actions(global_status, row, config.actions)
                    if new_row is None:
                        if not output_debug:
                            pop_row_staging(row)
                        if verbose:
                            ic('Filtered out: ', row.flat)
                        if output_file_filtered_out:
                            row_list_filtered_out.append(row.flat)
                        continue
                    row = new_row
                except Exception as e:
                    if verbose:
                        ic(index)
                        ic(flat_row)
                        ic(row.flat)
                    raise e
            if config.pick:
                remap_columns(row, config.pick)
            if not output_debug:
                pop_row_staging(row)
            new_flat_rows.append(row.flat)
        new_df = pd.DataFrame(new_flat_rows)
        df_list.append(new_df)
        # NOTE: concatの仕様が変わり、all-NAの列を含むdfを連結しようとすると警告が出るようになった
        #if ic(new_df.dropna(axis=1, how='all').empty):
        #    ic(new_df.dropna(axis=1, how='all'))
        #    raise ValueError('No rows to output.')
        #df_list.append(new_df.dropna(axis=1, how='all'))
    all_df = pd.concat(df_list)
    #ic(all_df)
    ic(len(all_df))
    #ic(all_df.columns)
    #ic(all_df.iloc[0])
    if output_file:
        ic('Saing to: ', output_file)
        saver(all_df, output_file)
    else:
        ic(all_df)
    if row_list_filtered_out:
        df_filtered_out = pd.DataFrame(row_list_filtered_out)
        ic('Saving filtered out to: ', output_file_filtered_out)
        saver(df_filtered_out, output_file_filtered_out)

def merge(
    previous_files: list[str],
    modification_files: list[str],
    primary_keys: list[str],
):
    ic.enable()
    ic()
    ic(previous_files)
    ic(modification_files)
    ic(primary_keys)
    all_rows = []
    global_status = GlobalStatus()
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
            desc=f'Processing: {previous_file}',
            total=len(df),
        ):
            row = prepare_row(flat_row)
            #key = tuple([get_nested_field_value(row.flat, key) for key in primary_keys])
            all_rows.append(row)
