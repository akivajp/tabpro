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

# local

from . config import (
    AssignArrayConfig,
    PushConfig,
    setup_config,
    setup_pick_with_args,
)
from . constants import (
    FILE_FIELD,
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

dict_loaders: dict[str, callable] = {}
def register_loader(
    ext: str,
):
    def decorator(loader):
        dict_loaders[ext] = loader
        return loader
    return decorator

dict_savers: dict[str, callable] = {}
def register_saver(
    ext: str,
):
    def decorator(saver):
        dict_savers[ext] = saver
        return saver
    return decorator

@register_loader('.csv')
def load_csv(
    input_file: str,
):
    # utf-8
    #df = pd.read_csv(input_file)
    # UTF-8 with BOM
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    return df

@register_loader('.xlsx')
def load_excel(
    input_file: str,
):
    df = pd.read_excel(input_file)
    return df

@register_loader('.json')
def load_json(
    input_file: str,
):
    with open(input_file, 'r') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f'Invalid JSON array data: {input_file}')
    #ic(data[0])
    rows = []
    for row in data:
        new_row = flatten_row(row)
        rows.append(new_row)
    df = pd.DataFrame(rows)
    return df

@register_saver('.json')
def save_json(
    df: pd.DataFrame,
    output_file: str,
):
    # NOTE: この方法だとスラッシュがすべてエスケープされてしまった
    #df.to_json(
    #    output_file,
    #    orient='records',
    #    force_ascii=False,
    #    indent=2,
    #    escape_forward_slashes=False,
    #)
    #ic(df.iloc[0])
    data = df.to_dict(orient='records')
    #ic(data[0])
    data = [nest(row) for row in data]
    #ic(data[0])
    with open(output_file, 'w') as f:
        json.dump(
            data,
            f,
            indent=2,
            ensure_ascii=False,
        )

@register_loader('.jsonl')
def load_jsonl(
    input_file: str,
):
    rows = []
    with open(input_file, 'r') as f:
        for line in f:
            row = json.loads(line)
            rows.append(row)
    df = pd.DataFrame(rows)
    return df

@register_saver('.jsonl')
def save_jsonl(
    df: pd.DataFrame,
    output_file: str,
):
    # NOTE: この方法だとスラッシュがすべてエスケープされてしまった
    #df.to_json(
    #    output_file,
    #    orient='records',
    #    lines=True,
    #    force_ascii=False,
    #)
    with open(output_file, 'w') as f:
        for index, row in df.iterrows():
            data = row.to_dict()
            json.dump(
                data,
                f,
                ensure_ascii=False,
            )
            f.write('\n')

@register_saver('.csv')
def save_csv(
    df: pd.DataFrame,
    output_file: str,
):
    # utf-8
    #df.to_csv(output_file, index=False)
    # UTF-8 with BOM
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

@register_saver('.xlsx')
def save_excel(
    df: pd.DataFrame,
    output_file: str,
):
    # openpyxl
    df.to_excel(output_file, index=False)
    # xlsxwriter
    #writer = pd.ExcelWriter(
    #    output_file,
    #    engine='xlsxwriter',
    #    engine_kwargs={
    #        'options': {
    #            'strings_to_urls': False,
    #        },
    #    }
    #)
    #df.to_excel(writer, index=False)
    #writer.close()

def assign_array(
    row: OrderedDict,
    dict_config: Mapping[str, list[AssignArrayConfig]],
):
    new_row = OrderedDict(row)
    #ic(dict_config)
    for key, config in dict_config.items():
        array = []
        for item in config:
            value, found = search_column_value(row, item.field)
            if found and value is not None:
                array.append(value)
            elif not item.optional:
                array.append(None)
        new_row[f'{STAGING_FIELD}.{key}'] = array
    return new_row

def search_column_value_from_nested(
    nested_row: OrderedDict,
    column: str,
):
    if STAGING_FIELD in nested_row:
        value, found = get_nested_field_value(nested_row[STAGING_FIELD], column)
        if found:
            return value, True
    value, found = get_nested_field_value(nested_row[STAGING_FIELD], column)
    original, found = get_nested_field_value(nested_row, f'{STAGING_FIELD}.{INPUT_FIELD}')
    if found:
        value, found = get_nested_field_value(original, column)
        if found:
            return value, True
    value, found = get_nested_field_value(nested_row, column)
    if found:
        return value, True
    return None, False


def push_fields(
    row: OrderedDict,
    list_config: list[PushConfig],
):
    nested_row = nest(row)
    for config in list_config:
        target_value, found = search_column_value_from_nested(nested_row, config.target)
        if found:
            array = target_value
        else:
            array = []
            #set_field_value(nested_row, f'{STAGING_FIELD}.{config.target}', array)
            set_row_staging_value(nested_row, config.target, array)
        source_value, found = search_column_value_from_nested(nested_row, config.source)
        if config.condition is None:
            array.append(source_value)
            continue
        condition_value, found = search_column_value_from_nested(nested_row, config.condition)
        if condition_value:
            array.append(source_value)
    return flatten_row(nested_row)

def assign_length(
    row: OrderedDict,
    dict_fields: OrderedDict,
):
    new_row = OrderedDict(row)
    for key, field in dict_fields.items():
        value, found = search_column_value(row, field)
        if found:
            new_row[f'{STAGING_FIELD}.{key}'] = len(value)
    return new_row

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
):
    ic.enable()
    ic()
    ic(input_files)
    df_list = []
    row_list_filtered_out = []
    global_status = GlobalStatus()
    config = setup_config(config_path)
    ic(config)
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
            orig_row = prepare_row(flat_row)
            row = prepare_row(flat_row)
            if STAGING_FIELD not in orig_row.nested:
                set_row_staging_value(row, FILE_FIELD, input_file)
                set_row_staging_value(row, INPUT_FIELD, orig_row.nested)
            if config.process.assign_array:
                row.flat= assign_array(row.flat, config.process.assign_array)
            if config.process.push:
                row.flat = push_fields(row.flat, config.process.push)
            if config.process.assign_length:
                row.flat = assign_length(row.flat, config.process.assign_length)
            if config.actions:
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
            if config.pick:
                remap_columns(row, config.pick)
            if not output_debug:
                pop_row_staging(row)
            new_flat_rows.append(row.flat)
        new_df = pd.DataFrame(new_flat_rows)
        df_list.append(new_df)
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
