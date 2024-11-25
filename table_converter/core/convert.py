# -*- coding: utf-8 -*-

import json
import os

from collections import OrderedDict

# 3-rd party modules

from icecream import ic
import numpy as np
import pandas as pd

# local

from . config import setup_config

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

@register_loader('.xlsx')
def load_excel(
    input_file: str,
):
    df = pd.read_excel(input_file)
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
    with open(output_file, 'w') as f:
        json.dump(
            data,
            f,
            indent=2,
            ensure_ascii=False,
        )

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

def set_field_value(
    data: OrderedDict,
    field: str,
    value: any,
):
    if '.' in field:
        field, rest = field.split('.', 1)
        if field not in data:
            data[field] = OrderedDict()
        set_field_value(data[field], rest, value)
    else:
        data[field] = value

def get_field_value(
    data: OrderedDict,
    field: str,
):
    if field in data:
        return data[field], True
    if '.' in field:
        field, rest = field.split('.', 1)
        if field in data:
            return get_field_value(data[field], rest)
    return None, False

def search_column_value(
    row: OrderedDict,
    column: str,
):
    if '__debug__' in row:
        value, found = get_field_value(row['__debug__'], column)
        if found:
            return value, True
    value, found = get_field_value(row['__debug__'], column)
    original, found = get_field_value(row, '__debug__.__original__')
    if found:
        value, found = get_field_value(original, column)
        if found:
            return value, True
    value, found = get_field_value(row, column)
    if found:
        set_field_value(row, column, value)
        return value, True
    return None, False

def map_constants(
    row: OrderedDict,
    dict_constants: OrderedDict,
):
    new_row = OrderedDict(row)
    for column in dict_constants.keys():
        #set_field_value(new_row, column, dict_constants[column])
        set_field_value(new_row, f'__debug__.{column}', dict_constants[column])
    return new_row

def map_formats(
    row: OrderedDict,
    dict_formats: OrderedDict,
):
    new_row = OrderedDict(row)
    for column in dict_formats.keys():
        template = dict_formats[column]
        params = {}
        params.update(row['__debug__'])
        params.update(row)
        formatted = None
        while formatted is None:
            try:
                formatted = template.format(**params)
            except KeyError as e:
                #ic(e)
                #ic(e.args)
                #ic(e.args[0])
                key = e.args[0]
                params[key] = f'__{key}__undefined__'
            except:
                raise
        set_field_value(new_row, f'__debug__.{column}', formatted)
    return new_row

def remap_columns(
    row: OrderedDict,
    dict_remap: OrderedDict,
):
    new_row = OrderedDict()
    for column in dict_remap.keys():
        value, found = search_column_value(row, dict_remap[column])
        if found:
            set_field_value(new_row, column, value)
    for column in row.keys():
        if column == '__debug__':
            # NOTE: Ignore debug fields
            set_field_value(new_row, column, row[column])
    return new_row

def apply_fields_split_by_newline(
    row: OrderedDict,
    dict_fields: OrderedDict,
):
    new_row = OrderedDict(row)
    for column in dict_fields:
        value, found = search_column_value(row, dict_fields[column])
        #ic(value, found)
        if found:
            if isinstance(value, str):
                new_value = value.split('\n')
                set_field_value(new_row, f'__debug__.{column}', new_value)
            else:
                set_field_value(new_row, f'__debug__.{column}', value)
    return new_row

def create_id_stat_node():
    return {
        'max_id': 0,
        'dict_value_to_id': {},
        'dict_id_to_node': {},
    }

def assign_id_in_node(
    row: OrderedDict,
    column: str,
    dict_assignment: OrderedDict,
    id_stat_node: dict,
):
    value, found = search_column_value(row, dict_assignment[column])
    if not found:
        raise KeyError(f'Column not found: {column}, existing columns: {row.keys()}')
    if value not in id_stat_node['dict_value_to_id']:
        field_id = id_stat_node['max_id'] + 1
        id_stat_node['max_id'] = field_id
        id_stat_node['dict_value_to_id'][value] = field_id
        node = create_id_stat_node()
        id_stat_node['dict_id_to_node'][field_id] = node
    else:
        field_id = id_stat_node['dict_value_to_id'][value]
        node = id_stat_node['dict_id_to_node'][field_id]
    set_field_value(row, f'__debug__.{column}', field_id)
    set_field_value(row, f'__debug__.__ids__.{column}', field_id)
    return node

def assign_id(
    row: OrderedDict,
    dict_assignment: OrderedDict,
    root_id_stat_node: dict,
):
    new_row = OrderedDict(row)
    node = root_id_stat_node
    for column in dict_assignment:
        node = assign_id_in_node(new_row, column, dict_assignment, node)
    return new_row

def convert(
    input_files: list[str],
    output_file: str | None = None,
    config_path: str | None = None,
    assign_constants: str | None = None,
    assign_formats: str | None = None,
    pickup_columns: str | None = None,
    fields_to_split_by_newline: str | None = None,
    fields_to_assign_ids: str | None = None,
    output_debug: bool = False,
):
    ic.enable()
    ic()
    ic(input_files)
    df_list = []
    dict_assign_ids= None
    root_id_stat = create_id_stat_node()
    config = setup_config(config_path)
    ic(config)
    if assign_constants:
        fields = assign_constants.split(',')
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                config.process.assign_constants[dst] = src
            else:
                raise ValueError(f'Invalid constant assignment: {field}')
    if assign_formats:
        fields = assign_formats.split(',')
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                config.process.assign_formats[dst] = src
            else:
                raise ValueError(f'Invalid template assignment: {field}')
    if pickup_columns:
        fields = pickup_columns.split(',')
        for field in fields:
            if '=' in field:
                dst, value = field.split('=')
                config.map[dst] = value
            else:
                config.map[field] = field
    if fields_to_split_by_newline:
        fields = fields_to_split_by_newline.split(',')
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                config.process.split_by_newline[dst] = src
            else:
                raise ValueError(f'Invalid split by newline: {field}')
    if fields_to_assign_ids:
        dict_assign_ids = OrderedDict()
        fields = fields_to_assign_ids.split(',')
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                dict_assign_ids[dst] = src
            else:
                raise ValueError(f'Invalid id assignment: {field}')
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
        ic(len(df))
        ic(df.columns)
        ic(df.iloc[0])
        new_rows = []
        for index, row in df.iterrows():
            orig = OrderedDict(row)
            new_row = OrderedDict(row)
            set_field_value(new_row, '__debug__.__original__', orig)
            set_field_value(new_row, '__debug__.__file__', input_file)
            if config.process.assign_constants:
                new_row = map_constants(new_row, config.process.assign_constants)
            if config.map:
                new_row = remap_columns(new_row, config.map)
            if config.process.split_by_newline:
                new_row = apply_fields_split_by_newline(new_row, config.process.split_by_newline)
            if dict_assign_ids:
                new_row = assign_id(new_row, dict_assign_ids, root_id_stat)
            if config.process.assign_formats:
                new_row = map_formats(new_row, config.process.assign_formats)
            if config.map:
                new_row = remap_columns(new_row, config.map)
            if not output_debug:
                new_row.pop('__debug__', None)
            new_rows.append(new_row)
        new_df = pd.DataFrame(new_rows)
        df_list.append(new_df)
    all_df = pd.concat(df_list)
    #ic(all_df)
    ic(len(all_df))
    ic(all_df.columns)
    ic(all_df.iloc[0])
    if output_file:
        ic('Saing to: ', output_file)
        saver(all_df, output_file)
