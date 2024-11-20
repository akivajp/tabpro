# -*- coding: utf-8 -*-

import json
import os

from collections import OrderedDict

import pandas as pd
from icecream import ic

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
    df.to_json(
        output_file,
        orient='records',
        force_ascii=False,
        indent=2,
    )

@register_saver('.jsonl')
def save_jsonl(
    df: pd.DataFrame,
    output_file: str,
):
    df.to_json(
        output_file,
        orient='records',
        lines=True,
        force_ascii=False,
    )

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

def map_constants(
    row: OrderedDict,
    dict_constants: OrderedDict,
):
    new_row = OrderedDict(row)
    for column in dict_constants.keys():
        set_field_value(new_row, column, dict_constants[column])
    return new_row

def remap_columns(
    row: OrderedDict,
    dict_remap: OrderedDict,
):
    new_row = OrderedDict()
    for column in dict_remap.keys():
        if '__debug__' in row:
            value, found = get_field_value(row['__debug__'], dict_remap[column])
            if found:
                set_field_value(new_row, column, value)
        value, found = get_field_value(row['__debug__'], dict_remap[column])
        original, found = get_field_value(row, '__debug__.__original__')
        if found:
            value, found = get_field_value(original, dict_remap[column])
            if found:
                set_field_value(new_row, column, value)
                continue
        value, found = get_field_value(row, column)
        if found:
            set_field_value(new_row, column, value)
            continue
    for column in row.keys():
        if column == '__debug__':
            # NOTE: Ignore debug fields
            set_field_value(new_row, column, row[column])
    return new_row

def apply_fields_split_by_newline(
    row: OrderedDict,
    fields: list[str],
):
    new_row = OrderedDict(row)
    for field in fields:
        value, found = get_field_value(row, field)
        if isinstance(value, str):
            new_value = value.split('\n')
            set_field_value(new_row, field, new_value)
    return new_row

def create_id_stat_node():
    return {
        'max_id': 0,
        'dict_value_to_id': {},
        'dict_id_to_node': {},
    }

def assign_id_in_node(
    row: OrderedDict,
    field: str,
    id_stat_node: dict,
):
    value, found = get_field_value(row, field)
    if not found:
        return
        #raise KeyError(f'Field not found: {field}, existing fields: {row.keys()}')
    if value not in id_stat_node['dict_value_to_id']:
        field_id = id_stat_node['max_id'] + 1
        id_stat_node['max_id'] = field_id
        id_stat_node['dict_value_to_id'][value] = field_id
        node = create_id_stat_node()
        id_stat_node['dict_id_to_node'][field_id] = node
    else:
        field_id = id_stat_node['dict_value_to_id'][value]
        node = id_stat_node['dict_id_to_node'][field_id]
    set_field_value(row, f'__debug__.__ids__.{field}', field_id)
    return node

def assign_id(
    row: OrderedDict,
    fields: list[str],
    root_id_stat_node: dict,
):
    new_row = OrderedDict(row)
    node = root_id_stat_node
    for field in fields:
        node = assign_id_in_node(new_row, field, node)
    return new_row

def convert(
    input_files: list[str],
    output_file: str | None = None,
    assign_constants: str | None = None,
    pickup_columns: str | None = None,
    fields_to_split_by_newline: str | None = None,
    fields_to_assign_ids: str | None = None,
):
    ic()
    ic(input_files)
    df_list = []
    dict_constants: OrderedDict | None = None
    dict_columns: OrderedDict | None = None
    list_fields_to_split_by_newline = None
    list_fields_to_assign_id = None
    root_id_stat = create_id_stat_node()
    if assign_constants:
        dict_constants = OrderedDict()
        fields = assign_constants.split(',')
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                dict_constants[dst] = src
            else:
                raise ValueError(f'Invalid constant assignment: {field}')
    if pickup_columns:
        dict_columns = OrderedDict()
        fields = pickup_columns.split(',')
        for field in fields:
            if '=' in field:
                dst, value = field.split('=')
                dict_columns[dst] = value
            else:
                dict_columns[field] = field
    if fields_to_split_by_newline:
        list_fields_to_split_by_newline = fields_to_split_by_newline.split(',')
    if fields_to_assign_ids:
        list_fields_to_assign_id = fields_to_assign_ids.split(',')
    if output_file:
        ext = os.path.splitext(output_file)[1]
        if ext not in dict_savers:
            raise ValueError(f'Unsupported file type: {ext}')
        saver = dict_savers[ext]
    for input_file in input_files:
        ic(input_file)
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        ext = os.path.splitext(input_file)[1]
        ic(ext)
        if ext not in dict_loaders:
            raise ValueError(f'Unsupported file type: {ext}')
        df = dict_loaders[ext](input_file)
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
            if dict_constants:
                new_row = map_constants(new_row, dict_constants)
            if dict_columns:
                new_row = remap_columns(new_row, dict_columns)
            if list_fields_to_split_by_newline:
                new_row = apply_fields_split_by_newline(new_row, list_fields_to_split_by_newline)
            if list_fields_to_assign_id:
                new_row = assign_id(new_row, list_fields_to_assign_id, root_id_stat)
            if dict_columns:
                new_row = remap_columns(new_row, dict_columns)
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
