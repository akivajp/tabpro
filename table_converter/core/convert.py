# -*- coding: utf-8 -*-

import json
import os

from collections import OrderedDict

import pandas as pd
from icecream import ic

map_loaders: dict[str, callable] = {}
def register_loader(
    ext: str,
):
    def decorator(loader):
        map_loaders[ext] = loader
        return loader
    return decorator

map_savers: dict[str, callable] = {}
def register_saver(
    ext: str,
):
    def decorator(saver):
        map_savers[ext] = saver
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
    default: any = None,
    raise_error: bool = True,
):
    if field in data:
        return data[field]
    if '.' in field:
        field, rest = field.split('.', 1)
        if field in data:
            return get_field_value(data[field], rest)
    if raise_error:
        raise KeyError(f'Field not found: {field}, existing fields: {data.keys()}')
    return default

def remap(
    row: OrderedDict,
    column_map: OrderedDict,
):
    new_row = OrderedDict()
    mapped_set = set()
    for column in column_map.keys():
        set_field_value(new_row, column, row[column_map[column]])
        mapped_set.add(column_map[column])
    rest = OrderedDict()
    for column in row.keys():
        if column.startswith('__') and column.endswith('__'):
            # NOTE: Ignore debug fields
            set_field_value(new_row, column, row[column])
        elif column not in mapped_set:
            rest[column] = row[column]
    set_field_value(new_row, '__debug__.__rest__', rest)
    return new_row

def apply_fields_split_by_newline(
    row: OrderedDict,
    fields: list[str],
):
    #new_row = OrderedDict(row)
    new_row = row
    for field in fields:
        value = get_field_value(row, field)
        if isinstance(value, str):
            new_value = value.split('\n')
            set_field_value(new_row, field, new_value)
    return new_row

def create_id_stat_node():
    return {
        'max_id': 0,
        'map_value_to_id': {},
        'map_id_to_node': {},
    }

def assign_id_in_node(
    row: OrderedDict,
    field: str,
    id_stat_node: dict,
):
    new_row = row
    value = get_field_value(row, field)
    if value not in id_stat_node['map_value_to_id']:
        field_id = id_stat_node['max_id'] + 1
        id_stat_node['max_id'] = field_id
        id_stat_node['map_value_to_id'][value] = field_id
        node = create_id_stat_node()
        id_stat_node['map_id_to_node'][field_id] = node
    else:
        field_id = id_stat_node['map_value_to_id'][value]
        node = id_stat_node['map_id_to_node'][field_id]
    set_field_value(new_row, f'__debug__.__ids__.{field}', field_id)
    return node, new_row

def assign_id(
    row: OrderedDict,
    fields: list[str],
    root_id_stat_node: dict,
):
    new_row = row
    node = root_id_stat_node
    for field in fields:
        node, new_row = assign_id_in_node(new_row, field, node)
    return new_row

def convert(
    input_files: list[str],
    output_file: str | None = None,
    pickup_columns: str | None = None,
    fields_to_split_by_newline: str | None = None,
    fields_to_assign_id: str | None = None,
):
    ic()
    ic(input_files)
    df_list = []
    column_map: OrderedDict | None = None
    list_fields_to_split_by_newline = None
    list_fields_to_assign_id = None
    root_id_stat = create_id_stat_node()
    if pickup_columns:
        column_map = OrderedDict()
        fields = pickup_columns.split(',')
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                column_map[dst] = src
            else:
                column_map[field] = field
    if fields_to_split_by_newline:
        list_fields_to_split_by_newline = fields_to_split_by_newline.split(',')
    if fields_to_assign_id:
        list_fields_to_assign_id = fields_to_assign_id.split(',')
    if output_file:
        ext = os.path.splitext(output_file)[1]
        if ext not in map_savers:
            raise ValueError(f'Unsupported file type: {ext}')
        saver = map_savers[ext]
    for input_file in input_files:
        ic(input_file)
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        ext = os.path.splitext(input_file)[1]
        ic(ext)
        if ext not in map_loaders:
            raise ValueError(f'Unsupported file type: {ext}')
        df = map_loaders[ext](input_file)
        #ic(df)
        ic(len(df))
        ic(df.columns)
        ic(df.iloc[0])
        new_rows = []
        for index, row in df.iterrows():
            new_row = OrderedDict(row)
            set_field_value(new_row, '__debug__.__file__', input_file)
            if column_map:
                new_row = remap(new_row, column_map)
            if list_fields_to_split_by_newline:
                new_row = apply_fields_split_by_newline(new_row, list_fields_to_split_by_newline)
            if list_fields_to_assign_id:
                new_row = assign_id(new_row, list_fields_to_assign_id, root_id_stat)
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
