# -*- coding: utf-8 -*-

import dataclasses
import json
import os

from collections import OrderedDict
from collections import defaultdict
from typing import Mapping

# 3-rd party modules

from icecream import ic
import numpy as np
import pandas as pd

# local

from . config import setup_config
from . config import AssignIdConfig

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

type ContextColumnTuple = tuple[str]
type ContextValueTuple = tuple 
type PrimaryColumnTuple = tuple[str]
type PrimaryValueTuple = tuple

@dataclasses.dataclass
class IdMap:
    max_id: int = 0
    dict_value_to_id: Mapping[PrimaryValueTuple, int] = \
        dataclasses.field(default_factory=defaultdict)
    dict_id_to_value: Mapping[int, PrimaryValueTuple] = \
        dataclasses.field(default_factory=defaultdict)

type IdContextMap = Mapping[
    (
        ContextColumnTuple,
        ContextValueTuple,
        PrimaryColumnTuple,
    ),
    IdMap
]

def create_id_context_map() -> IdContextMap:
    return defaultdict(IdMap)

def assign_id(
    row: OrderedDict,
    dict_assignment: Mapping[str, AssignIdConfig],
    id_context_map: IdContextMap,
):
    new_row = OrderedDict(row)
    for column, config in dict_assignment.items():
        context_columns = []
        context_values = []
        if config.context:
            for context_column in config.context:
                value, found = search_column_value(new_row, context_column)
                if not found:
                    raise KeyError(f'Column not found: {context_column}, existing columns: {new_row.keys()}')
                context_columns.append(context_column)
                context_values.append(value)
        primary_columns = []
        primary_values = []
        for primary_column in config.primary:
            value, found = search_column_value(new_row, primary_column)
            if not found:
                raise KeyError(f'Column not found: {primary_column}, existing columns: {new_row.keys()}')
            primary_columns.append(primary_column)
            primary_values.append(value)
        context_key = (
            tuple(context_columns),
            tuple(context_values),
            tuple(primary_columns),
        )
        primary_value = tuple(primary_values)
        id_map = id_context_map[context_key]
        if primary_value not in id_map.dict_value_to_id:
            field_id = id_map.max_id + 1
            id_map.max_id = field_id
            id_map.dict_value_to_id[primary_value] = field_id
            id_map.dict_id_to_value[field_id] = primary_value
        else:
            field_id = id_map.dict_value_to_id[primary_value]
        set_field_value(new_row, f'__debug__.{column}', field_id)
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
    id_context_map = create_id_context_map()
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
        #dict_assign_ids = OrderedDict()
        fields = fields_to_assign_ids.split(',')
        context = []
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                config.process.assign_ids[dst] = AssignIdConfig(
                    primary = [src],
                    context = context,
                )
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
            if config.process.assign_ids:
                new_row = assign_id(new_row, config.process.assign_ids, id_context_map)
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
