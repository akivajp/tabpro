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
from logzero import logger
import numpy as np
import pandas as pd

# local

from . config import (
    AssignArrayConfig,
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
from . functions.get_nested_field_value import get_nested_field_value
from . functions.get_nested_field_value import get_nested_field_value
from . functions.search_column_value import search_column_value

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

from . io import (
    get_loader,
    #get_saver,
    get_writer,
    #load,
    #save,
)

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
    #skip_header: bool = False,
    no_header: bool = False,
):
    ic.enable()
    ic()
    ic(input_files)
    #df_list = []
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
        #saver = get_saver(output_file)
        writer = get_writer(output_file)
    ic(config)
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        base_name = os.path.basename(input_file)
        loader = get_loader(input_file, no_header=no_header)
        logger.info('# rows: %s', len(loader))
        for index, row in enumerate(loader):
            file_row_index = f'{input_file}:{index}'
            if file_row_index in set_ignore_file_rows:
                continue
            short_file_row_index = f'{base_name}:{index}'
            if short_file_row_index in set_ignore_file_rows:
                continue
            orig_row = row.clone()
            if STAGING_FIELD not in row:
                row[FILE_FIELD] = input_file
                row[FILE_ROW_INDEX_FIELD] = file_row_index
                row[ROW_INDEX_FIELD] = index
                row[INPUT_FIELD] = orig_row.nested
            if config.process.assign_array:
                row.flat= assign_array(row.flat, config.process.assign_array)
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
                        #ic(flat_row)
                        ic(row.flat)
                    raise e
            if config.pick:
                remap_columns(row, config.pick)
            if not output_debug:
                pop_row_staging(row)
            writer.push_row(row)
    if writer:
        writer.close()
    #else:
    #    ic(all_df)
    if row_list_filtered_out:
        df_filtered_out = pd.DataFrame(row_list_filtered_out)
        ic('Saving filtered out to: ', output_file_filtered_out)
        writer(df_filtered_out, output_file_filtered_out)
