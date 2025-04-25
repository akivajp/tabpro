# -*- coding: utf-8 -*-

import os

from collections import OrderedDict

from typing import (
    Any,
)

# 3-rd party modules

from icecream import ic

# local

from .. logging import logger

from . constants import (
    FILE_FIELD,
    ROW_INDEX_FIELD,
    FILE_ROW_INDEX_FIELD,
)

from . functions.search_column_value import search_column_value

from . io import (
    get_loader,
    get_writer,
    save,
)

from . classes.row import Row

from . console.views import (
    Panel,
)

from . progress import (
    Progress,
)

from .functions.get_primary_key import get_primary_key

def set_staging_values(
    row: Row,
    file_path: str,
    file_row_index: int,
):
    str_file_row_index = f'{file_path}:{file_row_index}'
    for key, value in [
        [FILE_FIELD, file_path],
        [FILE_ROW_INDEX_FIELD, str_file_row_index],
        [ROW_INDEX_FIELD, file_row_index],
    ]:
        if key not in row.staging:
            row.staging[key] = value

def merge(
    previous_files: list[str],
    modification_files: list[str],
    keys: list[str],
    allow_duplicate_conventional_keys: bool = False,
    allow_duplicate_modification_keys: bool = False,
    ignore_not_found: bool = False,
    output_base_data_file: str | None = None,
    output_modified_data_file: str | None = None,
    output_remaining_data_file: str | None = None,
    merge_fields: list[str] | None = None,
    merge_staging: bool = False,
    use_staging: bool = False,
):
    progress = Progress(
        #redirect_stdout = False,
        #transient=True,
    )
    progress.start()
    console = progress.console
    console.log('previous files: ', previous_files)
    console.log('modification files: ', modification_files)
    console.log('keys: ', keys)
    dict_key_to_row: dict[Any, Row] = OrderedDict()
    set_modified_keys = set()
    all_base_rows = []
    all_modified_rows = []
    list_ignored_keys = []
    num_modified = 0
    if use_staging:
        merge_staging = True
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
        loader = get_loader(
            previous_file,
            progress=progress,
        )
        console.log('# rows: ', len(loader))
        #for index, row in enumerate(tqdm(
        #    loader,
        #    desc=f'Loading: {previous_file}',
        #    total=len(loader),
        #)):
        for index, row in enumerate(progress.track(
            loader,
            description=f'prcessing ...',
        )):
            set_staging_values(
                row,
                previous_file,
                index,
            )
            primary_key = get_primary_key(row, keys)
            if not allow_duplicate_conventional_keys:
                if primary_key in dict_key_to_row:
                    ic(index)
                    raise ValueError(f'Duplicate key: {primary_key}')
            dict_key_to_row[primary_key] = row
            all_base_rows.append(row)
    for modification_file in modification_files:
        if not os.path.exists(modification_file):
            raise FileNotFoundError(f'File not found: {modification_file}')
        loader = get_loader(
            modification_file,
            progress=progress,
        )
        console.log('# rows: ', len(loader))
        for index, row in enumerate(progress.track(
            loader,
            description=f'processing ...',
        )):
            set_staging_values(
                row,
                modification_file,
                index,
            )
            primary_key = get_primary_key(row, keys)
            if primary_key not in dict_key_to_row:
                if ignore_not_found:
                    logger.debug('primary_key not found in previous files: %s', primary_key)
                    list_ignored_keys.append(primary_key)
                    continue
                logger.error('index: %s', index)
                raise ValueError(f'key not found: {primary_key}')
            target_row = dict_key_to_row[primary_key]
            if primary_key in set_modified_keys:
                if not allow_duplicate_modification_keys:
                    logger.error('index: %s', index)
                    raise ValueError(f'duplicate key: {primary_key}')
                #console.log('skipped duplicate key: ', primary_key)
            else:
                all_modified_rows.append(target_row)
            if merge_fields is None:
                merge_fields = []
                for field in row.flat.keys():
                    if field.startswith('__staging__.'):
                        continue
                    merge_fields.append(field)
            if merge_staging:
                if '__staging__' not in merge_fields:
                    merge_fields.append('__staging__')
            #logger.debug('merge fields: %s', merge_fields)
            for field in merge_fields:
                value, found = search_column_value(row.nested, field)
                #logger.debug('field: %s', field)
                #logger.debug('found: %s', found)
                #logger.debug('value: %s', value)
                if found:
                    target_row[field] = value
            set_modified_keys.add(primary_key)
            num_modified += 1
    console.log('# modifications: ', num_modified)
    console.log('# modified rows: ', len(all_modified_rows))
    if ignore_not_found:
        logger.debug('# ignored keys: %s', len(list_ignored_keys))
    if output_base_data_file:
        #ic('Saving to: ', output_base_data_file)
        save(
            all_base_rows,
            output_base_data_file,
            progress=progress,
        )
    if output_modified_data_file:
        save(
            all_modified_rows,
            output_modified_data_file,
            progress=progress,
        )
    if output_remaining_data_file:
        remaining_rows = []
        for key, row in dict_key_to_row.items():
            if key not in set_modified_keys:
                remaining_rows.append(row)
        #ic(len(remaining_rows))
        #ic('Saving to: ', output_remaining_data_file)
        console.log('# remaining rows: ', len(remaining_rows))
        save(
            remaining_rows,
            output_remaining_data_file,
            progress=progress,
        )
    progress.stop()
