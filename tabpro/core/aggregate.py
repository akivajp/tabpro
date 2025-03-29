# -*- coding: utf-8 -*-

import os
import sys

from collections import OrderedDict

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

# 3-rd party modules

from icecream import ic
import pandas as pd

from . progress import Progress

# local

from . config import (
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

from . actions import (
    do_actions,
    pop_row_staging,
    remap_columns,
    setup_actions_with_args,
)

from . types import (
    GlobalStatus,
)

from . io import (
    get_loader,
    get_writer,
)

def aggregate(
    input_files: list[str],
    output_file: str | None = None,
    #output_file_filtered_out: str | None = None,
    #config_path: str | None = None,
    #output_debug: bool = False,
    #list_actions: list[str] | None = None,
    #list_pick_columns: list[str] | None = None,
    #action_delimiter: str = ':',
    verbose: bool = False,
    #ignore_file_rows: list[str] | None = None,
    #no_header: bool = False,
):
    #console = Console()
    progress = Progress(
        #console = console,
        redirect_stdout = False,
    )
    progress.start()
    #ic.enable()
    console = progress.console
    console.log('input_files: ', input_files)
    row_list_filtered_out = []
    set_ignore_file_rows = set()
    global_status = GlobalStatus()
    if output_file:
        ext = os.path.splitext(output_file)[1]
        if ext not in ['.json']:
            raise ValueError(f'Unsupported output file extension: {ext}')
    num_stacked_rows = 0
    aggregated = OrderedDict()
    dict_counters = OrderedDict()
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        base_name = os.path.basename(input_file)
        loader = get_loader(
            input_file,
            progress=progress,
        )
        console.log('# rows: ', len(loader))
        for index, row in enumerate(loader):
            file_row_index = f'{input_file}:{index}'
            if file_row_index in set_ignore_file_rows:
                continue
            short_file_row_index = f'{base_name}:{index}'
            if short_file_row_index in set_ignore_file_rows:
                continue
            orig_row = row.clone()
            if STAGING_FIELD not in row:
                row.staging[FILE_FIELD] = input_file
                row.staging[FILE_ROW_INDEX_FIELD] = file_row_index
                row.staging[ROW_INDEX_FIELD] = index
                row.staging[INPUT_FIELD] = orig_row.nested
                if loader.extension in ['.csv', '.xlsx']:
                    for key_index, (key, value) in enumerate(orig_row.flat.items()):
                        row.staging[f'{INPUT_FIELD}.__values__.{key_index}'] = value
            #if writer:
            #    writer.push_row(row)
            #else:
            #    pass
            num_stacked_rows += 1
    console.log('Total input rows: ', num_stacked_rows)
    #if writer:
    #    writer.close()
    #else:
    #    ic(all_df)
    #if row_list_filtered_out:
    #    df_filtered_out = pd.DataFrame(row_list_filtered_out)
    #    ic('Saving filtered out to: ', output_file_filtered_out)
    #    writer(df_filtered_out, output_file_filtered_out)
