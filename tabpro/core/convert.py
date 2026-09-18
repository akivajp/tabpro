# -*- coding: utf-8 -*-

import os
import sys

# 3-rd party modules


from . progress import Progress

# local

from .. logging import logger

from . config import (
    setup_config,
    setup_pick_with_args,
)
from . constants import (
    FILE_FIELD,
    ROW_INDEX_FIELD,
    FILE_ROW_INDEX_FIELD,
    INPUT_FIELD,
)

from .actions import (
    do_actions,
    remap_columns,
    setup_actions_with_args,
)

from .actions.types import (
    GlobalStatus,
)

from . io import (
    get_loader,
    get_writer,
)

from . console.views import Panel

from . classes.row import Row

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
    no_header: bool = False,
    sheet: str | None = None,
    all_sheets: bool = False,
):
    #console = Console()
    progress = Progress(
        #console = console,
        redirect_stdout = False,
    )
    progress.start()
    console = progress.console
    logger.info('input_files: %s', input_files)
    row_list_filtered_out = []
    set_ignore_file_rows = set()
    global_status = GlobalStatus()
    config = setup_config(config_path)
    #console.log('config: ', config)
    if ignore_file_rows:
        set_ignore_file_rows = set(ignore_file_rows)
    if list_pick_columns:
        setup_pick_with_args(config, list_pick_columns)
    if list_actions:
        setup_actions_with_args(
            config,
            list_actions,
            action_delimiter=action_delimiter,
        )
    writer = None
    if output_file:
        writer = get_writer(
            output_file,
            progress=progress,
        )
    num_stacked_rows = 0
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        base_name = os.path.basename(input_file)
        loader = get_loader(
            input_file,
            no_header=no_header,
            progress=progress,
            sheet=sheet,
            all_sheets=all_sheets,
        )
        for index, row in enumerate(loader):
            file_row_index = f'{input_file}:{index}'
            if file_row_index in set_ignore_file_rows:
                continue
            short_file_row_index = f'{base_name}:{index}'
            if short_file_row_index in set_ignore_file_rows:
                continue
            # NOTE:
            #   入力値の記録には、ローダーが付与した由来情報 (__sheet__ など) を
            #   含めない。clone() では staging ごと複製されてしまい、
            #   __input__ や __values__ にシート名が混入する。
            orig_row = Row()
            for column in row.keys():
                orig_row[column] = row[column]
            # NOTE:
            #   既に由来情報を持つ行は上書きしない (中間ファイルを何段経由しても
            #   最初の入力ファイルの何行目かを保つため)。
            #   判定には __file__ の有無を用いる。__staging__ 全体の有無で見ると、
            #   ローダーが付与した __sheet__ だけで由来情報が設定されなくなる。
            if FILE_FIELD not in row.staging:
                row.staging[FILE_FIELD] = input_file
                row.staging[FILE_ROW_INDEX_FIELD] = file_row_index
                row.staging[ROW_INDEX_FIELD] = index
                row.staging[INPUT_FIELD] = orig_row.nested
                if loader.extension in ['.csv', '.tsv', '.xls', '.xlsx'] and not no_header:
                    for key_index, (key, value) in enumerate(orig_row.flat.items()):
                        row.staging[f'{INPUT_FIELD}.__values__.{key_index}'] = value
            if config.actions:
                try:
                    new_row = do_actions(global_status, row, config.actions)
                    if new_row is None:
                        if not output_debug:
                            row.pop_staging()
                        if verbose:
                            console.log('filtered out: ', row.flat)
                        if output_file_filtered_out:
                            # NOTE:
                            #   以前は row.flat (単なる OrderedDict) を
                            #   追加していたため、書き出し時に Writer が
                            #   期待する Row の属性が無く AttributeError に
                            #   なっていた。Row のまま渡す。
                            row_list_filtered_out.append(row)
                        continue
                    row = new_row
                except Exception as e:
                    if verbose:
                        #console.log('error in row index: ', index)
                        logger.error('error in row index: ', index)
                    raise e
            if config.pick:
                row = remap_columns(row, config.pick)
            if writer is None:
                if sys.stdout.isatty():
                    if num_stacked_rows == 0:
                        console.print(
                            Panel(
                                row.nested,
                                title='First Row',
                            )
                        )
            if not output_debug:
                row.pop_staging()
            if writer:
                writer.push_row(row)
            else:
                pass
            num_stacked_rows += 1
    console.log('total processed input rows: ', num_stacked_rows)
    if writer:
        writer.close()
    if row_list_filtered_out:
        #df_filtered_out = pd.DataFrame(row_list_filtered_out)
        if output_file_filtered_out:
            console.log('saving filtered out to: ', output_file_filtered_out)
            writer = get_writer(
                output_file_filtered_out,
                progress=progress,
            )
            writer.push_rows(row_list_filtered_out)
            writer.close()
    progress.stop()
