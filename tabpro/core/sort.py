# -*- coding: utf-8 -*-

import json
import os
import sys

from collections import OrderedDict

from typing import (
    Any,
)

# 3-rd party modules

from . progress import Progress

# local

from .classes.row import Row

from . io import (
    check_writer,
    get_loader,
    get_writer,
)

from . console.views import (
    Panel,
)

from .functions.get_primary_key import get_primary_key

def sort(
    sort_keys: list[str] | str,
    input_files: list[str],
    output_file: str | None = None,
    reverse: bool = False,
    verbose: bool = False,
):
    progress = Progress(
        redirect_stdout = False,
    )
    progress.start()
    console = progress.console
    console.log('input_files: ', input_files)
    if output_file:
        check_writer(output_file)
    all_input_row_items: list[tuple[Any, Row]] = []
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        loader = get_loader(
            input_file,
            progress=progress,
        )
        console.log('# rows: ', len(loader))
        for index, row in enumerate(loader):
            primary_key = get_primary_key(row, sort_keys)
            all_input_row_items.append((primary_key, row))
    console.log('# input rows: ', len(all_input_row_items))
    console.log('sorting rows...')
    all_input_row_items.sort(
        key=lambda x: x[0],
        reverse=reverse,
    )
    if output_file is None and sys.stdout.isatty():
        console.print(Panel(
            all_input_row_items[0][1],
            title='first row',
            title_align='left',
            border_style='cyan',
        ))
    elif output_file:
        writer = get_writer(
            output_file,
            progress=progress,
        )
        for key, row in all_input_row_items:
            writer.push_row(row)
        writer.close()
    progress.stop()
