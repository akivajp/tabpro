# -*- coding: utf-8 -*-

import json
import os
import sys

from collections import OrderedDict

# 3-rd party modules

from . progress import Progress

# local

from . types import (
    GlobalStatus,
)

from . io import (
    get_loader,
)

from . console.views import (
    Panel,
)

def get_sorted(
    counter: dict,
    max_items: int = 100,
    reverse: bool = True,
):
    dict_sorted = OrderedDict()
    for key, value in sorted(
        counter.items(),
        key=lambda item: item[1],
        reverse=reverse,
    ):
        dict_sorted[key] = value
        if len(dict_sorted) >= max_items:
            break
    return dict_sorted

def aggregate(
    input_files: list[str],
    output_file: str | None = None,
    verbose: bool = False,
):
    progress = Progress(
        redirect_stdout = False,
    )
    progress.start()
    console = progress.console
    console.log('input_files: ', input_files)
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
        loader = get_loader(
            input_file,
            progress=progress,
        )
        console.log('# rows: ', len(loader))
        for index, row in enumerate(loader):
            for key, value in row.items():
                aggregation = aggregated.setdefault(key, {})
                counter = dict_counters.setdefault(key, {})
                if not isinstance(value, (list)):
                    counter[value] = counter.get(value, 0) + 1
                if isinstance(value, (list)):
                    for item in value:
                        if isinstance(item, list):
                            continue
                        if isinstance(item, dict):
                            continue
                        counter[item] = counter.get(item, 0) + 1
                if hasattr(value, '__len__'):
                    length = len(value)
                    if length > aggregation.get('max_length', -1):
                        aggregation['max_length'] = length
                    if length < aggregation.get('min_length', 10 ** 10):
                        aggregation['min_length'] = length
            num_stacked_rows += 1
    for key, aggregation in aggregated.items():
        counter = dict_counters[key]
        if len(counter) > 0:
            aggregation['num_variations'] = len(counter)
            if len(counter) <= 50:
                aggregation['count'] = get_sorted(counter)
    console.log('Total input rows: ', num_stacked_rows)
    if output_file:
        pass
    if output_file is None and sys.stdout.isatty():
        console.print(Panel(
            aggregated,
            title='Aggregation',
            title_align='left',
            border_style='cyan',
        ))
    else:
        json_aggregated = json.dumps(
            aggregated,
            indent=4,
            ensure_ascii=False,
        )
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_aggregated)
        else:
            # NOTE: output redirection
            print(json_aggregated)
