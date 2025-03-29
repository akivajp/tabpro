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

def aggregate_one(
    aggregated: dict,
    dict_counters: dict,
    key: str,
    value: str,
):
    aggregation = aggregated.setdefault(key, {})
    counter = dict_counters.setdefault(key, {})
    if not isinstance(value, (list)):
        counter[value] = counter.get(value, 0) + 1
    if isinstance(value, (list)):
        for list_item in value:
            if isinstance(list_item, list):
                continue
            if isinstance(list_item, dict):
                for dict_key, dict_value in list_item.items():
                    full_key = f'{key}.[].{dict_key}'
                    aggregate_one(
                        aggregated,
                        dict_counters,
                        full_key,
                        dict_value,
                    )
                continue
            counter[list_item] = counter.get(list_item, 0) + 1
    if hasattr(value, '__len__'):
        length = len(value)
        if length > aggregation.get('max_length', -1):
            aggregation['max_length'] = length
        if length < aggregation.get('min_length', 10 ** 10):
            aggregation['min_length'] = length

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
    if output_file:
        ext = os.path.splitext(output_file)[1]
        if ext not in ['.json']:
            raise ValueError(f'Unsupported output file extension: {ext}')
    aggregated = OrderedDict()
    dict_counters = OrderedDict()
    num_input_rows = 0
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
                aggregate_one(
                    aggregated,
                    dict_counters,
                    key,
                    value,
                )
            num_input_rows += 1
    for key, aggregation in aggregated.items():
        counter = dict_counters[key]
        if len(counter) > 0:
            aggregation['num_variations'] = len(counter)
            threashold = 50
            top_n  = 10
            if len(counter) <= threashold:
                aggregation['count'] = get_sorted(counter)
            else:
                aggregation[f'count_{top_n}'] = get_sorted(
                    counter,
                    max_items=top_n,
                    reverse=True,
                )
    console.log('Total input rows: ', num_input_rows)
    dict_output = OrderedDict()
    dict_output['num_rows'] = num_input_rows
    dict_output['aggregated'] = aggregated
    if output_file is None and sys.stdout.isatty():
        console.print(Panel(
            dict_output,
            title='Aggregation',
            title_align='left',
            border_style='cyan',
        ))
    else:
        json_output = json.dumps(
            dict_output,
            indent=4,
            ensure_ascii=False,
        )
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_output)
        else:
            # NOTE: output redirection
            print(json_output)
