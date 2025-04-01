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

class ValueCounter:
    def __init__(self):
        self.counter = OrderedDict()
        self.count1 = 0
        self.max_count = 0

    def add(self, key: str):
        if key not in self.counter:
            self.counter[key] = 0
        self.counter[key] += 1
        if self.counter[key] == 1:
            self.count1 += 1
        if self.counter[key] == 2:
            self.count1 -= 1
        if self.counter[key] > self.max_count:
            self.max_count = self.counter[key]

    def items(self):
        return self.counter.items()
    
    def __len__(self):
        return len(self.counter)

def get_sorted(
    #counter: dict,
    counter: ValueCounter,
    max_items: int | None = 100,
    reverse: bool = True,
    min_count: int = 0,
):
    dict_sorted = OrderedDict()
    for key, value in sorted(
        counter.items(),
        key=lambda item: item[1],
        reverse=reverse,
    ):
        if value < min_count:
            if reverse:
                break
            continue
        dict_sorted[key] = value
        if max_items is not None:
            if len(dict_sorted) >= max_items:
                break
    return dict_sorted

def aggregate_one(
    aggregated: dict,
    dict_counters: dict[str, ValueCounter],
    key: str,
    value: str,
):
    aggregation = aggregated.setdefault(key, {})
    #counter = dict_counters.setdefault(key, {})
    if key not in dict_counters:
        dict_counters[key] = ValueCounter()
    counter = dict_counters[key]
    if not isinstance(value, (list)):
        counter.add(value)
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
            counter.add(list_item)
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
    list_keys_to_show_duplicates: list[str] | None = None,
    show_count_threshold: int = 50,
    list_keys_to_show_all_count: list[str] | None = None,
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
    if list_keys_to_show_duplicates is None:
        list_keys_to_show_duplicates = []
    if list_keys_to_show_all_count is None:
        list_keys_to_show_all_count = []
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
            aggregation['max_count'] = counter.max_count
            top_threshold = 50
            count1_threshold = 30
            top_n  = 10
            show_all = False
            if len(counter) <= top_threshold:
                show_all = True
            elif key in list_keys_to_show_all_count:
                if counter.max_count > 1:
                    # NOTE: show all only if max_count > 1
                    show_all = True
            if show_all:
                aggregation['count'] = get_sorted(counter)
            elif counter.max_count > 1:
                aggregation[f'count_top{top_n}'] = get_sorted(
                    counter,
                    max_items=top_n,
                    reverse=True,
                )
                #console.log('count1: ', counter.count1)
                if counter.count1 <= count1_threshold:
                    aggregation['count1'] = get_sorted(
                        counter,
                        max_items=counter.count1,
                        reverse=False,
                    )
                if key in list_keys_to_show_duplicates:
                    aggregation[f'count_duplicates'] = get_sorted(
                        counter,
                        max_items=None,
                        reverse=True,
                        min_count=2,
                    )
    console.log('total input rows: ', num_input_rows)
    dict_output = OrderedDict()
    dict_output['num_rows'] = num_input_rows
    dict_output['aggregated'] = aggregated
    if output_file is None and sys.stdout.isatty():
        console.print(Panel(
            dict_output,
            title='aggregation',
            title_align='left',
            border_style='cyan',
        ))
    else:
        console.log('writing output to: ', output_file)
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
