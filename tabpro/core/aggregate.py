# -*- coding: utf-8 -*-

from typing import (
    Any,
)

import json
import os
import sys

from collections import OrderedDict

# 3-rd party modules

from . progress import Progress

# local

from . io import (
    get_loader,
)

from . console.views import (
    Panel,
)

class ValueCounter:
    def __init__(self):
        self.main_counter = OrderedDict()
        self.type_counter = OrderedDict()
        self.num_count1 = 0
        self.max_count = 0

    def add_value(self, value: Any):
        if value not in self.main_counter:
            self.main_counter[value] = 0
        self.main_counter[value] += 1
        if self.main_counter[value] == 1:
            self.num_count1 += 1
        if self.main_counter[value] == 2:
            self.num_count1 -= 1
        if self.main_counter[value] > self.max_count:
            self.max_count = self.main_counter[value]

    def add_type(self, value: Any):
        if type(value) == str:
            str_type = 'string'
        elif type(value) == int:
            str_type = 'integer'
        elif type(value) == float:
            str_type = 'float'
        elif type(value) == bool:
            str_type = 'boolean'
        elif type(value) == list:
            str_type = 'array'
        elif type(value) == dict:
            str_type = 'object'
        elif value is None:
            str_type = 'null'
        else:
            raise ValueError(f'Unsupported type: {type(value)}')
        if str_type not in self.type_counter:
            self.type_counter[str_type] = 0
        self.type_counter[str_type] += 1

    def items(self):
        return self.main_counter.items()
    
    def __len__(self):
        return len(self.main_counter)

def get_sorted(
    counter: dict[str, Any],
    show_count_max_length: int,
    max_items: int | None = 100,
    reverse: bool = True,
    min_count: int = 0,
):
    dict_sorted = OrderedDict()
    if max_items == 0:
        return dict_sorted
    for key, value in sorted(
        counter.items(),
        key=lambda item: item[1],
        reverse=reverse,
    ):
        if value < min_count:
            if reverse:
                break
            continue
        show_key = key
        if isinstance(key, str):
            if len(key) > show_count_max_length:
                show_key = key[:show_count_max_length] + '...'
        dict_sorted[show_key] = value
        if max_items is not None:
            if len(dict_sorted) >= max_items:
                break
    return dict_sorted

def aggregate_one(
    aggregated: dict,
    dict_counters: dict[str, ValueCounter],
    key: str,
    value: Any,
    list_keys_to_expand: list[str],
):
    aggregation = aggregated.setdefault(key, {})
    if key not in dict_counters:
        dict_counters[key] = ValueCounter()
    counter = dict_counters[key]
    counter.add_type(value)
    if not isinstance(value, (list)):
        counter.add_value(value)
    if isinstance(value, (list)):
        for list_index, list_item in enumerate(value):
            if isinstance(list_item, list):
                continue
            if isinstance(list_item, dict):
                for dict_key, dict_value in list_item.items():
                    full_key = f'{key}[].{dict_key}'
                    aggregate_one(
                        aggregated,
                        dict_counters,
                        full_key,
                        dict_value,
                        list_keys_to_expand,
                    )
                    if key in list_keys_to_expand:
                        # NOTE: expand list item
                        full_key = f'{key}[{list_index}].{dict_key}'
                        aggregate_one(
                            aggregated,
                            dict_counters,
                            full_key,
                            dict_value,
                            list_keys_to_expand,
                        )
                continue
            counter.add_value(list_item)
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
    list_keys_to_expand: list[str] | None = None,
    show_count_max_length: int = 100,
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
    if list_keys_to_expand is None: 
        list_keys_to_expand = []
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
                    list_keys_to_expand,
                )
            num_input_rows += 1
    for key, aggregation in aggregated.items():
        counter = dict_counters[key]
        if len(counter) > 0:
            aggregation['num_variations'] = len(counter)
            aggregation['max_count'] = counter.max_count
            aggregation['type_count'] = get_sorted(
                counter.type_counter,
                show_count_max_length,
                reverse=True,
            )
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
                aggregation['count'] = get_sorted(
                    counter.main_counter,
                    show_count_max_length,
                )
            else:
                aggregation[f'count_top{top_n}'] = get_sorted(
                    counter.main_counter,
                    show_count_max_length,
                    max_items=top_n,
                    reverse=True,
                )
                #console.log('count1: ', counter.count1)
                if counter.max_count > 1:
                    if counter.num_count1 <= count1_threshold:
                        aggregation['count1'] = get_sorted(
                            counter.main_counter,
                            show_count_max_length,
                            max_items=counter.num_count1,
                            reverse=False,
                        )
                if key in list_keys_to_show_duplicates:
                    aggregation[f'count_duplicates'] = get_sorted(
                        counter.main_counter,
                        show_count_max_length,
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
    progress.stop()
