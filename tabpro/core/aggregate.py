# -*- coding: utf-8 -*-

from typing import (
    Any,
)

import json
import os
import sys
import unicodedata

from collections import OrderedDict
from typing import Mapping

# 3-rd party modules

from . progress import Progress

# local

from . io import (
    get_loader,
)

from . console.views import (
    Panel,
)

from rich.table import Table

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

def normalize_column_name(name: Any) -> str:
    '''
    綴り揺れの検出に用いる列名の正規化キーを返す。

    全角・半角、大文字・小文字、前後の空白の違いを吸収する。
    編集距離のような曖昧一致は行わない (誤検出が確認コストになるため)。

    Args:
        name: 正規化する列名。

    Returns:
        正規化されたキー文字列。
    '''
    return unicodedata.normalize('NFKC', str(name)).strip().lower()

def compare_file_columns(
    dict_file_columns: Mapping[str, list[str]],
) -> OrderedDict:
    '''
    ファイルごとの列構成を突き合わせ、欠落・余剰・綴り揺れを検出する。

    半数以上のファイルに存在する列を「基準」とみなし、
    基準にあるのに無い列を欠落、基準に無い列を余剰として報告する。

    Args:
        dict_file_columns: ファイルパスから、その列名リストへの対応。

    Returns:
        比較結果 (files, columns, matrix, majority_columns,
        missing, extra, name_variants) を格納した辞書。
    '''
    files = list(dict_file_columns.keys())
    num_files = len(files)
    # NOTE: 列の並びは最初に現れた順を保つ (辞書順に並べ替えると元の形が分からなくなる)
    all_columns: list[str] = []
    for columns in dict_file_columns.values():
        for column in columns:
            if column not in all_columns:
                all_columns.append(column)
    counts = OrderedDict(
        (column, sum(1 for c in dict_file_columns.values() if column in c))
        for column in all_columns
    )
    # NOTE:
    #   「半数以上」とし、ちょうど半分も基準に含める。
    #   過半数 (> 半分) にすると、2ファイルの比較や空ファイルが混ざった場合に
    #   基準が1つも成立せず、片方の列が丸ごと「余剰」として報告されてしまう。
    #   欠落として挙げるほうが、差し戻しの判断材料として有用。
    expected_columns = [
        column for column, count in counts.items()
        if count * 2 >= num_files
    ]
    missing: OrderedDict[str, list[str]] = OrderedDict()
    extra: OrderedDict[str, list[str]] = OrderedDict()
    for file_path, columns in dict_file_columns.items():
        lacking = [c for c in expected_columns if c not in columns]
        surplus = [c for c in columns if c not in expected_columns]
        if lacking:
            missing[file_path] = lacking
        if surplus:
            extra[file_path] = surplus
    # NOTE: 正規化すると衝突する列名同士を綴り揺れの候補とする
    dict_normalized: OrderedDict[str, list[str]] = OrderedDict()
    for column in all_columns:
        dict_normalized.setdefault(normalize_column_name(column), []).append(column)
    name_variants = [
        variants for variants in dict_normalized.values() if len(variants) > 1
    ]
    result = OrderedDict()
    result['files'] = files
    result['columns'] = all_columns
    result['file_counts'] = counts
    result['expected_columns'] = expected_columns
    result['matrix'] = OrderedDict(
        (file_path, OrderedDict(
            (column, column in columns) for column in all_columns
        ))
        for file_path, columns in dict_file_columns.items()
    )
    result['missing'] = missing
    result['extra'] = extra
    result['name_variants'] = name_variants
    return result

# NOTE:
#   ファイル数・列数が多いと行列表示は読めなくなるため、
#   この範囲に収まるときだけ行列を出し、それ以外は集計表に切り替える。
#   いずれの場合も、完全な行列は JSON レポートに含まれる。
MATRIX_MAX_FILES = 25
MATRIX_MAX_COLUMNS = 12

def print_column_comparison(
    console,
    comparison: Mapping,
) -> None:
    '''
    列構成の比較結果を端末に表示する。

    Args:
        console: 出力先の rich Console。
        comparison: compare_file_columns() の戻り値。
    '''
    files = comparison['files']
    columns = comparison['columns']
    num_files = len(files)
    if len(files) <= MATRIX_MAX_FILES and len(columns) <= MATRIX_MAX_COLUMNS:
        table = Table(title='columns per file', title_justify='left')
        table.add_column('file', overflow='fold')
        for column in columns:
            table.add_column(str(column), justify='center')
        for file_path in files:
            present = comparison['matrix'][file_path]
            cells = [
                '[green]o[/green]' if present[c] else '[red]-[/red]'
                for c in columns
            ]
            table.add_row(os.path.basename(file_path), *cells)
        console.print(table)
    else:
        table = Table(title='column occurrence', title_justify='left')
        table.add_column('column', overflow='fold')
        table.add_column('files', justify='right')
        table.add_column('status')
        for column, count in comparison['file_counts'].items():
            if count == num_files:
                status = '[green]all[/green]'
            elif column in comparison['expected_columns']:
                status = f'[yellow]missing in {num_files - count}[/yellow]'
            else:
                status = f'[red]only in {count}[/red]'
            table.add_row(str(column), f'{count}/{num_files}', status)
        console.print(table)
    if comparison['missing'] or comparison['extra']:
        table = Table(title='files that differ from the expected columns', title_justify='left')
        table.add_column('file', overflow='fold')
        table.add_column('missing', overflow='fold')
        table.add_column('extra', overflow='fold')
        for file_path in files:
            lacking = comparison['missing'].get(file_path, [])
            surplus = comparison['extra'].get(file_path, [])
            if not lacking and not surplus:
                continue
            table.add_row(
                os.path.basename(file_path),
                '[red]' + ', '.join(map(str, lacking)) + '[/red]' if lacking else '',
                '[yellow]' + ', '.join(map(str, surplus)) + '[/yellow]' if surplus else '',
            )
        console.print(table)
    if comparison['name_variants']:
        table = Table(
            title='column names that differ only in case, width or spacing',
            title_justify='left',
        )
        table.add_column('variants', overflow='fold')
        for variants in comparison['name_variants']:
            table.add_row('[yellow]' + ' / '.join(map(str, variants)) + '[/yellow]')
        console.print(table)

def aggregate(
    input_files: list[str],
    output_file: str | None = None,
    verbose: bool = False,
    list_keys_to_show_duplicates: list[str] | None = None,
    show_count_threshold: int = 50,
    list_keys_to_show_all_count: list[str] | None = None,
    list_keys_to_expand: list[str] | None = None,
    show_count_max_length: int = 100,
    compare_columns: bool = False,
    sheet: str | None = None,
    all_sheets: bool = False,
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
    # NOTE: ファイルごとの列構成 (--compare-columns 指定時のみ使用)
    dict_file_columns: OrderedDict[str, list[str]] = OrderedDict()
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
            sheet=sheet,
            all_sheets=all_sheets,
        )
        if compare_columns:
            # NOTE: 行が1件も無いファイルも比較対象に含める
            dict_file_columns.setdefault(input_file, [])
        for index, row in enumerate(loader):
            for key, value in row.items():
                aggregate_one(
                    aggregated,
                    dict_counters,
                    key,
                    value,
                    list_keys_to_expand,
                )
            if compare_columns:
                # NOTE:
                #   行によって列が欠けることがあるため、
                #   ファイル内の全行にわたって列名の和を取る。
                columns = dict_file_columns[input_file]
                for key in row.keys():
                    if key not in columns:
                        columns.append(key)
            num_input_rows += 1
    for key, aggregation in aggregated.items():
        counter = dict_counters[key]
        if len(counter) > 0:
            aggregation['num_variations'] = len(counter)
            aggregation['max_count'] = counter.max_count
            aggregation['min_count'] = sorted(counter.main_counter.values())[0]
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
    # NOTE:
    #   既定では従来どおりの出力形式を保つため、
    #   --compare-columns を指定したときだけ項目を追加する。
    if compare_columns:
        comparison = compare_file_columns(dict_file_columns)
        print_column_comparison(console, comparison)
        dict_output['column_comparison'] = comparison
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
