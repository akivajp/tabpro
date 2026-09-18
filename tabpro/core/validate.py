# -*- coding: utf-8 -*-
'''
入力データが期待する仕様を満たしているかを検査する。

convert が「変換」を担うのに対し、本モジュールは「検査」のみを担う。
両者を分けているのは、変換は失敗した値を既定値などで埋めて先に進めるのに対し、
検査は満たさないことを事実として報告しなければならないため。
'''

import dataclasses
import os
import re
import sys

from collections import OrderedDict
from typing import (
    Any,
    Callable,
    Mapping,
    TypeAlias,
)

import yaml

from rich.table import Table

from . classes.row import Row
from . constants import VIOLATIONS_FIELD

from . io import (
    check_writer,
    get_loader,
    get_writer,
)

from . progress import Progress

class SchemaError(ValueError):
    '''スキーマ定義そのものに誤りがある場合に送出される。'''

# 検査規則: (値, 規則の引数) を受け取り、満たしていれば True を返す
RuleChecker: TypeAlias = Callable[[Any, Any], bool]

dict_rules: OrderedDict = OrderedDict()

def register_rule(
    name: str,
) -> Callable[[RuleChecker], RuleChecker]:
    '''
    検査規則を名前で登録するデコレータ。

    actions と同じ登録方式を用いることで、規則の追加を1関数で行えるようにする。

    Args:
        name: スキーマ上で用いる規則名。

    Returns:
        関数を登録して返すデコレータ。
    '''
    def decorator(checker: RuleChecker) -> RuleChecker:
        dict_rules[name] = checker
        return checker
    return decorator

TYPE_NAMES = ['bool', 'float', 'int', 'str']

@register_rule('type')
def check_type(value: Any, expected: Any) -> bool:
    '''値が指定の型として解釈できるかを判定する。'''
    if expected == 'str':
        # NOTE: CSV 由来の値は全て文字列であり、str は常に満たす
        return True
    if expected == 'bool':
        if isinstance(value, bool):
            return True
        return str(value).strip().lower() in [
            'true', 'false', 'yes', 'no', 'on', 'off', '1', '0',
        ]
    if expected == 'int':
        # NOTE: bool は int の subclass だが、別の型として扱う
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        try:
            int(str(value).strip())
        except ValueError:
            return False
        return True
    if expected == 'float':
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        try:
            float(str(value).strip())
        except ValueError:
            return False
        return True
    raise SchemaError(
        f'Unsupported type: {expected}. Must be one of {TYPE_NAMES}.'
    )

@register_rule('enum')
def check_enum(value: Any, expected: Any) -> bool:
    '''値が許容値の一覧に含まれるかを判定する。'''
    if value in expected:
        return True
    # NOTE: CSV 由来の値は文字列なので、文字列としての一致も許す
    return str(value) in [str(item) for item in expected]

@register_rule('pattern')
def check_pattern(value: Any, expected: Any) -> bool:
    '''値が正規表現に一致するかを判定する。'''
    return re.search(str(expected), str(value)) is not None

@dataclasses.dataclass
class ColumnSchema:
    '''1つの列に対する検査仕様。'''
    name: str
    required: bool = False
    # NOTE: スキーマに書かれた順に検査するため、辞書ではなく並びで保持する
    checks: list[tuple[str, Any]] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class Schema:
    '''入力データ全体に対する検査仕様。'''
    columns: list[ColumnSchema] = dataclasses.field(default_factory=list)

def load_schema(
    schema_path: str,
) -> Schema:
    '''
    YAML で記述されたスキーマを読み込む。

    Args:
        schema_path: スキーマファイルのパス。

    Returns:
        読み込まれた Schema。

    Raises:
        SchemaError: スキーマの記述に誤りがある場合。
    '''
    if not os.path.exists(schema_path):
        raise SchemaError(f'Schema file not found: {schema_path}')
    if not schema_path.endswith(('.yaml', '.yml')):
        raise SchemaError(
            f'Only YAML schema files are supported: {schema_path}'
        )
    with open(schema_path, 'r', encoding='utf-8') as f:
        loaded = yaml.safe_load(f)
    if not isinstance(loaded, Mapping):
        raise SchemaError('Schema must be a mapping.')
    dict_columns = loaded.get('columns')
    if not isinstance(dict_columns, Mapping):
        raise SchemaError("Schema must have a 'columns' mapping.")
    schema = Schema()
    for name, definition in dict_columns.items():
        if definition is None:
            # NOTE: 規則を書かない場合は「列の存在だけを期待しない」任意項目とする
            definition = {}
        if not isinstance(definition, Mapping):
            raise SchemaError(
                f'Definition of column {name!r} must be a mapping.'
            )
        column = ColumnSchema(name=str(name))
        for key, param in definition.items():
            if key == 'required':
                if not isinstance(param, bool):
                    raise SchemaError(
                        f'required of column {name!r} must be a boolean.'
                    )
                column.required = param
                continue
            if key not in dict_rules:
                raise SchemaError(
                    f'Unsupported rule {key!r} for column {name!r}. '
                    f'Supported rules: {sorted(dict_rules)}'
                )
            if key == 'type' and param not in TYPE_NAMES:
                raise SchemaError(
                    f'Unsupported type {param!r} for column {name!r}. '
                    f'Must be one of {TYPE_NAMES}.'
                )
            if key == 'enum' and not isinstance(param, list):
                raise SchemaError(
                    f'enum of column {name!r} must be a list.'
                )
            column.checks.append((key, param))
        schema.columns.append(column)
    if not schema.columns:
        raise SchemaError('Schema defines no columns.')
    return schema

def is_empty_value(
    value: Any,
    found: Any,
) -> bool:
    '''値が未検出または空とみなせるかを判定する。'''
    if not found:
        return True
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False

def validate_row(
    row: Row,
    schema: Schema,
) -> list[OrderedDict]:
    '''
    1行を検査し、違反の一覧を返す。

    Args:
        row: 検査対象の行。
        schema: 検査仕様。

    Returns:
        違反を表す辞書のリスト。違反が無ければ空リスト。
    '''
    violations: list[OrderedDict] = []
    for column in schema.columns:
        value, found = row.search(column.name)
        if is_empty_value(value, found):
            if column.required:
                violations.append(OrderedDict([
                    ('column', column.name),
                    ('rule', 'required'),
                    ('expected', 'a non-empty value'),
                    ('actual', None if not found else value),
                ]))
            # NOTE:
            #   任意項目が空の場合、型や正規表現の検査は意味を持たないため行わない。
            #   必須項目が空の場合も、違反を1件に絞るため後続の検査は行わない。
            continue
        for rule_name, param in column.checks:
            checker = dict_rules[rule_name]
            if not checker(value, param):
                violations.append(OrderedDict([
                    ('column', column.name),
                    ('rule', rule_name),
                    ('expected', param),
                    ('actual', value),
                ]))
    return violations

@dataclasses.dataclass
class ValidationResult:
    '''検査全体の結果。'''
    num_rows: int = 0
    num_valid: int = 0
    num_invalid: int = 0
    violations: list[OrderedDict] = dataclasses.field(default_factory=list)

    @property
    def ok(self) -> bool:
        '''全ての行が仕様を満たしていれば True。'''
        return self.num_invalid == 0

def print_summary(
    console,
    result: ValidationResult,
) -> None:
    '''
    検査結果の要約を端末に表示する。

    Args:
        console: 出力先の rich Console。
        result: 検査結果。
    '''
    table = Table(title='validation summary', title_justify='left')
    table.add_column('rows', justify='right')
    table.add_column('valid', justify='right')
    table.add_column('invalid', justify='right')
    table.add_row(
        str(result.num_rows),
        f'[green]{result.num_valid}[/green]',
        f'[red]{result.num_invalid}[/red]' if result.num_invalid
        else f'[green]{result.num_invalid}[/green]',
    )
    console.print(table)
    if not result.violations:
        return
    # NOTE: 列と規則の組ごとに件数を集計し、どこを直せばよいかが一目で分かるようにする
    counts: OrderedDict = OrderedDict()
    for violation in result.violations:
        key = (violation['column'], violation['rule'])
        counts[key] = counts.get(key, 0) + 1
    table = Table(title='violations by column and rule', title_justify='left')
    table.add_column('column', overflow='fold')
    table.add_column('rule')
    table.add_column('count', justify='right')
    for (column, rule), count in sorted(
        counts.items(), key=lambda item: item[1], reverse=True,
    ):
        table.add_row(str(column), str(rule), f'[red]{count}[/red]')
    console.print(table)

def validate(
    input_files: list[str],
    schema_path: str,
    output_valid: str | None = None,
    output_invalid: str | None = None,
    report_file: str | None = None,
    verbose: bool = False,
) -> ValidationResult:
    '''
    入力ファイルがスキーマを満たしているかを検査する。

    Args:
        input_files: 検査対象のファイル。
        schema_path: スキーマ (YAML) のパス。
        output_valid: 仕様を満たした行の書き出し先。
        output_invalid: 違反した行の書き出し先 (違反理由が付与される)。
        report_file: 違反一覧の書き出し先。
        verbose: 詳細ログを出すかどうか。

    Returns:
        検査結果。

    Raises:
        SchemaError: スキーマの記述に誤りがある場合。
        FileNotFoundError: 入力ファイルが存在しない場合。
    '''
    schema = load_schema(schema_path)
    # NOTE: 長い検査の後で書き出せないと分かるのを避けるため、先に拡張子を検査する
    for output_path in [output_valid, output_invalid, report_file]:
        if output_path:
            check_writer(output_path)
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
    progress = Progress(
        redirect_stdout = False,
    )
    progress.start()
    console = progress.console
    console.log('input_files: ', input_files)
    console.log('schema: ', schema_path)
    result = ValidationResult()
    writer_valid = get_writer(output_valid, progress=progress) \
        if output_valid else None
    writer_invalid = get_writer(output_invalid, progress=progress) \
        if output_invalid else None
    try:
        for input_file in input_files:
            loader = get_loader(
                input_file,
                progress=progress,
            )
            console.log('# rows: ', len(loader))
            for index, row in enumerate(loader):
                result.num_rows += 1
                violations = validate_row(row, schema)
                if not violations:
                    result.num_valid += 1
                    if writer_valid:
                        writer_valid.push_row(row)
                    continue
                result.num_invalid += 1
                for violation in violations:
                    # NOTE: レポートからどのファイルの何行目かを辿れるようにする
                    record = OrderedDict()
                    record['file'] = input_file
                    record['row_index'] = index
                    record.update(violation)
                    result.violations.append(record)
                    if verbose:
                        console.log('violation: ', record)
                if writer_invalid:
                    # NOTE:
                    #   違反情報は __staging__ ではなく専用の名前空間に置く。
                    #   __staging__ は出力前に捨てられる中間値のための領域であり、
                    #   捨ててはならない違反情報とは性質が逆であるため。
                    row[VIOLATIONS_FIELD] = violations
                    writer_invalid.push_row(row)
    finally:
        if writer_valid:
            writer_valid.close()
        if writer_invalid:
            writer_invalid.close()
    if report_file:
        console.log('writing report into: ', report_file)
        writer_report = get_writer(report_file, progress=progress)
        writer_report.push_rows([
            Row.from_dict(violation) for violation in result.violations
        ])
        writer_report.close()
    print_summary(console, result)
    progress.stop()
    return result
