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

@register_rule('max_length')
def check_max_length(value: Any, expected: Any) -> bool:
    '''値の文字数が上限以内かを判定する。'''
    return len(str(value)) <= int(expected)

# NOTE:
#   unique は「既出の値」を覚えていないと判定できないため、
#   1行だけを見る dict_rules ではなく検査状態を伴う経路で扱う。
STATEFUL_RULES = ['unique']

UNIQUE_SCOPES = [True, 'per_file']
UNKNOWN_COLUMN_MODES = ['error', 'warn', 'ignore']

@dataclasses.dataclass
class ValidationState:
    '''
    複数行・複数ファイルにまたがる検査のための状態。

    保持するのは検査対象の列の値のみで、行データ自体は保持しない。
    '''
    # (スコープキー, 列名) から、既出の値の集合への対応
    seen_values: dict = dataclasses.field(default_factory=dict)
    # スキーマに無い列の名前から、出現したファイル数への対応
    unknown_columns: OrderedDict = dataclasses.field(default_factory=OrderedDict)

    def is_first_occurrence(
        self,
        scope_key: str,
        column: str,
        value: Any,
    ) -> bool:
        '''
        その値がまだ現れていなければ True を返し、現れたものとして記録する。

        Args:
            scope_key: 一意性を判定する範囲を表すキー。
            column: 対象の列名。
            value: 判定する値。

        Returns:
            初出であれば True。
        '''
        key = (scope_key, column)
        values = self.seen_values.setdefault(key, set())
        # NOTE:
        #   CSV 由来の値は文字列、JSON 由来は数値になりうるため、
        #   文字列に揃えて比較する。
        normalized = str(value)
        if normalized in values:
            return False
        values.add(normalized)
        return True

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
    # スキーマに無い列が現れたときの扱い
    unknown_columns: str = 'warn'

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
    mode = loaded.get('unknown_columns', 'warn')
    if mode not in UNKNOWN_COLUMN_MODES:
        raise SchemaError(
            f'Unsupported unknown_columns mode: {mode!r}. '
            f'Must be one of {UNKNOWN_COLUMN_MODES}.'
        )
    schema.unknown_columns = mode
    for name, definition in dict_columns.items():
        if isinstance(name, bool):
            # NOTE:
            #   YAML は no / No / on / off などを真偽値として解釈するため、
            #   引用符を付けずに書いた列名が黙って True / False に化ける。
            #   さらに no と No は同じ False に潰れて定義が合流してしまうため、
            #   気付けるようにここで明示的に停止する。
            raise SchemaError(
                f'Column name was read as the boolean {name}. '
                'YAML treats no, No, on, off and similar words as booleans; '
                'quote the column name to keep it as text.'
            )
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
            if key not in dict_rules and key not in STATEFUL_RULES:
                raise SchemaError(
                    f'Unsupported rule {key!r} for column {name!r}. '
                    f'Supported rules: {sorted(list(dict_rules) + STATEFUL_RULES)}'
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
            if key == 'unique' and param not in UNIQUE_SCOPES:
                if param is False:
                    # NOTE: unique: false は「検査しない」の意なので登録しない
                    continue
                raise SchemaError(
                    f'unique of column {name!r} must be true or "per_file".'
                )
            if key == 'max_length':
                if isinstance(param, bool) or not isinstance(param, int):
                    raise SchemaError(
                        f'max_length of column {name!r} must be an integer.'
                    )
                if param < 0:
                    raise SchemaError(
                        f'max_length of column {name!r} must not be negative.'
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

def find_unknown_columns(
    row: Row,
    schema: Schema,
) -> list[str]:
    '''
    スキーマに定義されていない列の名前を返す。

    ネストした値はフラット表現 (`a.b`) で現れるため、
    スキーマが `a` を定義していれば `a.b` も既知として扱う。

    Args:
        row: 対象の行。
        schema: 検査仕様。

    Returns:
        未定義の列名のリスト。
    '''
    known = [column.name for column in schema.columns]
    unknown: list[str] = []
    for key in row.keys():
        name = str(key)
        if name in known:
            continue
        if any(name.startswith(f'{k}.') for k in known):
            continue
        if name not in unknown:
            unknown.append(name)
    return unknown

def validate_row(
    row: Row,
    schema: Schema,
    state: ValidationState | None = None,
    file_path: str = '',
) -> list[OrderedDict]:
    '''
    1行を検査し、違反の一覧を返す。

    Args:
        row: 検査対象の行。
        schema: 検査仕様。
        state: 複数行にまたがる検査 (unique) のための状態。
            省略した場合は毎回新しい状態を用いるため、unique は常に満たされる。
        file_path: unique を per_file で判定する際に用いるファイルのパス。

    Returns:
        違反を表す辞書のリスト。違反が無ければ空リスト。
    '''
    if state is None:
        state = ValidationState()
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
            if rule_name == 'unique':
                # NOTE:
                #   最初の出現は適合とし、2件目以降を違反とする。
                #   こうすると差し戻す対象が一意に決まる。
                scope_key = file_path if param == 'per_file' else ''
                if state.is_first_occurrence(scope_key, column.name, value):
                    continue
                violations.append(OrderedDict([
                    ('column', column.name),
                    ('rule', 'unique'),
                    ('expected', 'per_file' if param == 'per_file'
                        else 'unique across all inputs'),
                    ('actual', value),
                ]))
                continue
            checker = dict_rules[rule_name]
            if not checker(value, param):
                violations.append(OrderedDict([
                    ('column', column.name),
                    ('rule', rule_name),
                    ('expected', param),
                    ('actual', value),
                ]))
    if schema.unknown_columns != 'ignore':
        for name in find_unknown_columns(row, schema):
            if schema.unknown_columns == 'error':
                violations.append(OrderedDict([
                    ('column', name),
                    ('rule', 'unknown_column'),
                    ('expected', 'a column defined in the schema'),
                    ('actual', name),
                ]))
            else:
                # NOTE: warn では違反とせず、最後にまとめて件数を報告する
                state.unknown_columns[name] = \
                    state.unknown_columns.get(name, 0) + 1
    return violations

@dataclasses.dataclass
class ValidationResult:
    '''検査全体の結果。'''
    num_rows: int = 0
    num_valid: int = 0
    num_invalid: int = 0
    violations: list[OrderedDict] = dataclasses.field(default_factory=list)
    # スキーマに無い列 (unknown_columns: warn のときに記録される)
    unknown_columns: OrderedDict = dataclasses.field(default_factory=OrderedDict)

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
    if result.unknown_columns:
        # NOTE:
        #   warn では違反としないが、黙って通すと
        #   列の作り変えに気付けないため必ず表示する。
        table = Table(
            title='columns not defined in the schema',
            title_justify='left',
        )
        table.add_column('column', overflow='fold')
        table.add_column('rows', justify='right')
        for name, count in result.unknown_columns.items():
            table.add_row(f'[yellow]{name}[/yellow]', str(count))
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
    # NOTE: unique の判定は入力全体にまたがるため、ファイルをまたいで保持する
    state = ValidationState()
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
                violations = validate_row(
                    row, schema, state=state, file_path=input_file,
                )
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
    result.unknown_columns = state.unknown_columns
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
