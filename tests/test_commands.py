# -*- coding: utf-8 -*-
'''
各コマンド (convert / sort / compare / merge / aggregate) の回帰テスト。

特に、過去に「無言で何もしない」「無言でデータを破損させる」挙動をしていた
経路を重点的に検証する。
'''

import argparse
import csv
import json
import shutil
import subprocess

from collections import OrderedDict

from pathlib import Path

import pytest

from tabpro.core.aggregate import (
    aggregate,
    compare_file_columns,
    normalize_column_name,
)
from tabpro.core.compare import compare
from tabpro.core.convert import convert
from tabpro.core.merge import merge
from tabpro.core.io.extensions.io_dbq import (
    QuerySpecError,
    iter_sqlalchemy_rows,
    load_query_spec,
    mask_url,
)
from tabpro.core.sort import sort
from tabpro.core.classes.row import Row
from tabpro.core.validate import (
    SchemaError,
    load_schema,
    validate,
    validate_row,
)

from tabpro.commands.aggregate_tables import setup_parser as setup_aggregate_parser
from tabpro.commands.compare_tables import setup_parser as setup_compare_parser
from tabpro.commands.convert_tables import setup_parser as setup_convert_parser
from tabpro.commands.merge_tables import setup_parser as setup_merge_parser
from tabpro.commands.sort_tables import setup_parser as setup_sort_parser

CSV_SAMPLE = (
    'id,name,score,note\n'
    '1,alice,10,"hello, world"\n'
    '2,bob,20,\n'
    '3,carol,30,x\n'
)

CSV_MODIFIED = (
    'id,name,score,note\n'
    '1,alice,10,"hello, world"\n'
    '2,bob,25,\n'
    '4,dave,40,y\n'
)

JSONL_SAMPLE = (
    '{"id": 1, "name": "alice", "nested": {"k": "v"}}\n'
    '{"id": 2, "name": "bob", "nested": {"k": "w"}}\n'
)

def write_file(path: Path, content: str) -> Path:
    '''テスト用の入力ファイルを UTF-8 で書き出す。'''
    path.write_text(content, encoding='utf-8')
    return path

def read_jsonl(path: Path) -> list[dict]:
    '''JSON Lines ファイルを辞書のリストとして読み込む。'''
    with open(path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]

def read_csv(path: Path) -> list[dict]:
    '''CSV ファイルを辞書のリストとして読み込む。'''
    with open(path, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))

def read_tsv(path: Path) -> list[dict]:
    '''TSV ファイルを辞書のリストとして読み込む。'''
    with open(path, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f, delimiter='\t'))

@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    '''標準的な CSV 入力ファイル。'''
    return write_file(tmp_path / 'input.csv', CSV_SAMPLE)

@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    '''ネストした値を含む JSON Lines 入力ファイル。'''
    return write_file(tmp_path / 'input.jsonl', JSONL_SAMPLE)

# --- convert: 基本動作 -------------------------------------------------

def test_convert_csv_to_jsonl(csv_file: Path, tmp_path: Path):
    '''CSV から JSON Lines へ変換できる。'''
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(csv_file)], output_file=str(output))
    rows = read_jsonl(output)
    assert len(rows) == 3
    assert rows[0] == {
        'id': '1', 'name': 'alice', 'score': '10', 'note': 'hello, world',
    }

def test_convert_jsonl_to_csv_flattens_nested(jsonl_file: Path, tmp_path: Path):
    '''ネストした値がドット区切りのカラムに展開される。'''
    output = tmp_path / 'out.csv'
    convert(input_files=[str(jsonl_file)], output_file=str(output))
    rows = read_csv(output)
    assert rows[0]['nested.k'] == 'v'

def test_convert_pick_columns(csv_file: Path, tmp_path: Path):
    '''--pick で指定したカラムのみが出力される。'''
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        list_pick_columns=['id', 'name'],
    )
    assert read_jsonl(output)[0] == {'id': '1', 'name': 'alice'}

def test_convert_tsv_input(tmp_path: Path):
    """TSV を入力として読み込める。"""
    source = write_file(
        tmp_path / 'input.tsv',
        'id\tname\n1\talice\n2\tbob\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert read_jsonl(output) == [
        {'id': '1', 'name': 'alice'},
        {'id': '2', 'name': 'bob'},
    ]

def test_convert_tsv_output(csv_file: Path, tmp_path: Path):
    """TSV を出力先として書き出せる。"""
    output = tmp_path / 'out.tsv'
    convert(input_files=[str(csv_file)], output_file=str(output))
    rows = read_tsv(output)
    assert len(rows) == 3
    # NOTE: カンマを含む値がタブ区切りではそのまま保持される
    assert rows[0]['note'] == 'hello, world'

def test_convert_tsv_round_trip(tmp_path: Path):
    """CSV -> TSV -> CSV で内容が保たれる。"""
    source = write_file(tmp_path / 'input.csv', CSV_SAMPLE)
    intermediate = tmp_path / 'mid.tsv'
    output = tmp_path / 'out.csv'
    convert(input_files=[str(source)], output_file=str(intermediate))
    convert(input_files=[str(intermediate)], output_file=str(output))
    assert read_csv(output) == read_csv(source)

def test_convert_preserves_newline_in_quoted_field(tmp_path: Path):
    """引用符で囲まれたフィールド内の改行が保持される。"""
    source = write_file(
        tmp_path / 'input.csv',
        'id,note\n1,"line1\nline2"\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert read_jsonl(output)[0]['note'] == 'line1\nline2'

# --- Excel: シート選択と旧形式 -------------------------------------------

DATA_DIR = Path(__file__).parent / 'data'

def write_workbook(path: Path, sheets: dict[str, list[dict]]) -> Path:
    """複数シートを持つ .xlsx を作成する。"""
    import pandas as pd
    with pd.ExcelWriter(path) as writer:
        for name, records in sheets.items():
            pd.DataFrame(records).to_excel(writer, sheet_name=name, index=False)
    return path

@pytest.fixture
def workbook(tmp_path: Path) -> Path:
    """3枚のシートを持つブック。"""
    return write_workbook(tmp_path / 'book.xlsx', {
        'first': [{'id': 1, 'name': 'alice'}, {'id': 2, 'name': 'bob'}],
        'second': [{'id': 3, 'name': 'carol'}],
        'third': [{'id': 4, 'name': 'dave'}],
    })

def test_excel_reads_the_first_sheet_by_default(
    workbook: Path, tmp_path: Path,
):
    """既定では最初の可視シートのみを読む (従来どおり)。"""
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(workbook)], output_file=str(output))
    assert [row['name'] for row in read_jsonl(output)] == ['alice', 'bob']

def test_excel_warns_when_sheets_are_skipped(
    workbook: Path, tmp_path: Path, capsys,
):
    """
    回帰テスト: 読み飛ばすシートがあることが利用者に伝わること。

    以前は複数シートがあっても最初の1枚だけを黙って読み、
    残りのデータが何の表示も無く欠落していた。
    """
    convert(
        input_files=[str(workbook)],
        output_file=str(tmp_path / 'out.jsonl'),
    )
    captured = capsys.readouterr()
    assert 'warning' in (captured.out + captured.err)
    assert 'second' in (captured.out + captured.err)

def test_excel_sheet_can_be_chosen(workbook: Path, tmp_path: Path):
    """--sheet で読むシートを指定できる。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(workbook)],
        output_file=str(output),
        sheet='second',
    )
    assert [row['name'] for row in read_jsonl(output)] == ['carol']

def test_excel_unknown_sheet_is_rejected(workbook: Path, tmp_path: Path):
    """存在しないシート名は、名前と候補を添えて拒否される。"""
    with pytest.raises(ValueError, match='Sheet not found'):
        convert(
            input_files=[str(workbook)],
            output_file=str(tmp_path / 'out.jsonl'),
            sheet='nosuch',
        )

def test_excel_all_sheets(workbook: Path, tmp_path: Path):
    """--all-sheets で全ての可視シートが読まれる。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(workbook)],
        output_file=str(output),
        all_sheets=True,
    )
    assert [row['name'] for row in read_jsonl(output)] == \
        ['alice', 'bob', 'carol', 'dave']

def test_excel_all_sheets_records_the_sheet_name(
    workbook: Path, tmp_path: Path,
):
    """--all-sheets では、どのシート由来かが staging に残る。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(workbook)],
        output_file=str(output),
        all_sheets=True,
        output_debug=True,
    )
    rows = read_jsonl(output)
    assert rows[0]['__staging__']['__sheet__'] == 'first'
    assert rows[2]['__staging__']['__sheet__'] == 'second'

def test_excel_all_sheets_keeps_the_input_record_clean(
    workbook: Path, tmp_path: Path,
):
    """シート名が、入力値の記録 (__input__) に混入しない。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(workbook)],
        output_file=str(output),
        all_sheets=True,
        output_debug=True,
    )
    recorded = read_jsonl(output)[0]['__staging__']['__input__']
    assert '__staging__' not in recorded
    assert list(recorded['__values__'].values()) == ['1', 'alice']

def test_legacy_xls_can_be_read(tmp_path: Path):
    """旧形式 (.xls) を読み込める。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(DATA_DIR / 'legacy.xls')],
        output_file=str(output),
    )
    assert [row['name'] for row in read_jsonl(output)] == ['alice', 'bob']

def test_legacy_xls_all_sheets(tmp_path: Path):
    """旧形式でも --all-sheets が効く。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(DATA_DIR / 'legacy.xls')],
        output_file=str(output),
        all_sheets=True,
    )
    assert [row['name'] for row in read_jsonl(output)] == \
        ['alice', 'bob', 'carol']

def test_legacy_xls_cannot_be_written(tmp_path: Path):
    """.xls は読めるが書けないことが、そう明示される。"""
    with pytest.raises(ValueError, match='read but not written'):
        convert(
            input_files=[str(DATA_DIR / 'legacy.xls')],
            output_file=str(tmp_path / 'out.xls'),
        )

# --- 由来情報 (staging) の引き継ぎ ---------------------------------------

def test_existing_provenance_is_not_overwritten(tmp_path: Path):
    """
    既に由来情報を持つ行は、中間ファイルを経由しても上書きされない。

    納品物に問題があったとき、最初の入力ファイルの何行目が元だったかを
    辿れることに依存した運用があるため、この性質を固定する。
    """
    source = write_file(
        tmp_path / 'mid.jsonl',
        '{"id": "1", "name": "alice", '
        '"__staging__": {"__file__": "original.csv", "__row_index__": 7}}\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(source)],
        output_file=str(output),
        output_debug=True,
        list_actions=['cast:id=id:as=int'],
        list_pick_columns=['id', 'name'],
    )
    staging = read_jsonl(output)[0]['__staging__']
    assert staging['__file__'] == 'original.csv'
    assert staging['__row_index__'] == 7

def test_provenance_is_dropped_for_delivery(tmp_path: Path):
    """既定では、納品用に由来情報が落とされる。"""
    source = write_file(
        tmp_path / 'mid.jsonl',
        '{"id": "1", "__staging__": {"__file__": "original.csv"}}\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert read_jsonl(output)[0] == {'id': '1'}

# --- parse アクション ---------------------------------------------------

def test_parse_action_as_json_is_applied(tmp_path: Path):
    '''
    回帰テスト: as=json 指定の parse が無言で無視されないこと。

    setup_parse_action のインデント誤りにより、以前は as=boolean 以外の
    指定が一切登録されず、エラーも出ないまま何も起きなかった。
    '''
    source = write_file(
        tmp_path / 'input.csv',
        'id,payload\n1,"{""a"": 1}"\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(source)],
        output_file=str(output),
        list_actions=['parse:parsed=payload:as=json'],
        list_pick_columns=['id', 'parsed'],
    )
    rows = read_jsonl(output)
    assert rows[0]['parsed'] == {'a': 1}

def test_parse_action_rejects_unknown_type(csv_file: Path, tmp_path: Path):
    '''未対応の as 指定は明示的にエラーになる。'''
    with pytest.raises(ValueError):
        convert(
            input_files=[str(csv_file)],
            output_file=str(tmp_path / 'out.jsonl'),
            list_actions=['parse:x=note:as=unknown'],
        )

# --- フィルタと除外行の書き出し -----------------------------------------

def test_filter_writes_filtered_out_rows(csv_file: Path, tmp_path: Path):
    """
    回帰テスト: --output-file-filtered-out が機能すること。

    以前は Row ではなく row.flat (OrderedDict) を蓄積していたため、
    書き出し時に AttributeError で必ず落ちていた。
    """
    output = tmp_path / 'out.jsonl'
    rejected = tmp_path / 'rejected.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        output_file_filtered_out=str(rejected),
        list_actions=['filter:name==bob'],
    )
    assert [row['name'] for row in read_jsonl(output)] == ['bob']
    assert [row['name'] for row in read_jsonl(rejected)] == ['alice', 'carol']

def test_filter_writes_filtered_out_rows_to_csv(csv_file: Path, tmp_path: Path):
    """除外行の書き出しは CSV でも機能する。"""
    rejected = tmp_path / 'rejected.csv'
    convert(
        input_files=[str(csv_file)],
        output_file=str(tmp_path / 'out.csv'),
        output_file_filtered_out=str(rejected),
        list_actions=['filter:name==bob'],
    )
    assert [row['name'] for row in read_csv(rejected)] == ['alice', 'carol']

def test_filter_by_regex(csv_file: Path, tmp_path: Path):
    """正規表現によるフィルタが機能する。"""
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        list_actions=['filter:name=~^a'],
    )
    assert [row['name'] for row in read_jsonl(output)] == ['alice']

# --- cast アクション ----------------------------------------------------

def test_cast_existing_field(csv_file: Path, tmp_path: Path):
    '''存在するフィールドは指定の型に変換される。'''
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        list_actions=['cast:score=score:as=int'],
        list_pick_columns=['id', 'score'],
    )
    assert read_jsonl(output)[0]['score'] == 10

def test_cast_missing_field_does_not_write_none_string(
    csv_file: Path, tmp_path: Path,
):
    '''
    回帰テスト: 存在しないフィールドへの as=str が文字列 'None' を
    書き込まないこと (無言のデータ破損)。
    '''
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        list_actions=['cast:v=nosuch:as=str'],
        list_pick_columns=['id', 'v'],
    )
    for row in read_jsonl(output):
        assert 'v' not in row

def test_cast_missing_field_does_not_raise(csv_file: Path, tmp_path: Path):
    '''
    回帰テスト: required 指定の無い cast が、フィールド欠損だけを理由に
    処理全体を停止させないこと。
    '''
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        list_actions=['cast:v=nosuch:as=int'],
        list_pick_columns=['id', 'v'],
    )
    assert len(read_jsonl(output)) == 3

def test_cast_missing_field_uses_default(csv_file: Path, tmp_path: Path):
    '''default 指定があれば欠損時に既定値が入る。'''
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        list_actions=['cast:v=nosuch:as=int,default=0'],
        list_pick_columns=['id', 'v'],
    )
    assert read_jsonl(output)[0]['v'] == '0'

# --- YAML 設定ファイル経路 ----------------------------------------------

def test_config_assign_length(csv_file: Path, tmp_path: Path):
    '''
    回帰テスト: process.assign_length が AttributeError にならないこと。
    '''
    config = write_file(
        tmp_path / 'config.yaml',
        'process:\n'
        '  assign_length:\n'
        '    namelen: name\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        config_path=str(config),
        list_pick_columns=['id', 'namelen'],
    )
    assert read_jsonl(output)[0]['namelen'] == len('alice')

def test_config_assign_array(csv_file: Path, tmp_path: Path):
    '''
    回帰テスト: process.assign_array が TypeError にならず、
    かつアクションとして実行されること。
    '''
    config = write_file(
        tmp_path / 'config.yaml',
        'process:\n'
        '  assign_array:\n'
        '    arr:\n'
        '      - name\n'
        '      - note\n',
    )
    output = tmp_path / 'out.jsonl'
    convert(
        input_files=[str(csv_file)],
        output_file=str(output),
        config_path=str(config),
        list_pick_columns=['id', 'arr'],
    )
    assert read_jsonl(output)[0]['arr'] == ['alice', 'hello, world']

# --- その他のコマンド ---------------------------------------------------

def test_sort(csv_file: Path, tmp_path: Path):
    '''指定キーの降順で並べ替えられる。'''
    output = tmp_path / 'out.csv'
    sort(
        sort_keys=['score'],
        input_files=[str(csv_file)],
        output_file=str(output),
        reverse=True,
    )
    assert [row['score'] for row in read_csv(output)] == ['30', '20', '10']

def test_compare(csv_file: Path, tmp_path: Path):
    '''値の変更・削除・追加がそれぞれ差分として検出される。'''
    modified = write_file(tmp_path / 'modified.csv', CSV_MODIFIED)
    output = tmp_path / 'diff.json'
    compare(
        path1=str(csv_file),
        path2=str(modified),
        output_path=str(output),
        query_keys=['id'],
    )
    diff = json.loads(output.read_text(encoding='utf-8'))
    assert len(diff) == 3
    assert diff[0]['diff'] == {'-score': '20', '+score': '25'}

def test_merge(csv_file: Path, tmp_path: Path):
    '''主キーが一致する行の値が修正ファイルの内容で更新される。'''
    modified = write_file(tmp_path / 'modified.csv', CSV_MODIFIED)
    output = tmp_path / 'merged.csv'
    merge(
        previous_files=[str(csv_file)],
        modification_files=[str(modified)],
        keys=['id'],
        ignore_not_found=True,
        output_base_data_file=str(output),
    )
    rows = {row['id']: row for row in read_csv(output)}
    assert rows['2']['score'] == '25'
    assert rows['3']['score'] == '30'

def test_aggregate(csv_file: Path, tmp_path: Path):
    '''行数とカラムごとの変種数が集計される。'''
    output = tmp_path / 'agg.json'
    aggregate(input_files=[str(csv_file)], output_file=str(output))
    result = json.loads(output.read_text(encoding='utf-8'))
    assert result['num_rows'] == 3
    assert result['aggregated']['name']['num_variations'] == 3

# --- aggregate: ファイル別の列構成比較 -----------------------------------

def test_normalize_column_name():
    """大文字小文字・全半角・前後空白の違いが吸収される。"""
    assert normalize_column_name('Comment') == normalize_column_name('comment')
    assert normalize_column_name(' id ') == normalize_column_name('id')
    assert normalize_column_name('ＩＤ') == normalize_column_name('id')
    # NOTE: 意味が同じでも綴りが違うものは別扱い (曖昧一致はしない)
    assert normalize_column_name('label') != normalize_column_name('ラベル')

def test_compare_file_columns_detects_missing_and_extra():
    """半数以上のファイルにある列を基準に、欠落列と余剰列が検出される。"""
    result = compare_file_columns(OrderedDict([
        ('a.csv', ['id', 'label', 'comment']),
        ('b.csv', ['id', 'label']),
        ('c.csv', ['id', 'label', 'comment', 'memo']),
    ]))
    assert result['expected_columns'] == ['id', 'label', 'comment']
    assert result['missing'] == {'b.csv': ['comment']}
    assert result['extra'] == {'c.csv': ['memo']}

def test_compare_file_columns_detects_name_variants():
    """綴り揺れの候補が列挙される。"""
    result = compare_file_columns(OrderedDict([
        ('a.csv', ['id', 'comment']),
        ('b.csv', ['id', 'Comment']),
    ]))
    assert result['name_variants'] == [['comment', 'Comment']]

def test_compare_file_columns_without_anomalies():
    """全ファイルの列構成が同一なら何も報告されない。"""
    result = compare_file_columns(OrderedDict([
        ('a.csv', ['id', 'name']),
        ('b.csv', ['id', 'name']),
    ]))
    assert result['missing'] == {}
    assert result['extra'] == {}
    assert result['name_variants'] == []
    assert result['expected_columns'] == ['id', 'name']

def test_aggregate_compare_columns(tmp_path: Path):
    """--compare-columns の結果がレポートに含まれる。"""
    a = write_file(tmp_path / 'a.csv', 'id,label,comment\n1,x,ok\n')
    b = write_file(tmp_path / 'b.csv', 'id,label,Comment\n2,y,ng\n')
    c = write_file(tmp_path / 'c.csv', 'id,label,comment\n3,z,ok\n')
    output = tmp_path / 'agg.json'
    aggregate(
        input_files=[str(a), str(b), str(c)],
        output_file=str(output),
        compare_columns=True,
    )
    result = json.loads(output.read_text(encoding='utf-8'))
    comparison = result['column_comparison']
    assert comparison['expected_columns'] == ['id', 'label', 'comment']
    assert comparison['missing'] == {str(b): ['comment']}
    assert comparison['extra'] == {str(b): ['Comment']}
    assert comparison['name_variants'] == [['comment', 'Comment']]
    assert comparison['matrix'][str(a)]['comment'] is True
    assert comparison['matrix'][str(b)]['comment'] is False

def test_aggregate_without_compare_columns_keeps_output_format(
    csv_file: Path, tmp_path: Path,
):
    """既定では従来どおりの出力形式が保たれる。"""
    output = tmp_path / 'agg.json'
    aggregate(input_files=[str(csv_file)], output_file=str(output))
    result = json.loads(output.read_text(encoding='utf-8'))
    assert list(result.keys()) == ['num_rows', 'aggregated']

def test_aggregate_compare_columns_with_empty_file(tmp_path: Path):
    """行が1件も無いファイルも比較対象に含まれる。"""
    a = write_file(tmp_path / 'a.csv', 'id,name\n1,alice\n')
    empty = write_file(tmp_path / 'empty.csv', 'id,name\n')
    output = tmp_path / 'agg.json'
    aggregate(
        input_files=[str(a), str(empty)],
        output_file=str(output),
        compare_columns=True,
    )
    comparison = json.loads(output.read_text(encoding='utf-8'))['column_comparison']
    assert comparison['files'] == [str(a), str(empty)]
    assert comparison['missing'] == {str(empty): ['id', 'name']}

# --- validate: スキーマ検査 ----------------------------------------------

SPEC_SAMPLE = """
columns:
  id:
    required: true
    type: int
    pattern: '^[0-9]{4}$'
  label:
    required: true
    enum: [positive, negative, neutral]
  comment:
    required: false
"""

VALIDATE_SAMPLE = (
    'id,label,comment\n'
    '0001,positive,ok\n'      # 適合
    'abc,positive,\n'         # id が型・正規表現の両方に違反
    '0002,POSITIVE,x\n'       # label が許容値に無い
    '0003,,y\n'               # label が必須なのに空
    '12,neutral,z\n'          # id は整数だが桁数が合わない
)

@pytest.fixture
def schema_file(tmp_path: Path) -> Path:
    """標準的なスキーマファイル。"""
    return write_file(tmp_path / 'spec.yaml', SPEC_SAMPLE)

def test_load_schema(schema_file: Path):
    """スキーマが定義順どおりに読み込まれる。"""
    schema = load_schema(str(schema_file))
    assert [c.name for c in schema.columns] == ['id', 'label', 'comment']
    assert schema.columns[0].required is True
    assert schema.columns[0].checks == [
        ('type', 'int'), ('pattern', '^[0-9]{4}$'),
    ]
    assert schema.columns[2].required is False

@pytest.mark.parametrize(
    'content, reason',
    [
        ('columns:\n  id:\n    nosuchrule: true\n', '未対応の規則'),
        ('columns:\n  id:\n    type: complex\n', '未対応の型'),
        ('columns:\n  id:\n    enum: positive\n', 'enum がリストでない'),
        ('columns:\n  id:\n    required: yes_please\n', 'required が真偽値でない'),
        ('columns: []\n', 'columns がマッピングでない'),
        ('foo: bar\n', 'columns が無い'),
    ],
)
def test_load_schema_rejects_bad_definitions(
    tmp_path: Path, content: str, reason: str,
):
    """スキーマ自体の誤りは検査開始前に明示的なエラーになる。"""
    path = write_file(tmp_path / 'spec.yaml', content)
    with pytest.raises(SchemaError):
        load_schema(str(path))

def test_load_schema_rejects_non_yaml(tmp_path: Path):
    """YAML 以外のスキーマファイルは受け付けない。"""
    path = write_file(tmp_path / 'spec.json', '{}')
    with pytest.raises(SchemaError):
        load_schema(str(path))

def test_validate_row_accepts_valid_row(schema_file: Path):
    """仕様を満たす行には違反が出ない。"""
    schema = load_schema(str(schema_file))
    row = Row.from_dict({'id': '0001', 'label': 'positive', 'comment': 'ok'})
    assert validate_row(row, schema) == []

def test_validate_row_reports_every_failing_rule(schema_file: Path):
    """1つの値が複数の規則に違反する場合、全て報告される。"""
    schema = load_schema(str(schema_file))
    row = Row.from_dict({'id': 'abc', 'label': 'positive'})
    violations = validate_row(row, schema)
    assert [(v['column'], v['rule']) for v in violations] == [
        ('id', 'type'), ('id', 'pattern'),
    ]

def test_validate_row_required_stops_further_checks(schema_file: Path):
    """必須項目が空のとき、違反は required の1件に絞られる。"""
    schema = load_schema(str(schema_file))
    row = Row.from_dict({'id': '', 'label': 'positive'})
    violations = validate_row(row, schema)
    assert [(v['column'], v['rule']) for v in violations] == [('id', 'required')]

def test_validate_row_skips_checks_for_empty_optional(tmp_path: Path):
    """任意項目が空なら、型などの検査は行われない。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  score:\n    required: false\n    type: int\n',
    )
    schema = load_schema(str(path))
    assert validate_row(Row.from_dict({'score': ''}), schema) == []
    assert validate_row(Row.from_dict({}), schema) == []
    # NOTE: 値があるなら任意項目でも検査される
    assert len(validate_row(Row.from_dict({'score': 'x'}), schema)) == 1

def test_validate_type_rule_distinguishes_bool_from_int(tmp_path: Path):
    """bool は int としては扱わない。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  flag:\n    required: true\n    type: int\n',
    )
    schema = load_schema(str(path))
    assert len(validate_row(Row.from_dict({'flag': True}), schema)) == 1
    assert validate_row(Row.from_dict({'flag': 3}), schema) == []

def test_validate_separates_valid_and_invalid_rows(
    schema_file: Path, tmp_path: Path,
):
    """適合行と違反行がそれぞれの出力先に振り分けられる。"""
    source = write_file(tmp_path / 'in.csv', VALIDATE_SAMPLE)
    valid = tmp_path / 'valid.jsonl'
    invalid = tmp_path / 'invalid.jsonl'
    result = validate(
        input_files=[str(source)],
        schema_path=str(schema_file),
        output_valid=str(valid),
        output_invalid=str(invalid),
    )
    assert (result.num_rows, result.num_valid, result.num_invalid) == (5, 1, 4)
    assert result.ok is False
    assert [row['id'] for row in read_jsonl(valid)] == ['0001']
    assert [row['id'] for row in read_jsonl(invalid)] == \
        ['abc', '0002', '0003', '12']

def test_validate_attaches_violations_to_invalid_rows(
    schema_file: Path, tmp_path: Path,
):
    """違反行には理由が __violations__ として付与される。"""
    source = write_file(tmp_path / 'in.csv', VALIDATE_SAMPLE)
    invalid = tmp_path / 'invalid.jsonl'
    validate(
        input_files=[str(source)],
        schema_path=str(schema_file),
        output_invalid=str(invalid),
    )
    rows = read_jsonl(invalid)
    assert rows[0]['__violations__'] == [
        {'column': 'id', 'rule': 'type',
         'expected': 'int', 'actual': 'abc'},
        {'column': 'id', 'rule': 'pattern',
         'expected': '^[0-9]{4}$', 'actual': 'abc'},
    ]

def test_validate_does_not_touch_valid_rows(
    schema_file: Path, tmp_path: Path,
):
    """適合行に __violations__ は付かない。"""
    source = write_file(tmp_path / 'in.csv', VALIDATE_SAMPLE)
    valid = tmp_path / 'valid.jsonl'
    validate(
        input_files=[str(source)],
        schema_path=str(schema_file),
        output_valid=str(valid),
    )
    assert '__violations__' not in read_jsonl(valid)[0]

def test_validate_report_records_the_source_location(
    schema_file: Path, tmp_path: Path,
):
    """レポートから、どのファイルの何行目かを辿れる。"""
    source = write_file(tmp_path / 'in.csv', VALIDATE_SAMPLE)
    report = tmp_path / 'report.json'
    validate(
        input_files=[str(source)],
        schema_path=str(schema_file),
        report_file=str(report),
    )
    records = json.loads(report.read_text(encoding='utf-8'))
    assert len(records) == 5
    assert records[0]['file'] == str(source)
    assert records[0]['row_index'] == 1

def test_validate_ok_when_every_row_satisfies_the_schema(
    schema_file: Path, tmp_path: Path,
):
    """全行が適合すれば ok になる。"""
    source = write_file(
        tmp_path / 'in.csv', 'id,label,comment\n0001,positive,ok\n',
    )
    result = validate(
        input_files=[str(source)], schema_path=str(schema_file),
    )
    assert result.ok is True
    assert result.violations == []

def test_validate_rejects_unwritable_output_before_reading(
    schema_file: Path, tmp_path: Path,
):
    """書き出せない出力形式は、読み込みを始める前に弾かれる。"""
    source = write_file(tmp_path / 'in.csv', VALIDATE_SAMPLE)
    with pytest.raises(ValueError):
        validate(
            input_files=[str(source)],
            schema_path=str(schema_file),
            report_file=str(tmp_path / 'report.txt'),
        )

# --- validate: unique / max_length / unknown_columns ---------------------

def test_max_length_rule(tmp_path: Path):
    """文字数の上限を超えた値が違反として検出される。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  comment:\n    required: true\n    max_length: 5\n',
    )
    schema = load_schema(str(path))
    assert validate_row(Row.from_dict({'comment': '12345'}), schema) == []
    violations = validate_row(Row.from_dict({'comment': '123456'}), schema)
    assert [(v['column'], v['rule']) for v in violations] == \
        [('comment', 'max_length')]

@pytest.mark.parametrize('param', ['five', 3.5, True, -1])
def test_max_length_rejects_bad_parameter(tmp_path: Path, param):
    """max_length に整数以外を書いたらスキーマの誤りとして弾かれる。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        f'columns:\n  comment:\n    max_length: {param}\n',
    )
    with pytest.raises(SchemaError):
        load_schema(str(path))

def test_unique_reports_only_later_occurrences(tmp_path: Path):
    """
    重複のうち2件目以降を違反とする。

    最初の出現を適合とすることで、差し戻す対象が一意に決まる。
    """
    schema_path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    required: true\n    unique: true\n',
    )
    source = write_file(tmp_path / 'in.csv', 'id\n1\n2\n1\n1\n')
    report = tmp_path / 'report.json'
    result = validate(
        input_files=[str(source)],
        schema_path=str(schema_path),
        report_file=str(report),
    )
    assert result.num_invalid == 2
    records = json.loads(report.read_text(encoding='utf-8'))
    assert [r['row_index'] for r in records] == [2, 3]

def test_unique_spans_all_input_files_by_default(tmp_path: Path):
    """既定の unique は入力ファイル全体を通じて判定される。"""
    schema_path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    required: true\n    unique: true\n',
    )
    a = write_file(tmp_path / 'a.csv', 'id\n1\n2\n')
    b = write_file(tmp_path / 'b.csv', 'id\n2\n3\n')
    report = tmp_path / 'report.json'
    result = validate(
        input_files=[str(a), str(b)],
        schema_path=str(schema_path),
        report_file=str(report),
    )
    assert result.num_invalid == 1
    records = json.loads(report.read_text(encoding='utf-8'))
    assert records[0]['file'] == str(b)
    assert records[0]['actual'] == '2'

def test_unique_per_file_is_scoped_to_each_file(tmp_path: Path):
    """unique: per_file ではファイルをまたいだ重複を違反としない。"""
    schema_path = write_file(
        tmp_path / 'spec.yaml',
        "columns:\n  'no':\n    required: true\n    unique: per_file\n",
    )
    a = write_file(tmp_path / 'a.csv', 'no\n1\n2\n')
    b = write_file(tmp_path / 'b.csv', 'no\n1\n1\n')
    result = validate(
        input_files=[str(a), str(b)],
        schema_path=str(schema_path),
    )
    # NOTE: b.csv 内での重複1件のみ。a.csv との重複は対象外。
    assert result.num_invalid == 1

def test_unique_rejects_bad_scope(tmp_path: Path):
    """unique に未対応の値を書いたらスキーマの誤りとして弾かれる。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    unique: per_batch\n',
    )
    with pytest.raises(SchemaError):
        load_schema(str(path))

def test_unique_false_disables_the_check(tmp_path: Path):
    """unique: false は検査しないことを意味する。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    required: true\n    unique: false\n',
    )
    schema = load_schema(str(path))
    assert schema.columns[0].checks == []

def test_unknown_columns_warn_is_not_a_violation(tmp_path: Path):
    """既定の warn では、未定義の列があっても違反にはしない。"""
    schema_path = write_file(
        tmp_path / 'spec.yaml', 'columns:\n  id:\n    required: true\n',
    )
    source = write_file(tmp_path / 'in.csv', 'id,memo\n1,m\n2,m\n')
    result = validate(
        input_files=[str(source)], schema_path=str(schema_path),
    )
    assert result.ok is True
    assert result.unknown_columns == {'memo': 2}

def test_unknown_columns_error_is_a_violation(tmp_path: Path):
    """error では、未定義の列が違反として報告される。"""
    schema_path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    required: true\n'
        'unknown_columns: error\n',
    )
    source = write_file(tmp_path / 'in.csv', 'id,memo\n1,m\n')
    invalid = tmp_path / 'invalid.jsonl'
    result = validate(
        input_files=[str(source)],
        schema_path=str(schema_path),
        output_invalid=str(invalid),
    )
    assert result.ok is False
    assert read_jsonl(invalid)[0]['__violations__'] == [{
        'column': 'memo',
        'rule': 'unknown_column',
        'expected': 'a column defined in the schema',
        'actual': 'memo',
    }]

def test_unknown_columns_ignore_says_nothing(tmp_path: Path):
    """ignore では、未定義の列を記録すらしない。"""
    schema_path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    required: true\n'
        'unknown_columns: ignore\n',
    )
    source = write_file(tmp_path / 'in.csv', 'id,memo\n1,m\n')
    result = validate(
        input_files=[str(source)], schema_path=str(schema_path),
    )
    assert result.ok is True
    assert result.unknown_columns == {}

def test_unknown_columns_accepts_nested_children(tmp_path: Path):
    """スキーマが親を定義していれば、ネストした子は未定義扱いにしない。"""
    schema_path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  nested:\n    required: false\n'
        'unknown_columns: error\n',
    )
    source = write_file(
        tmp_path / 'in.jsonl', '{"nested": {"k": "v"}}\n',
    )
    result = validate(
        input_files=[str(source)], schema_path=str(schema_path),
    )
    assert result.ok is True

def test_schema_rejects_column_name_read_as_boolean(tmp_path: Path):
    """
    YAML が真偽値と解釈する列名は、黙って化けずにエラーになる。

    no / No / on / off などは引用符が無いと真偽値になり、
    さらに no と No は同じ False に潰れて定義が合流してしまう。
    """
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  no:\n    required: true\n',
    )
    with pytest.raises(SchemaError, match='boolean'):
        load_schema(str(path))

def test_schema_accepts_quoted_column_name(tmp_path: Path):
    """引用符を付ければ、そのままの列名として扱われる。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        "columns:\n  'no':\n    required: true\n",
    )
    schema = load_schema(str(path))
    assert [c.name for c in schema.columns] == ['no']

def test_unknown_columns_rejects_bad_mode(tmp_path: Path):
    """unknown_columns に未対応の値を書いたら弾かれる。"""
    path = write_file(
        tmp_path / 'spec.yaml',
        'columns:\n  id:\n    required: true\n'
        'unknown_columns: explode\n',
    )
    with pytest.raises(SchemaError):
        load_schema(str(path))

# --- データベースを入力とする (.dbq) -------------------------------------

def write_database(path: Path) -> Path:
    """テスト用の SQLite データベースを作成する。"""
    import sqlite3
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            'CREATE TABLE customers (id TEXT, name TEXT, status TEXT)'
        )
        connection.executemany(
            'INSERT INTO customers VALUES (?,?,?)',
            [
                ('0001', 'alice', 'active'),
                ('0002', 'bob', 'inactive'),
                ('0003', 'carol', 'active'),
            ],
        )
        connection.commit()
    finally:
        connection.close()
    return path

@pytest.fixture
def database(tmp_path: Path) -> Path:
    """顧客表を持つ SQLite データベース。"""
    return write_database(tmp_path / 'master.db')

def write_dbq(path: Path, url: str, query: str) -> Path:
    """.dbq ファイルを作成する。"""
    return write_file(path, f'url: {url}\nquery: |\n  {query}\n')

def test_dbq_is_read_like_any_other_input(database: Path, tmp_path: Path):
    """データベースの問い合わせ結果を、ファイルと同じように読み込める。"""
    source = write_dbq(
        tmp_path / 'master.dbq',
        f'sqlite:///{database}',
        "SELECT id, name FROM customers WHERE status = 'active'",
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert read_jsonl(output) == [
        {'id': '0001', 'name': 'alice'},
        {'id': '0003', 'name': 'carol'},
    ]

def test_dbq_can_be_the_base_of_a_merge(database: Path, tmp_path: Path):
    """マスタを merge の突合対象として直接指定できる。"""
    source = write_dbq(
        tmp_path / 'master.dbq',
        f'sqlite:///{database}',
        'SELECT id, name, status FROM customers',
    )
    corrections = write_file(
        tmp_path / 'fix.csv', 'id,status\n0002,active\n',
    )
    output = tmp_path / 'merged.jsonl'
    merge(
        previous_files=[str(source)],
        modification_files=[str(corrections)],
        keys=['id'],
        output_base_data_file=str(output),
    )
    rows = {row['id']: row for row in read_jsonl(output)}
    assert rows['0002']['status'] == 'active'
    assert rows['0001']['status'] == 'active'

def test_dbq_expands_environment_variables(
    database: Path, tmp_path: Path, monkeypatch,
):
    """URL 中の ${VAR} が環境変数で置換される。"""
    monkeypatch.setenv('TABPRO_TEST_DB_DIR', str(database.parent))
    source = write_dbq(
        tmp_path / 'master.dbq',
        'sqlite:///${TABPRO_TEST_DB_DIR}/master.db',
        'SELECT id FROM customers',
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert len(read_jsonl(output)) == 3

def test_dbq_rejects_undefined_environment_variable(tmp_path: Path):
    """
    未定義の環境変数はエラーになる。

    そのまま残すと、パスワードが '${VAR}' のまま接続を試みることになり、
    原因の分かりにくい認証失敗になる。
    """
    source = write_dbq(
        tmp_path / 'master.dbq',
        'sqlite:///${TABPRO_NOT_SET_ANYWHERE}/x.db',
        'SELECT 1',
    )
    with pytest.raises(QuerySpecError, match='Environment variable'):
        load_query_spec(str(source))

@pytest.mark.parametrize(
    'query',
    [
        'DELETE FROM customers',
        'UPDATE customers SET name = "x"',
        'DROP TABLE customers',
        'INSERT INTO customers VALUES (1, 2, 3)',
    ],
)
def test_dbq_rejects_write_queries(tmp_path: Path, query: str):
    """書き込みを行うクエリは入口で断られる。"""
    source = write_dbq(tmp_path / 'q.dbq', 'sqlite:///x.db', query)
    with pytest.raises(QuerySpecError, match='read-only'):
        load_query_spec(str(source))

def test_dbq_rejects_a_write_hidden_behind_a_comment(tmp_path: Path):
    """コメントで SELECT に見せかけた書き込みも断られる。"""
    source = write_file(
        tmp_path / 'q.dbq',
        'url: sqlite:///x.db\n'
        'query: |\n'
        '  -- SELECT\n'
        '  DROP TABLE customers\n',
    )
    with pytest.raises(QuerySpecError, match='read-only'):
        load_query_spec(str(source))

def test_dbq_rejects_multiple_statements(tmp_path: Path):
    """複数の命令を並べることはできない。"""
    source = write_dbq(
        tmp_path / 'q.dbq', 'sqlite:///x.db',
        'SELECT 1; DROP TABLE customers',
    )
    with pytest.raises(QuerySpecError, match='single statement'):
        load_query_spec(str(source))

def test_dbq_allows_with_clause(database: Path, tmp_path: Path):
    """WITH で始まるクエリは読み取りとして許される。"""
    source = write_dbq(
        tmp_path / 'q.dbq', f'sqlite:///{database}',
        'WITH a AS (SELECT * FROM customers) SELECT id FROM a',
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert len(read_jsonl(output)) == 3

def test_dbq_allows_a_trailing_semicolon(database: Path, tmp_path: Path):
    """末尾のセミコロンは許される。"""
    source = write_dbq(
        tmp_path / 'q.dbq', f'sqlite:///{database}',
        'SELECT id FROM customers;',
    )
    output = tmp_path / 'out.jsonl'
    convert(input_files=[str(source)], output_file=str(output))
    assert len(read_jsonl(output)) == 3

@pytest.mark.parametrize(
    'content',
    [
        'url: sqlite:///x.db\n',
        'query: SELECT 1\n',
        'just a string\n',
        'url: 1\nquery: SELECT 1\n',
    ],
)
def test_dbq_rejects_incomplete_specifications(tmp_path: Path, content: str):
    """url と query が揃っていない .dbq は拒否される。"""
    source = write_file(tmp_path / 'q.dbq', content)
    with pytest.raises(QuerySpecError):
        load_query_spec(str(source))

def test_dbq_missing_database_file(tmp_path: Path):
    """存在しないデータベースは、そう分かる形で失敗する。"""
    source = write_dbq(
        tmp_path / 'q.dbq', f'sqlite:///{tmp_path}/nosuch.db', 'SELECT 1',
    )
    with pytest.raises(FileNotFoundError):
        convert(
            input_files=[str(source)],
            output_file=str(tmp_path / 'out.jsonl'),
        )

def test_dbq_masks_credentials_in_the_url():
    """ログに出す URL から認証情報が伏せられる。"""
    assert mask_url('postgresql://user:secret@host:5432/db') == \
        'postgresql://***@host:5432/db'
    assert mask_url('sqlite:///master.db') == 'sqlite:///master.db'

def test_dbq_reports_a_missing_sqlalchemy(tmp_path: Path, monkeypatch):
    """SQLAlchemy が無い場合、何を入れればよいかが示される。"""
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == 'sqlalchemy':
            raise ImportError('no sqlalchemy')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', fake_import)
    with pytest.raises(QuerySpecError, match=r'tabpro\[sql\]'):
        list(iter_sqlalchemy_rows('postgresql://host/db', 'SELECT 1'))

def test_sqlalchemy_path_reads_the_same_rows(database: Path):
    """
    SQLAlchemy 経路も同じ結果を返す。

    sqlite URL を用いることで、サーバーを立てずに検証する。
    """
    pytest.importorskip('sqlalchemy')
    results = [
        (columns, list(rows))
        for columns, rows in iter_sqlalchemy_rows(
            f'sqlite:///{database}', 'SELECT id, name FROM customers',
        )
    ]
    assert results[0][0] == ['id', 'name']
    assert results[0][1][0] == ('0001', 'alice')

# --- 複数値オプションの繰り返し指定 -------------------------------------

def build_parser(setup_parser) -> argparse.ArgumentParser:
    """テスト用に各コマンドのパーサーを組み立てる。"""
    parser = argparse.ArgumentParser()
    setup_parser(parser)
    return parser

def test_repeated_do_actions_accumulate():
    """
    回帰テスト: --do を複数回書いても先の指定が捨てられないこと。

    argparse の既定 (store) では後の指定が前を上書きするため、
    警告も出ないまま最初のアクションだけが無視されていた。
    """
    parser = build_parser(setup_convert_parser)
    args = parser.parse_args([
        'in.csv',
        '--do', 'cast:score=score:as=int',
        '--do', 'filter:name==bob',
    ])
    assert args.do_actions == [
        'cast:score=score:as=int',
        'filter:name==bob',
    ]

def test_single_do_with_multiple_values_still_works():
    """1つの --do に複数値を並べる従来の書き方も変わらず動く。"""
    parser = build_parser(setup_convert_parser)
    args = parser.parse_args([
        'in.csv',
        '--do', 'cast:score=score:as=int', 'filter:name==bob',
    ])
    assert args.do_actions == [
        'cast:score=score:as=int',
        'filter:name==bob',
    ]

def test_unspecified_multi_value_option_stays_none():
    """指定しなければ従来どおり None のままであること。"""
    parser = build_parser(setup_convert_parser)
    args = parser.parse_args(['in.csv'])
    assert args.do_actions is None
    assert args.pick_columns is None

def test_repeated_pick_columns_accumulate():
    """--pick も繰り返し指定が累積される。"""
    parser = build_parser(setup_convert_parser)
    args = parser.parse_args(['in.csv', '--pick', 'id', '--pick', 'name'])
    assert args.pick_columns == ['id', 'name']

@pytest.mark.parametrize(
    'setup_parser, base_args, option, dest',
    [
        (setup_sort_parser, ['in.csv'], '--sort-keys', 'sort_keys'),
        (setup_aggregate_parser, ['in.csv'], '--keys-to-expand', 'keys_to_expand'),
        (
            setup_compare_parser,
            ['a.csv', 'b.csv', '--query', 'id'],
            '--compare-keys',
            'compare_keys',
        ),
        (
            setup_merge_parser,
            ['--previous', 'a.csv', '--new', 'b.csv', '--keys', 'id'],
            '--merge-fields',
            'merge_fields',
        ),
    ],
)
def test_repeated_options_accumulate_for_every_command(
    setup_parser, base_args: list[str], option: str, dest: str,
):
    """各コマンドの複数値オプションが一様に累積されること。"""
    parser = build_parser(setup_parser)
    args = parser.parse_args(base_args + [option, 'a', option, 'b'])
    assert getattr(args, dest) == ['a', 'b']

# --- コンソールスクリプト -----------------------------------------------

def test_console_script_help():
    '''インストール済みであれば tabpro --help が正常終了する。'''
    executable = shutil.which('tabpro')
    if executable is None:
        pytest.skip('tabpro コマンドがインストールされていない')
    completed = subprocess.run(
        [executable, '--help'],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert 'convert' in completed.stdout
