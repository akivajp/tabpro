# -*- coding: utf-8 -*-
'''
各コマンド (convert / sort / compare / merge / aggregate) の回帰テスト。

特に、過去に「無言で何もしない」「無言でデータを破損させる」挙動をしていた
経路を重点的に検証する。
'''

import csv
import json
import shutil
import subprocess

from pathlib import Path

import pytest

from tabpro.core.aggregate import aggregate
from tabpro.core.compare import compare
from tabpro.core.convert import convert
from tabpro.core.merge import merge
from tabpro.core.sort import sort

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
