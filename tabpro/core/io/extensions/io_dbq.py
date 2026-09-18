# -*- coding: utf-8 -*-
'''
データベースへの問い合わせ結果を入力として読み込む。

接続情報とクエリはコマンドラインではなくファイル (.dbq) に記述する。
コマンドラインに URL を書くと、認証情報がシェルの履歴や
`ps` の出力に残ってしまうため。
'''

import os
import re
import sqlite3

from collections import OrderedDict
from typing import (
    Any,
    Generator,
    Iterator,
)

import yaml

from rich.console import Console

from ... classes.row import Row
from ... progress import Progress

from . manage_loaders import register_loader

# 1回の取得でカーソルから読み出す行数
FETCH_SIZE = 1000

class QuerySpecError(ValueError):
    '''.dbq ファイルの記述に誤りがある場合に送出される。'''

regex_variable = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')

def expand_variables(
    value: str,
) -> str:
    '''
    文字列中の ${VAR} を環境変数で置換する。

    未定義の変数は明示的なエラーとする。そのまま残すと、
    パスワードが文字列 '${DB_PASSWORD}' のまま接続を試みることになり、
    原因の分かりにくい認証失敗になるため。

    Args:
        value: 置換対象の文字列。

    Returns:
        置換後の文字列。

    Raises:
        QuerySpecError: 未定義の環境変数が参照されている場合。
    '''
    def replace(matched: re.Match) -> str:
        name = matched.group(1)
        if name not in os.environ:
            raise QuerySpecError(
                f'Environment variable is not set: {name}'
            )
        return os.environ[name]
    return regex_variable.sub(replace, value)

def strip_sql_comments(
    query: str,
) -> str:
    '''SQL からコメントを取り除く (先頭の命令を判定するため)。'''
    without_block = re.sub(r'/\*.*?\*/', ' ', query, flags=re.S)
    without_line = re.sub(r'--[^\n]*', ' ', without_block)
    return without_line.strip()

def ensure_read_only(
    query: str,
) -> None:
    '''
    クエリが読み取り専用の単一命令であることを確かめる。

    書き込みを行う手段として使えてしまうと、設定ファイルの誤りが
    データの破壊に直結するため、入口で断る。

    Args:
        query: 検査するクエリ。

    Raises:
        QuerySpecError: SELECT/WITH 以外、または複数の命令を含む場合。
    '''
    stripped = strip_sql_comments(query)
    if not stripped:
        raise QuerySpecError('Query is empty.')
    head = stripped.split(None, 1)[0].lower()
    if head not in ['select', 'with']:
        raise QuerySpecError(
            f'Only read-only queries are allowed, but the query starts with '
            f'{head.upper()!r}. It must start with SELECT or WITH.'
        )
    # NOTE: 末尾のセミコロンは許すが、その後に続く命令は許さない
    body = stripped.rstrip().rstrip(';')
    if ';' in body:
        raise QuerySpecError(
            'Only a single statement is allowed; the query contains ";".'
        )

def load_query_spec(
    input_file: str,
) -> tuple[str, str]:
    '''
    .dbq ファイルを読み込み、接続 URL とクエリを返す。

    Args:
        input_file: .dbq ファイルのパス。

    Returns:
        (接続 URL, クエリ) の組。

    Raises:
        QuerySpecError: 記述に誤りがある場合。
    '''
    with open(input_file, 'r', encoding='utf-8') as f:
        loaded = yaml.safe_load(f)
    if not isinstance(loaded, dict):
        raise QuerySpecError(
            f'{input_file} must contain a mapping with "url" and "query".'
        )
    for key in ['url', 'query']:
        if key not in loaded:
            raise QuerySpecError(f'{key!r} is required in {input_file}.')
        if not isinstance(loaded[key], str):
            raise QuerySpecError(f'{key!r} must be a string in {input_file}.')
    url = expand_variables(loaded['url']).strip()
    query = expand_variables(loaded['query']).strip()
    ensure_read_only(query)
    return url, query

def iter_sqlite_rows(
    url: str,
    query: str,
) -> Generator[tuple[list[str], Iterator], None, None]:
    '''
    SQLite に接続してクエリを実行する。

    標準ライブラリだけで完結するため、追加の依存を必要としない。
    接続は読み取り専用で開く。

    Args:
        url: sqlite:// で始まる接続 URL。
        query: 実行するクエリ。

    Yields:
        (列名のリスト, 行のイテレータ) の組を1度だけ。
    '''
    # NOTE: sqlite:///path/to.db または sqlite:////abs/path.db
    path = url[len('sqlite://'):]
    if path.startswith('/'):
        path = path[1:]
    if not path:
        raise QuerySpecError(f'No database path in url: {url}')
    if not os.path.exists(path):
        raise FileNotFoundError(f'Database file not found: {path}')
    # NOTE: mode=ro で開くことで、書き込みを SQLite 側でも拒否させる
    uri = f'file:{path}?mode=ro'
    connection = sqlite3.connect(uri, uri=True)
    try:
        cursor = connection.execute(query)
        columns = [description[0] for description in cursor.description]
        yield columns, iter_cursor(cursor)
    finally:
        connection.close()

def iter_cursor(
    cursor: Any,
) -> Iterator:
    '''カーソルから一定件数ずつ読み出す (全件をメモリに載せないため)。'''
    while True:
        rows = cursor.fetchmany(FETCH_SIZE)
        if not rows:
            break
        for row in rows:
            yield row

def iter_sqlalchemy_rows(
    url: str,
    query: str,
) -> Generator[tuple[list[str], Iterator], None, None]:
    '''
    SQLAlchemy を用いて任意のデータベースに接続する。

    SQLAlchemy は規模の大きい依存であり、SQLite だけで足りる場合も多いため、
    必須依存ではなく optional extra としている。

    Args:
        url: SQLAlchemy 形式の接続 URL。
        query: 実行するクエリ。

    Yields:
        (列名のリスト, 行のイテレータ) の組を1度だけ。

    Raises:
        QuerySpecError: SQLAlchemy が導入されていない場合。
    '''
    try:
        import sqlalchemy
    except ImportError:
        raise QuerySpecError(
            f'Reading from {url.split("://", 1)[0]} requires SQLAlchemy, '
            'which is not installed. Install it with: pip install "tabpro[sql]" '
            '(sqlite:// URLs need no extra installation).'
        )
    engine = sqlalchemy.create_engine(url)
    try:
        with engine.connect() as connection:
            result = connection.execution_options(
                stream_results=True,
            ).execute(sqlalchemy.text(query))
            columns = list(result.keys())
            yield columns, iter_cursor(result)
    finally:
        engine.dispose()

@register_loader('.dbq')
def load_dbq(
    input_file: str,
    progress: Progress | None = None,
    limit: int | None = None,
    **kwargs,
) -> Generator[Row, None, None]:
    '''
    .dbq ファイルに記された問い合わせの結果を読み込む。

    Args:
        input_file: .dbq ファイルのパス。
        progress: 進捗表示に用いる Progress。
        limit: 読み込む行数の上限。

    Yields:
        1行分の Row。
    '''
    quiet = kwargs.get('quiet', False)
    console = progress.console if progress else Console()
    url, query = load_query_spec(input_file)
    if not quiet:
        # NOTE: URL には認証情報が含まれうるため、そのままは表示しない
        console.log('querying database: ', mask_url(url))
    if url.startswith('sqlite://'):
        iterator = iter_sqlite_rows(url, query)
    else:
        iterator = iter_sqlalchemy_rows(url, query)
    task_id = None
    for columns, rows in iterator:
        if not quiet and progress is not None:
            task_id = progress.add_task('Loading rows...')
        for index, values in enumerate(rows):
            if limit is not None and index >= limit:
                break
            record = OrderedDict(zip(columns, values))
            if task_id is not None and progress is not None:
                progress.update(task_id, advance=1)
            yield Row.from_dict(record)
        if task_id is not None and progress is not None:
            progress.stop_task(task_id)

def mask_url(
    url: str,
) -> str:
    '''接続 URL から認証情報を伏せた表示用の文字列を作る。'''
    return re.sub(r'://[^/@]*@', '://***@', url)
