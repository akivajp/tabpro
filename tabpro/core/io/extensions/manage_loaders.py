import codecs
import os
from typing import Any, Generator, Protocol

from rich.console import Console

from ...classes.row import Row

# NOTE:
#   エンコーディングを明示しない場合の自動判定候補。
#   utf-8-sig は BOM 無しの UTF-8 もそのまま読めるため既定の第一候補。
#   utf-8-sig で失敗した場合は日本語環境で多い cp932 (Shift-JIS)、
#   euc-jp の順に試す。
FALLBACK_CANDIDATE_ENCODINGS = ['utf-8-sig', 'cp932', 'euc-jp']

def detect_text_encoding(
    input_file: str,
    console: Console | None = None,
    quiet: bool = False,
    candidates: list[str] = FALLBACK_CANDIDATE_ENCODINGS,
) -> str:
    '''
    入力ファイルを実際にデコードし、使えるエンコーディングを返す。

    NOTE:
        ファイル全体をチャンク単位でデコード検査するため、行データは
        メモリに保持しない。エンコーディングを明示指定した場合
        (--encoding) はこの関数を経由せず厳格に扱うため、
        この自動判定は既定の読み込みでのみ行われる。

    Args:
        input_file: 検査対象のファイルパス。
        console: 警告の出力先。
        quiet: 警告を抑制するかどうか。
        candidates: 試行するエンコーディングの候補 (先頭が既定)。

    Returns:
        ファイル全体をデコードできた最初のエンコーディング名。

    Raises:
        ValueError: どの候補でもデコードできなかった場合。
    '''
    for encoding in candidates:
        try:
            # NOTE:
            #   open(text mode) でなくバイナリ読み込み + incremental
            #   decoder を使うことで、巨大ファイルでもメモリを
            #   消費せずに検査できる。
            with open(input_file, 'rb') as f:
                decoder = codecs.getincrementaldecoder(encoding)()
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        decoder.decode(b'', True)
                        break
                    decoder.decode(chunk)
            if (
                encoding != candidates[0]
                and not quiet
                and console is not None
            ):
                console.log(
                    f'[yellow]warning: {input_file} is not decodable '
                    f'as {candidates[0]}; reading it as {encoding}[/yellow]'
                )
            return encoding
        except UnicodeDecodeError:
            continue
    raise ValueError(
        f'{input_file} could not be decoded with any candidate encoding '
        f'({", ".join(candidates)}). If you know the encoding, '
        f'specify it with --encoding (e.g. cp932)'
    )

class LoaderType(Protocol):
    def __call__(self, input_file: str, **kwargs: Any) -> Generator[Row, None, None]:
         ...

dict_loaders: dict[str, LoaderType] = {}
def register_loader(
    ext: str,
):
    def decorator(loader):
        dict_loaders[ext] = loader
        return loader
    return decorator

def get_loader(
    input_file: str,
):
    ext = os.path.splitext(input_file)[1]
    if ext not in dict_loaders:
        raise ValueError(f'Unsupported file type: {ext}')
    loader = dict_loaders[ext]
    return loader

def load(
    input_file: str,
    **kwargs,
):
    loader = get_loader(input_file)
    return loader(input_file, **kwargs)
