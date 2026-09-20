import os.path
import pandas as pd

from typing import Callable, TypeAlias


from ... progress import Progress

from ..writer import BaseWriter

# NOTE:
#   PEP 695 の `type` 文は Python 3.12 以降でしか使えないため、
#   3.10 以降をサポートするために TypeAlias による定義を用いる。
Saver: TypeAlias = Callable[[pd.DataFrame, str], None]

dict_writers: dict[str, type[BaseWriter]] = {}

# NOTE:
#   import 順の制約によりここで読み込む必要がある (Row を定義前に参照できない)。
from ...classes.row import Row  # noqa: E402

def register_writer(
    ext: str,
):
    def decorator(writer: type[BaseWriter]):
        dict_writers[ext] = writer
        return writer
    return decorator

def check_writer(
    output_file: str,
):
    ext = os.path.splitext(output_file)[1]
    if ext not in dict_writers:
        # NOTE:
        #   読み込めるのに書き出せない形式 (.xls など) は、
        #   単に未対応と言われるより理由が分かるほうがよい。
        from . manage_loaders import dict_loaders
        if ext in dict_loaders:
            raise ValueError(
                f'{ext} files can be read but not written. '
                f'Supported output types: {sorted(dict_writers)}'
            )
        # NOTE:
        #   以前は拡張子だけを表示しており、拡張子の無いパスを指定すると
        #   "Unsupported file type: " と空欄になり、どのファイルで
        #   問題が起きたか分からなかった。パスを先に示す。
        raise ValueError(
            f'Unsupported output file type: {output_file} '
            f'(extension: {ext or "none"}). '
            f'Supported output types: {sorted(dict_writers)}'
        )
    writer_class = dict_writers[ext]
    return writer_class

def get_writer(
    output_file: str,
    progress: Progress | None = None,
) -> BaseWriter:
    # NOTE:
    #   ディレクトリを指定すると拡張子判定に到達して
    #   "Unsupported file type: " (空の拡張子) となり、
    #   真の原因 (パスがディレクトリであること) が伝わらないため、
    #   先に弾いて分かりやすいエラーにする。
    if os.path.isdir(output_file):
        raise ValueError(f'output path is a directory: {output_file}')
    writer_class = check_writer(output_file)
    return writer_class(
        output_file,
        progress=progress,
    )

def save(
    rows: list[Row],
    output_file: str,
    progress: Progress | None = None,
):
    writer = get_writer(output_file, progress=progress)
    writer.push_rows(rows)
    writer.close()

def raise_error_if_output_overlaps(
    outputs: list[tuple[str | None, str]],
    input_files: list[str] | None = None,
):
    '''
    出力先どうし、および出力先と入力ファイルのパス重複を検出してエラーにする。

    NOTE:
        streaming writer は構築時 (open('w')) に対象ファイルを truncate する。
        入力と同じパスに出力すると、入力の読み込みと出力の truncate が競合し、
        入力ファイルが空に化ける (データ消失)。また出力先同士が同じパスだと、
        後から開かれた writer が先に開かれた writer の出力を破壊する。
        全行を読み込んでから書くコマンド (sort / aggregate など) では
        入力との重複が起きないが、出力先同士の重複は同じ事故になるため、
        両方の検出をこの関数にまとめて convert / validate / merge で共用する。

    Args:
        outputs: (出力パス, ラベル) のリスト。パスが None の項目は無視する。
        input_files: 入力ファイルのリスト (None なら入力との比較を行わない)。

    Raises:
        ValueError: 出力先どうしが同一、または出力先が入力と同一の場合。
    '''
    # NOTE:
    #   abspath の一致で未作成の出力先も比較し、加えて既存ファイル同士は
    #   samefile でシンボリックリンクやハードリンク経由の同一性も検出する。
    def is_same(candidate: str, other: str) -> bool:
        return (
            os.path.abspath(candidate) == os.path.abspath(other)
            or (
                os.path.exists(candidate)
                and os.path.exists(other)
                and os.path.samefile(candidate, other)
            )
        )

    non_none_outputs = [
        (path, label) for path, label in outputs if path
    ]
    # 出力先どうしの重複 (後から開かれた writer が先の出力を破壊する)
    for index_a in range(len(non_none_outputs)):
        for index_b in range(index_a + 1, len(non_none_outputs)):
            path_a, label_a = non_none_outputs[index_a]
            path_b, label_b = non_none_outputs[index_b]
            if is_same(path_a, path_b):
                raise ValueError(
                    f'{label_a} {path_a} and {label_b} {path_b} are the '
                    f'same path. Opening both would destroy the output '
                    f'written by the first writer. Specify different '
                    f'output paths.'
                )
    if not input_files:
        return
    # 出力先と入力の重複 (入力を読み切る前に truncate して入力が空に化ける)
    for candidate, label in non_none_outputs:
        for input_file in input_files:
            if is_same(candidate, input_file):
                raise ValueError(
                    f'{label} {candidate} is the same as input file '
                    f'{input_file}. Writing it would destroy the input '
                    f'before it is fully read. Specify a different '
                    f'output path.'
                )
