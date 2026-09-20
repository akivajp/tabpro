# -*- coding: utf-8 -*-

import os
import sys

# 3-rd party modules


from . progress import Progress

# local

from .. logging import logger

from . config import (
    setup_config,
    setup_pick_with_args,
)
from . constants import (
    FILE_FIELD,
    ROW_INDEX_FIELD,
    FILE_ROW_INDEX_FIELD,
    INPUT_FIELD,
)

from .actions import (
    do_actions,
    remap_columns,
    setup_actions_with_args,
)

from .actions.types import (
    GlobalStatus,
)

from . io import (
    get_loader,
    get_writer,
    raise_error_if_output_overlaps,
)

from . console.views import Panel

from . classes.row import Row

def raise_error_if_output_overlaps_input(
    input_files: list[str],
    output_file: str | None,
    output_file_filtered_out: str | None,
):
    '''
    出力先が入力ファイル、あるいは他の出力先と同じパスであればエラーにする。

    NOTE:
        convert は入力をストリーミング処理しながら出力を書くため、
        出力先が入力と同じパスだと出力の truncate が入力の読み込みと
        競合し、入力ファイルが空に化ける (データ消失)。
        出力先同士が同じパスでも、先に開かれた writer の出力が
        後から開かれた writer に破壊される。
        検出処理の実体は io.raise_error_if_output_overlaps にあり、
        validate / merge でも共用している。

    Raises:
        ValueError: 出力先がいずれかの入力ファイル、あるいは
            他の出力先と同一の場合。
    '''
    raise_error_if_output_overlaps(
        outputs=[
            (output_file, 'output file'),
            (output_file_filtered_out, 'filtered-out output file'),
        ],
        input_files=input_files,
    )

def convert(
    input_files: list[str],
    output_file: str | None = None,
    output_file_filtered_out: str | None = None,
    config_path: str | None = None,
    output_debug: bool = False,
    list_actions: list[str] | None = None,
    list_pick_columns: list[str] | None = None,
    action_delimiter: str = ':',
    verbose: bool = False,
    ignore_file_rows: list[str] | None = None,
    no_header: bool = False,
    sheet: str | None = None,
    all_sheets: bool = False,
    limit: int | None = None,
    encoding: str | None = None,
    no_warnings: bool = False,
):
    #console = Console()
    progress = Progress(
        #console = console,
        redirect_stdout = False,
    )
    progress.start()
    console = progress.console
    logger.info('input_files: %s', input_files)
    row_list_filtered_out = []
    set_ignore_file_rows = set()
    # NOTE:
    #   実際にどの行の除外に使われたかを記録する。
    #   --ignore の指定にタイポがあると1行も一致せず、除外したつもりの
    #   行が黙って納品物に残るため、未一致のエントリを最後に警告する。
    set_matched_ignore_file_rows = set()
    global_status = GlobalStatus()
    # NOTE:
    #   出力先が入力と同じパスだと入力が空に化けるため、
    #   何よりも先にチェックする。
    raise_error_if_output_overlaps_input(
        input_files,
        output_file,
        output_file_filtered_out,
    )
    config = setup_config(
        config_path,
        console=console,
        no_warnings=no_warnings,
    )
    #console.log('config: ', config)
    if ignore_file_rows:
        set_ignore_file_rows = set(ignore_file_rows)
    if list_pick_columns:
        setup_pick_with_args(config, list_pick_columns)
    if list_actions:
        setup_actions_with_args(
            config,
            list_actions,
            action_delimiter=action_delimiter,
        )
    writer = None
    if output_file:
        writer = get_writer(
            output_file,
            progress=progress,
        )
    num_stacked_rows = 0
    # NOTE:
    #   --pick に存在しない列を指定すると、以前は黙って出力が欠けていた
    #   (タイポに気づかないまま納品物ができる)。ただし複数ファイル連結時は
    #   「一部のファイルにしか無い列」を寛容に扱う意図もあるため、行ごとに
    #   ではなく、1つのファイル内で1行も出現しなかった列をファイル単位で
    #   1回だけ警告する。
    list_picked_sources = [
        pick_config.source for pick_config in config.pick
    ]
    dict_unpicked: dict[str, set[str]] = {}
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        base_name = os.path.basename(input_file)
        loader = get_loader(
            input_file,
            no_header=no_header,
            progress=progress,
            sheet=sheet,
            all_sheets=all_sheets,
            limit=limit,
            encoding=encoding,
        )
        # NOTE: ファイルごとに「1行も出現しなかった picked 列」を集める
        set_unfound_picked_sources = set(list_picked_sources)
        num_rows_in_file = 0
        for index, row in enumerate(loader):
            file_row_index = f'{input_file}:{index}'
            if file_row_index in set_ignore_file_rows:
                set_matched_ignore_file_rows.add(file_row_index)
                continue
            short_file_row_index = f'{base_name}:{index}'
            if short_file_row_index in set_ignore_file_rows:
                set_matched_ignore_file_rows.add(short_file_row_index)
                continue
            # NOTE:
            #   入力値の記録には、ローダーが付与した由来情報 (__sheet__ など) を
            #   含めない。clone() では staging ごと複製されてしまい、
            #   __input__ や __values__ にシート名が混入する。
            orig_row = Row()
            for column in row.keys():
                orig_row[column] = row[column]
            # NOTE:
            #   既に由来情報を持つ行は上書きしない (中間ファイルを何段経由しても
            #   最初の入力ファイルの何行目かを保つため)。
            #   判定には __file__ の有無を用いる。__staging__ 全体の有無で見ると、
            #   ローダーが付与した __sheet__ だけで由来情報が設定されなくなる。
            if FILE_FIELD not in row.staging:
                row.staging[FILE_FIELD] = input_file
                row.staging[FILE_ROW_INDEX_FIELD] = file_row_index
                row.staging[ROW_INDEX_FIELD] = index
                row.staging[INPUT_FIELD] = orig_row.nested
                if loader.extension in ['.csv', '.tsv', '.xls', '.xlsx'] and not no_header:
                    for key_index, (key, value) in enumerate(orig_row.flat.items()):
                        row.staging[f'{INPUT_FIELD}.__values__.{key_index}'] = value
            if config.actions:
                try:
                    new_row = do_actions(global_status, row, config.actions)
                    if new_row is None:
                        if not output_debug:
                            row.pop_staging()
                        if verbose:
                            console.log('filtered out: ', row.flat)
                        if output_file_filtered_out:
                            # NOTE:
                            #   以前は row.flat (単なる OrderedDict) を
                            #   追加していたため、書き出し時に Writer が
                            #   期待する Row の属性が無く AttributeError に
                            #   なっていた。Row のまま渡す。
                            row_list_filtered_out.append(row)
                        continue
                    row = new_row
                except Exception as e:
                    if verbose:
                        #console.log('error in row index: ', index)
                        logger.error('error in row index: ', index)
                    raise e
            if config.pick:
                # NOTE:
                #   remap と同じ row.search() で列の存否を調べる
                #   (staging 側にしか無い列も拾える)。
                #   全て見つかった後は set が空になるため、走査は止まる。
                if set_unfound_picked_sources:
                    for picked_source in list(set_unfound_picked_sources):
                        _, found = row.search(picked_source)
                        if found:
                            set_unfound_picked_sources.discard(picked_source)
                row = remap_columns(row, config.pick)
            if writer is None:
                if sys.stdout.isatty():
                    if num_stacked_rows == 0:
                        console.print(
                            Panel(
                                row.nested,
                                title='First Row',
                            )
                        )
            if not output_debug:
                row.pop_staging()
            if writer:
                try:
                    writer.push_row(row)
                except ValueError as e:
                    # NOTE:
                    #   csv.DictWriter の素のメッセージ
                    #   ("dict contains fields not in fieldnames: ...")
                    #   には行の出自が無いため、異常データの特定に
                    #   一手間必要だった。ここでなら入力ファイルと
                    #   行番号が分かるので、メッセージに付け足す。
                    #   (JSON のような全行溜め込み型の writer は
                    #   close 時に書き出すため対象外)
                    raise ValueError(
                        f'{e} (while writing input row {index + 1} '
                        f'of {input_file})'
                    ) from e
            else:
                pass
            num_stacked_rows += 1
            num_rows_in_file += 1
        # NOTE:
        #   このファイルで1行も出現しなかった picked 列を記録する。
        #   空入力 (行が1件も無い) の場合は全ての picked 列がここに残るが、
        #   出力も空なので警告しない。
        if config.pick and num_rows_in_file > 0 and set_unfound_picked_sources:
            dict_unpicked[input_file] = set_unfound_picked_sources
    console.log('total processed input rows: ', num_stacked_rows)
    if dict_unpicked and not no_warnings:
        for unpicked_input_file, unfound in dict_unpicked.items():
            # NOTE:
            #   以前は存在しない picked 列 (タイポ) が黙って無視され、
            #   出力の列が静かに欠けていた。
            console.log(
                f'[yellow]warning: --pick column(s) {sorted(unfound)} '
                f'were not found in any row of {unpicked_input_file} '
                f'and were omitted from the output.[/yellow]'
            )
    set_unmatched_ignore_file_rows = set_ignore_file_rows - set_matched_ignore_file_rows
    if set_unmatched_ignore_file_rows and not no_warnings:
        # NOTE:
        #   以前は一致しない --ignore 指定 (ファイル名や行番号のタイポ) が
        #   黙って無視され、除外したつもりの行が納品物に残っていた。
        console.log(
            f'[yellow]warning: {len(set_unmatched_ignore_file_rows)} '
            f'--ignore entries matched no row and were not applied: '
            f'{sorted(set_unmatched_ignore_file_rows)}[/yellow]'
        )
    if writer:
        writer.close()
    if output_file_filtered_out:
        # NOTE:
        #   除外行が 0 件でも空のファイルを出力する。
        #   以前は行が 1 件も除外されないとファイル自体を作らなかったため、
        #   ファイルの存在を前提にする後続のパイプラインが
        #   「除外が多かった日にだけ落ちる」不安定な挙動になっていた。
        console.log('saving filtered out to: ', output_file_filtered_out)
        writer = get_writer(
            output_file_filtered_out,
            progress=progress,
        )
        writer.push_rows(row_list_filtered_out)
        writer.close()
    progress.stop()
