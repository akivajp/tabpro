import json

from rich.console import Console

from . manage_loaders import (
    Row,
    register_loader,
    detect_text_encoding,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

from ... progress import (
    Progress,
)

from . io_json import loads_json

@register_loader('.jsonl')
def load_jsonl(
    input_file: str,
    progress: Progress | None = None,
    limit: int | None = None,
    **kwargs,
):
    orig_progress = progress
    quiet = kwargs.get('quiet', False)
    # NOTE:
    #   encoding の明示指定が無い場合は CSV/TSV と同じ自動判定に任せる
    #   (utf-8-sig は BOM 無しの UTF-8 も読める)。明示指定時は厳格に扱う。
    encoding = kwargs.get('encoding')
    if encoding is None:
        encoding = detect_text_encoding(
            input_file,
            console=progress.console if progress else Console(),
            quiet=quiet,
        )
    if progress is None:
        progress = Progress()
        progress.start()
    open_task_id = None
    if not quiet:
        progress.console.log('Loading from: ', input_file)
        description = 'Reading JSONL file'
        open_task_id = progress.add_task(
            description = description,
        )
        def fn_open(file, *args, **kwargs):
            return progress.open(
                file,
                *args,
                task_id = open_task_id,
                **kwargs,
            )
    else:
        fn_open = open
    if not quiet:
        count_task_id = progress.add_task(
            description = 'Loaded JSON rows',
            total = limit,
            disable = quiet,
        )
    with fn_open(input_file, 'r', encoding=encoding) as f:
        # NOTE:
        #   空行 (空白のみの行を含む) はデータを運ばないため、
        #   以前は json のパースエラーで変換全体が止まっていた。
        #   スキップして続行し、行番号を記録して読み込み完了後に警告する。
        list_skipped_blank_line_indices: list[int] = []
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            if not line.strip():
                list_skipped_blank_line_indices.append(i + 1)
                continue
            row = loads_json(line)
            if not quiet:
                progress.update(count_task_id, advance=1)
            yield Row.from_dict(row)
        f.close()
        if open_task_id is not None:
            progress.stop_task(open_task_id)
        if not quiet:
            progress.stop_task(count_task_id)
        if list_skipped_blank_line_indices:
            # NOTE:
            #   異常データの検知が目的のため、黙って捨てずに
            #   何行スキップしたかを報告する。行数が少なければ
            #   行番号も併記する (1始まり)。
            detail = ''
            if len(list_skipped_blank_line_indices) <= 10:
                shown = ', '.join(
                    str(index) for index in list_skipped_blank_line_indices
                )
                detail = f' (line {shown})'
            progress.console.log(
                f'[yellow]warning: {len(list_skipped_blank_line_indices)} '
                f'blank line(s) were skipped in {input_file}{detail}[/yellow]'
            )
    if orig_progress is None:
        progress.stop()

@register_writer('.jsonl')
class JsonLinesWriter(BaseWriter):
    def __init__(
        self,
        output_file: str,
        **kwargs,
    ):
        super().__init__(output_file, **kwargs)

    def support_streaming(self):
        return True

    def _write_row(self, row: Row):
        if not self.fobj:
            self._open()
            assert self.fobj is not None
        self.fobj.write(json.dumps(row.nested, ensure_ascii=False))
        self.fobj.write('\n')

    def _write_all_rows(self):
        if self.rows:
            for row in self.rows:
                self._write_row(row)
        if self.fobj:
            self.fobj.close()
