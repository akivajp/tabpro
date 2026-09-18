import sys

from typing import (
    Iterable,
    Sized,
    TypeVar,
)

from rich import progress
from rich.console import Console
from rich.progress import (
    TaskID,
    track as base_track,
)

T = TypeVar("T")

class Progress(progress.Progress):
    def __init__(
        self,
        *args,
        console: Console | None = None,
        **kwargs,
    ):
        if console is None:
            if sys.stdout.isatty():
                console = Console()
            else:
                console = Console(
                    stderr=True,
                    #force_terminal = True,
                    #force_interactive = False,
                )
        super().__init__(
            *args,
            console = console,
            **kwargs
        )

    def add_task(self,
        description: str,
        **kwargs
    ):
        indent = " " * 10
        total = kwargs.pop('total', None)
        task = super().add_task(
            f'{indent} [cyan]{description}',
            total = total,
            **kwargs
        )
        return task
    
    def get_default_columns(self):
        default_columns = super().get_default_columns()
        return [
            *default_columns,
            progress.MofNCompleteColumn(),
            "[yellow]Elasped:",
            progress.TimeElapsedColumn(),
        ]
    
    def track(
        self,
        sequence: Iterable[T],
        description: str,
        total: int | None = None,
        disable: bool = False,
        **kwargs,
    ):
        if total is None:
            if hasattr(sequence, '__len__'):
                assert isinstance(sequence, Sized)
                try:
                    total = len(sequence)
                except Exception:
                    # NOTE:
                    #   ストリーミング読み込み中の Loader のように、
                    #   件数を数えること自体に代償がある対象もある。
                    #   その場合は総数不明のまま進捗を表示する。
                    #   rich 側も length_hint() で __len__ を呼ぶため、
                    #   イテレータに包んで再度の問い合わせを防ぐ。
                    total = None
                    sequence = iter(sequence)
        if disable:
            return sequence
        return super().track(
            sequence,
            total = total,
            description = description,
            **kwargs,
        )

def track(
    sequence: Iterable[T],
    description: str,
    total: int | None = None,
    disable: bool = False,
    progress: Progress | None = None,
    **kwargs,
):
    if progress is None:
        return base_track(
            sequence,
            description = description,
            total = total,
            disable = disable,
            **kwargs,
        )
    return progress.track(
        sequence,
        description = description,
        total = total,
        disable = disable,
        **kwargs,
    )

__all__ = [
    'Console',
    'TaskID',
    'Progress',
    'track',
]