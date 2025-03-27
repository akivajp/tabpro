from typing import Iterable

from rich import progress
from rich.console import Console
from rich.progress import (
    Task,
    track as base_track,
)

class Progress(progress.Progress):
    def add_task(self, *args, **kwargs):
        indent = " " * 10
        first = args[0]
        total = kwargs.pop('total', None)
        task = super().add_task(
            f'{indent} [cyan]{first}',
            *args[1:],
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
        sequence: Iterable,
        description: str,
        total: int | None = None,
        disable: bool = False,
        **kwargs,
    ):
        if total is None:
            if hasattr(sequence, '__len__'):
                total = len(sequence)
        if disable:
            return sequence
        return super().track(
            sequence,
            total = total,
            description = description,
            **kwargs,
        )

def track(
    sequence: Iterable,
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
