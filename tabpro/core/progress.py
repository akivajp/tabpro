from typing import Iterable

from rich import progress
from rich.console import Console

class Progress(progress.Progress):
    def add_task(self, *args, **kwargs):
        indent = " " * 10
        first = args[0]
        task = super().add_task(
            f'{indent} [cyan]{first}',
            *args[1:],
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
    sequence: Iterable,
    description: str = 'Working...',
    total: int | None = None,
    console: Console | None = None,
):
    with Progress(
        console = console,
    ) as progress:
        if total is None:
            if hasattr(sequence, '__len__'):
                total = len(sequence)
        task = progress.add_task(description, total=total)
        for item in sequence:
            progress.update(task, advance=1)
            yield item
