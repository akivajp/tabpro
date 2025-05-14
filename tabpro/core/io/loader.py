'''
Loader class is responsible for loading the data from the source.
'''

import os.path

from rich.console import Console

from . extensions.manage_loaders import get_loader
from ..classes.row import Row

from .. progress import (
    Progress,
    track,
)

from tqdm.auto import tqdm

class Loader:
    def __init__(
        self,
        source: str,
        quiet: bool = False,
        no_header: bool = False,
        limit: int | None = None,
        progress: Progress | None = None,
    ):
        self.source = source
        self.quiet = quiet
        self.no_header = no_header
        self.limit = limit
        self.rows: list[Row] | None = None
        self.progress = progress
        self.fn_load = get_loader(
            self.source,
        )
        self.extension = os.path.splitext(self.source)[1]

    def __iter__(self):
        return self._yield_data()
    
    def __len__(self):
        if self.rows is None:
            for _ in self._yield_data():
                pass
        if self.rows is None:
            raise ValueError('No rows loaded')
        return len(self.rows)

    def _get_console(self):
        if self.console is None:
            self.console = Console()
        return self.console
    
    def _yield_data(self):
        if self.rows:
            for row in self.rows:
                yield row
        else:
            self.rows = []
            for row in self.fn_load(
                self.source,
                quiet=self.quiet,
                no_header=self.no_header,
                progress=self.progress,
                limit=self.limit,
            ):
                self.rows.append(row)
                yield row
