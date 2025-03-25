'''
Loader class is responsible for loading the data from the source.
'''

from . extensions.manage_loaders import get_loader
from ..classes.row import Row

from tqdm.auto import tqdm

class Loader:
    def __init__(
        self,
        source: str,
        quiet: bool = False,
        no_header: bool = False,
    ):
        self.source = source
        self.quiet = quiet
        self.no_header = no_header
        self.rows: list[Row] | None = None
        self.fn_load = get_loader(self.source)

    def __iter__(self):
        return self.yield_data()
    
    def __len__(self):
        if self.rows is None:
            for _ in self.yield_data(quiet=self.quiet):
                pass
        return len(self.rows)

    def yield_data(self, quiet: bool = False):
        if self.rows:
            def get_iter():
                if quiet:
                    return self.rows
                else:
                    return tqdm(self.rows, desc=f'Processing rows from: {self.source}')
            for row in get_iter():
                yield row
        else:
            self.rows = []
            for row in self.fn_load(
                self.source,
                quiet=quiet,
                no_header=self.no_header,
            ):
                self.rows.append(row)
                yield row
