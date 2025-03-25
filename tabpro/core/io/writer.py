'''
Base class for writing data to a file.
'''

from typing import (
    IO,
)

from ..classes.row import Row

from tqdm.auto import tqdm

class BaseWriter:
    def __init__(
        self,
        target: str,
        streaming: bool = True,
        quiet: bool = False,
        encoding: str = 'utf-8',
        skip_header: bool = False,
    ):
        self.target = target
        self.streaming = streaming
        self.quiet = quiet
        self.encoding = encoding
        self.skip_header = skip_header
        self.rows: list[Row] | None = None
        self.fobj: IO | None = None
        self.finished: bool = False
        if not self.support_streaming():
            self.streaming = False
        if self.streaming:
            self.open()

    def open(self):
        if self.fobj:
            return
        self.fobj = open(self.target, 'w', encoding=self.encoding)

    def support_streaming(self):
        return False

    def push_row(self, row: Row):
        if self.rows is None:
            self.rows = []
        self.rows.append(row)
        if self.streaming:
            self.write_row(row)

    def push_rows(self, rows: list[Row]):
        for row in rows:
            self.push_row(row)

    def write_row(self, row: Row):
        raise NotImplementedError
    
    def write_all_rows(self):
        raise NotImplementedError
    
    def close(self):
        if self.finished: return
        if self.rows:
            if not self.streaming:
                self.write_all_rows()
            self.finished = True
        if self.fobj:
            self.fobj.close()
            self.fobj = None
        return
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
    
    def __del__(self):
        self.close()
        return
    