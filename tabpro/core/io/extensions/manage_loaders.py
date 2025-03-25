import os
from typing import Any, Generator, Protocol

from ...classes.row import Row

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
