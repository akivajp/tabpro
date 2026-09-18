# NOTE:
#   これらは import した時点でローダーとライターを登録する副作用が目的であり、
#   名前としては使われない。未使用として取り除くと、対応形式が全て失われる。
from . extensions import io_csv  # noqa: F401
from . extensions import io_dbq  # noqa: F401
from . extensions import io_excel  # noqa: F401
from . extensions import io_json  # noqa: F401
from . extensions import io_jsonl  # noqa: F401

from . loader import Loader
from . extensions.manage_writers import (
    BaseWriter as Writer,
    check_writer,
    get_writer,
    save,
)

get_loader = Loader

__all__ = [
    'Loader',
    'Writer',
    'check_writer',
    'get_loader',
    'get_writer',
    'save',
]
