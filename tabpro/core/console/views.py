from rich.console import Console
from rich.text import (
    Text,
    TextType,
)
from rich.panel import Panel as BasePanel

def capture_dict(
    row: dict,
):
    console = Console()
    with console.capture() as capture:
        console.print_json(data=row)
    text = Text.from_ansi(capture.get())
    return text

class Panel(BasePanel):
    def __init__(
        self,
        data,
        title: TextType | None = None,
        *args,
        **kwargs,
    ):
        if isinstance(data, dict):
            data = capture_dict(data)
        super().__init__(
            data,
            title = title,
            *args, **kwargs
        )
