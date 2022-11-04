from typing_extensions import TypedDict
from typing import Dict


class Table(TypedDict):
    """
    Intended for typing usecases.
    """
    path: str
    name: str
    column_count: int
    nodes: Dict[str, str]
