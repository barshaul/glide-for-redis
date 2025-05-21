# glide/typing.py

from typing import Protocol, List, Any, Awaitable, TypeVar, Union
from constants import TResult

class TAsyncGlideClient(Protocol):
    def custom_command(self, args: List[Any]) -> Awaitable[TResult]:
        ...

class TSyncGlideClient(Protocol):
    def custom_command(self, args: List[Any]) -> TResult:
        ...
