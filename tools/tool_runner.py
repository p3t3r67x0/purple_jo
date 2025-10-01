"""Common CLI wrappers for tools scripts."""

from __future__ import annotations

from typing import Callable, ParamSpec, TypeVar


P = ParamSpec("P")
R = TypeVar("R")


class CLITool:
    """Lightweight wrapper that adapts legacy ``main`` functions into classes."""

    def __init__(self, runner: Callable[P, R]) -> None:
        self._runner = runner

    def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute the wrapped callable."""

        return self._runner(*args, **kwargs)
