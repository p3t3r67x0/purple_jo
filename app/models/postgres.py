"""App compatibility shim for shared PostgreSQL SQLModel definitions."""

from shared.models.postgres import *  # noqa: F401,F403
from shared.models.postgres import __all__  # noqa: F401
