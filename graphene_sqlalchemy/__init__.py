import pkg_resources

from .fields import SQLAlchemyConnectionField
from .types import SQLAlchemyObjectType
from .utils import get_query, get_session
from .sa_version import __sa_version__

__version__ = "2.3.0"

__all__ = [
    "__version__",
    "SQLAlchemyObjectType",
    "SQLAlchemyConnectionField",
    "get_query",
    "get_session",
]


if pkg_resources.get_distribution("SQLAlchemy").parsed_version.release < (1, 4):
    raise Exception("Use SQLAlchemy version > 1.4")
