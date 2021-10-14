import sqlalchemy

__sa_version__ = tuple(map(int, sqlalchemy.__version__.rstrip("b").split(".")))

__all__ = ["__sa_version__"]
