import ast
import re
from datetime import datetime

from setuptools import find_packages, setup

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("graphene_sqlalchemy_core/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )
    version += f".{int(datetime.now().timestamp())}"

requirements = [
    # To keep things simple, we only support newer versions of Graphene
    "graphene>=3.0b7",
    "promise>=2.3",
    # Tests fail with 1.0.19
    "SQLAlchemy>=1.4,<2",
    "singledispatch == 3.7.0",
    "aiodataloader == 0.2.0",
]
try:
    import enum
except ImportError:  # Python < 2.7 and Python 3.3
    requirements.append("enum34 >= 1.1.6")

tests_require = [
    "pytest>=6.2.0,<7.0",
    "pytest-cov>=2.11.0,<3.0",
    "sqlalchemy_utils>=0.37.0,<1.0",
    "pytest-benchmark>=3.4.0,<4.0",
    "mock == 4.0.3",
    "pytest-asyncio == 0.15.1",
    "aiosqlite == 0.17.0",
]

setup(
    name="graphene-sqlalchemy-core",
    version=version,
    description="Graphene SQLAlchemy core integration",
    long_description=open("README.rst").read(),
    url="https://gitlab.com/live-art-project/graphene-sqlalchemy-core",
    author="Syorsh sergey",
    author_email="myrik260138@gmail.com",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="api graphql protocol rest relay graphene",
    packages=find_packages(exclude=["tests"]),
    install_requires=requirements,
    extras_require={
        "dev": [
            "tox==3.7.0",  # Should be kept in sync with tox.ini
            "coveralls==1.10.0",
            "pre-commit==1.14.4",
        ],
        "test": tests_require,
    },
    tests_require=tests_require,
)
