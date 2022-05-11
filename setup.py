import ast
import re
from datetime import datetime

from setuptools import find_packages, setup

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("alchql/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )
    version += f".{int(datetime.now().timestamp())}"

requirements = [
    # To keep things simple, we only support newer versions of Graphene
    "graphene>=3.0",
    "promise>=2.3",
    "SQLAlchemy>=1.4,<2",
    "aiodataloader == 0.2.1",
    "protobuf",
    "setuptools>=v49.0.0",
]

tests_require = [
    "pytest>=6.2.0,<7.0",
    "coverage[toml]",
    "sqlalchemy_utils>=0.37.0,<1.0",
    "pytest-benchmark>=3.4.0,<4.0",
    "pytest-asyncio>=0.17.2,<0.18",
    "aiosqlite>=0.17.0,<0.18",
]

setup(
    name="alchql",
    version=version,
    description="Graphene SQLAlchemy core integration",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/startupmillio/alchql",
    author="Yorsh Sergey",
    author_email="myrik260138@gmail.com",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="api graphql protocol rest relay graphene",
    packages=find_packages(exclude=["tests"]),
    install_requires=requirements,
    extras_require={
        "test": tests_require,
    },
    tests_require=tests_require,
)
