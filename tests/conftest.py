import asyncio
from unittest.mock import patch

import graphene
import pytest
from graphql import ASTValidationRule, GraphQLError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from graphene_sqlalchemy_core.converter import convert_sqlalchemy_composite
from graphene_sqlalchemy_core.registry import reset_global_registry
from .models import Base, CompositeFullName


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def reset_registry():
    reset_global_registry()

    # Prevent tests that implicitly depend on Reporter from raising
    # Tests that explicitly depend on this behavior should re-register a converter
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.Field(graphene.Int)


@pytest.fixture(scope="function")
async def engine():
    e = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        convert_unicode=True,
        echo=False,
    )

    async with e.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    yield e

    e.dispose()


@pytest.fixture(scope="function")
async def session(engine):
    async with AsyncSession(engine) as s:
        yield s


@pytest.fixture
def raise_graphql():
    def r(self, x, *args, **kwargs):
        raise x

    def init(
        self,
        message: str,
        nodes=None,
        source=None,
        positions=None,
        path=None,
        original_error=None,
        extensions=None,
    ):
        if isinstance(original_error, Exception):
            raise original_error
        else:
            raise Exception(message)

    with patch.object(ASTValidationRule, "report_error", r), patch.object(
        GraphQLError, "__init__", init
    ):
        yield
