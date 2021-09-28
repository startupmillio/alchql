import graphene
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from .models import Base, CompositeFullName
from ..converter import convert_sqlalchemy_composite
from ..registry import reset_global_registry

test_db_url = "postgresql+asyncpg://godunov:godunov@127.0.0.1:5432/test_graphene"  # use in-memory database for tests


@pytest.fixture(autouse=True)
def reset_registry():
    reset_global_registry()

    # Prevent tests that implicitly depend on Reporter from raising
    # Tests that explicitly depend on this behavior should re-register a converter
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.Field(graphene.Int)


@pytest.fixture(scope="function")
async def session_factory():
    engine = create_async_engine(test_db_url, convert_unicode=True, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession(engine) as s:
        yield s

    # SQLite in-memory db is deleted when its connection is closed.
    # https://www.sqlite.org/inmemorydb.html
    engine.dispose()


@pytest.fixture(scope="function")
def session(session_factory):
    return session_factory
