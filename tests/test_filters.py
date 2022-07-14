import pytest
import sqlalchemy as sa
from graphene import (
    Context,
    ObjectType,
    Schema,
)
from sqlalchemy.ext.asyncio import AsyncSession

from alchql.consts import OP_EQ
from alchql.fields import (
    BatchSQLAlchemyConnectionField,
    FilterConnectionField,
)
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import (
    SQLAlchemyObjectType,
)
from .models import Reporter


async def add_reporter(session: AsyncSession):
    result = await session.execute(
        sa.insert(Reporter).values(
            {
                "first_name": "first_name",
                "last_name": "last_name",
                "email": "email",
                "favorite_pet_kind": "cat",
            }
        )
    )
    return result.lastrowid


@pytest.mark.asyncio
async def test_filter1(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)
            filter_fields = {
                Reporter.first_name: [OP_EQ],
            }

    class Query(ObjectType):
        reporter = FilterConnectionField(ReporterType, sort=None)

    await add_reporter(session)

    schema = Schema(query=Query, types=[ReporterType])
    result = await schema.execute_async(
        """
        query {
            reporter(firstName_Eq: "first_name") {
                edges{
                    node{
                        firstName
                    }
                }
            }
        }
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Reporter]),
        ],
    )

    assert not result.errors
    assert result.data["reporter"]["edges"][0]["node"]["firstName"] == "first_name"


@pytest.mark.asyncio
async def test_filter2(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)
            filter_fields = {
                Reporter.__table__.c.first_name: [OP_EQ],
            }

    class Query(ObjectType):
        reporter = FilterConnectionField(ReporterType, sort=None)

    await add_reporter(session)

    schema = Schema(query=Query, types=[ReporterType])
    result = await schema.execute_async(
        """
        query {
            reporter(firstName_Eq: "first_name") {
                edges{
                    node{
                        firstName
                    }
                }
            }
        }
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Reporter]),
        ],
    )

    assert not result.errors
    assert result.data["reporter"]["edges"][0]["node"]["firstName"] == "first_name"


@pytest.mark.asyncio
async def test_filter3(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)
            filter_fields = {
                Reporter.first_name.label("name_first"): [OP_EQ],
            }

    class Query(ObjectType):
        reporter = FilterConnectionField(ReporterType, sort=None)

    await add_reporter(session)

    schema = Schema(query=Query, types=[ReporterType])
    result = await schema.execute_async(
        """
        query {
            reporter(nameFirst_Eq: "first_name") {
                edges{
                    node{
                        firstName
                    }
                }
            }
        }
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Reporter]),
        ],
    )

    assert not result.errors
    assert result.data["reporter"]["edges"][0]["node"]["firstName"] == "first_name"
