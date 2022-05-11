import asyncio
import graphene
import pytest
import sqlalchemy as sa
from graphene import Context
from unittest import mock

from .models import Editor
from alchql.types import SQLAlchemyObjectType
from alchql.fields import SQLAlchemyConnectionField
from alchql.node import AsyncNode
from alchql import fields
from alchql.middlewares import LoaderMiddleware


async def add_test_data(session):
    editors = [f"Editor#{num}" for num in range(100)]

    async def create_editor(name):
        await session.execute(
            sa.insert(Editor).values(
                {
                    Editor.name: name,
                }
            )
        )

    await asyncio.gather(*[create_editor(name) for name in editors])


async def get_query():
    class EditorType(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        node = graphene.relay.Node.Field()
        editors = SQLAlchemyConnectionField(
            EditorType.connection, sort=EditorType.sort_argument()
        )

    return Query


@pytest.mark.asyncio
async def test_query_no_filters(session):
    await add_test_data(session)

    query = """
            query {
              editors {
                edges {
                  node {
                    id,
                    name
                  }
                }
              }
            }
        """

    limit = 10
    schema = graphene.Schema(query=await get_query())
    with mock.patch.object(fields, "DEFAULT_LIMIT", limit):
        result = await schema.execute_async(
            query,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Editor]),
            ],
        )
        assert len(result.data["editors"]["edges"]) == limit


@pytest.mark.asyncio
async def test_query_first_specified(session):
    await add_test_data(session)

    first = 50

    query = (
        """
            query {
              editors (first: """
        + str(first)
        + """) {
                edges {
                  node {
                    id,
                    name
                  }
                }
              }
            }
        """
    )

    schema = graphene.Schema(query=await get_query())
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )
    assert len(result.data["editors"]["edges"]) == first


@pytest.mark.asyncio
async def test_query_only_last_specified(session):
    await add_test_data(session)

    query = """
            query {
              editors (last: 1) {
                edges {
                  node {
                    id,
                    name
                  }
                }
              }
            }
        """

    schema = graphene.Schema(query=await get_query())
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )
    assert result.errors
