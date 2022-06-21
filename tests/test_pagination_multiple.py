from unittest.mock import patch

import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from alchql.connection import from_query
from alchql.connection.utils import offset_to_cursor
from alchql.consts import OP_ILIKE
from alchql.fields import FilterConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import SQLAlchemyObjectType
from .models import Editor


async def add_test_data(session):
    await session.execute(
        sa.insert(Editor).values(
            [
                {
                    Editor.name: f"Editor#{num}",
                }
                for num in range(100)
            ]
        )
    )


async def get_query():
    class EditorType(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (AsyncNode,)
            filter_fields = {
                Editor.name: [OP_ILIKE],
            }

    class Query(graphene.ObjectType):
        node = graphene.relay.Node.Field()
        editors = FilterConnectionField(EditorType, sort=EditorType.sort_argument())

    return Query


@pytest.mark.asyncio
async def test_query_forward(session, raise_graphql):
    await add_test_data(session)
    cursor = offset_to_cursor(3)

    query = """
    query {
      editors (first: 3, after:"%s") {
        edges {
          node {
            name
          }
        }
        pageInfo {
          startCursor
          endCursor
          hasPreviousPage
          hasNextPage
        }
      }
    }
    """ % (
        cursor
    )

    schema = graphene.Schema(query=await get_query())
    with patch.object(
        from_query, "get_count_query", side_effect=from_query.get_count_query
    ) as f:
        result = await schema.execute_async(
            query,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Editor]),
            ],
        )
        f.assert_not_called()

    assert not result.errors
    assert result.data == {
        "editors": {
            "edges": [
                {"node": {"name": "Editor#4"}},
                {"node": {"name": "Editor#5"}},
                {"node": {"name": "Editor#6"}},
            ],
            "pageInfo": {
                "endCursor": "YXJyYXljb25uZWN0aW9uOjY=",
                "hasNextPage": True,
                "hasPreviousPage": True,
                "startCursor": "YXJyYXljb25uZWN0aW9uOjQ=",
            },
        }
    }


@pytest.mark.asyncio
async def test_query_backward(session, raise_graphql):
    await add_test_data(session)
    cursor = offset_to_cursor(3)

    query = """
    query {
      editors (last: 3, before:"%s") {
        edges {
          node {
            name
          }
        }
        pageInfo {
          startCursor
          endCursor
          hasPreviousPage
          hasNextPage
        }
      }
    }
    """ % (
        cursor
    )

    schema = graphene.Schema(query=await get_query())
    with patch.object(
        from_query, "get_count_query", side_effect=from_query.get_count_query
    ) as f:
        result = await schema.execute_async(
            query,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Editor]),
            ],
        )
        f.assert_not_called()

    assert not result.errors
    assert result.data == {
        "editors": {
            "edges": [
                {"node": {"name": "Editor#0"}},
                {"node": {"name": "Editor#1"}},
                {"node": {"name": "Editor#2"}},
            ],
            "pageInfo": {
                "endCursor": "YXJyYXljb25uZWN0aW9uOjI=",
                "hasNextPage": True,
                "hasPreviousPage": False,
                "startCursor": "YXJyYXljb25uZWN0aW9uOjA=",
            },
        }
    }


@pytest.mark.asyncio
async def test_query_slice(session, raise_graphql):
    await add_test_data(session)
    after = offset_to_cursor(3)
    before = offset_to_cursor(7)

    query = """
    query {
      editors (after: "%s", before:"%s") {
        edges {
          node {
            name
          }
        }
        pageInfo {
          startCursor
          endCursor
          hasPreviousPage
          hasNextPage
        }
      }
    }
    """ % (
        after,
        before,
    )

    schema = graphene.Schema(query=await get_query())
    with patch.object(
        from_query, "get_count_query", side_effect=from_query.get_count_query
    ) as f:
        result = await schema.execute_async(
            query,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Editor]),
            ],
        )
        f.assert_not_called()

    assert not result.errors
    assert result.data == {
        "editors": {
            "edges": [
                {"node": {"name": "Editor#4"}},
                {"node": {"name": "Editor#5"}},
                {"node": {"name": "Editor#6"}},
            ],
            "pageInfo": {
                "startCursor": "YXJyYXljb25uZWN0aW9uOjQ=",
                "endCursor": "YXJyYXljb25uZWN0aW9uOjY=",
                "hasPreviousPage": True,
                "hasNextPage": True,
            },
        }
    }
