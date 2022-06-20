from unittest.mock import patch

import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from alchql.connection.utils import offset_to_cursor
from alchql.consts import OP_ILIKE
from alchql.fields import FilterConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import SQLAlchemyObjectType
from .models import Editor
from alchql.connection import from_query


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
async def test_query_first(session, raise_graphql):
    await add_test_data(session)

    query = """
    query {
      editors (first: 3) {
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
    """

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
async def test_query_last(session, raise_graphql):
    await add_test_data(session)

    query = """
    query {
      editors (last: 3) {
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
    """

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
        f.assert_called()

    assert not result.errors
    assert result.data == {
        "editors": {
            "edges": [
                {"node": {"name": "Editor#97"}},
                {"node": {"name": "Editor#98"}},
                {"node": {"name": "Editor#99"}},
            ],
            "pageInfo": {
                "endCursor": "YXJyYXljb25uZWN0aW9uOjk5",
                "hasNextPage": False,
                "hasPreviousPage": True,
                "startCursor": "YXJyYXljb25uZWN0aW9uOjk3",
            },
        }
    }


@pytest.mark.asyncio
async def test_query_after(session, raise_graphql):
    await add_test_data(session)
    after = offset_to_cursor(96)

    query = """
    query {
      editors (after: "%s") {
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
        f.assert_called()

    assert not result.errors
    assert result.data == {
        "editors": {
            "edges": [
                {"node": {"name": "Editor#97"}},
                {"node": {"name": "Editor#98"}},
                {"node": {"name": "Editor#99"}},
            ],
            "pageInfo": {
                "endCursor": "YXJyYXljb25uZWN0aW9uOjk5",
                "hasNextPage": False,
                "hasPreviousPage": True,
                "startCursor": "YXJyYXljb25uZWN0aW9uOjk3",
            },
        }
    }


@pytest.mark.asyncio
async def test_query_before(session, raise_graphql):
    await add_test_data(session)
    before = offset_to_cursor(3)

    query = """
    query {
      editors (before: "%s") {
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
