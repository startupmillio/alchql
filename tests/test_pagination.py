import graphene
import pytest
import sqlalchemy as sa
from graphene import Context
from unittest import mock

from .models import Editor
from alchql.consts import OP_ILIKE
from alchql.types import SQLAlchemyObjectType
from alchql.fields import FilterConnectionField
from alchql.node import AsyncNode
from alchql.connection import from_query
from alchql.middlewares import LoaderMiddleware


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


async def get_start_end_cursor(first: int, session):
    query = (
        """
            query {
              editors (first: %s) {
                edges {
                  node {
                    id
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
        % first
    )

    schema = graphene.Schema(query=await get_query())
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )
    page_info = result.data["editors"]["pageInfo"]
    start_cursor = page_info["startCursor"]
    end_cursor = page_info["endCursor"]
    return start_cursor, end_cursor


@pytest.mark.asyncio
async def test_query_no_filters(session):
    await add_test_data(session)

    query = """
            query {
              editors {
                edges {
                  node {
                    id
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

    limit = 10
    schema = graphene.Schema(query=await get_query())
    with mock.patch.object(from_query, "DEFAULT_LIMIT", limit):
        result = await schema.execute_async(
            query,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Editor]),
            ],
        )
        assert not result.errors
        assert len(result.data["editors"]["edges"]) == limit

        page_info = result.data["editors"]["pageInfo"]
        assert page_info["startCursor"]
        assert page_info["endCursor"]
        assert not page_info["hasPreviousPage"]
        assert page_info["hasNextPage"]


@pytest.mark.asyncio
async def test_query_first_specified(session):
    await add_test_data(session)

    first = 50

    query = (
        """
            query {
              editors (first: %s) {
                edges {
                  node {
                    id
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
        % first
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

    page_info = result.data["editors"]["pageInfo"]
    assert page_info["startCursor"]
    assert page_info["endCursor"]
    assert not page_info["hasPreviousPage"]
    assert page_info["hasNextPage"]


@pytest.mark.asyncio
async def test_query_first_after_specified(session):
    await add_test_data(session)

    _, end_cursor = await get_start_end_cursor(10, session)

    query = (
        """
        query {
          editors (first: 10, after: "%s") {
            edges {
              node {
                id
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
        % end_cursor
    )

    schema = graphene.Schema(query=await get_query())
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )

    editors = result.data["editors"]["edges"]
    assert len(editors) == 10
    assert editors[0]["node"]["name"] == "Editor#10"

    page_info = result.data["editors"]["pageInfo"]
    assert page_info["startCursor"]
    assert page_info["endCursor"]
    assert page_info["hasPreviousPage"]
    assert page_info["hasNextPage"]


@pytest.mark.asyncio
async def test_last_before_specified(session):
    await add_test_data(session)

    _, end_cursor = await get_start_end_cursor(20, session)

    query = (
        """
        query {
          editors (
            last: 10, 
            name_Ilike: "Editor", 
            before: "%s"
          ) {
            edges {
              node {
                id
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
        % end_cursor
    )

    schema = graphene.Schema(query=await get_query())
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )

    editors = result.data["editors"]["edges"]
    assert len(editors) == 10
    assert editors[0]["node"]["name"] == "Editor#9"

    page_info = result.data["editors"]["pageInfo"]
    assert page_info["startCursor"]
    assert page_info["endCursor"]
    assert page_info["hasPreviousPage"]
    assert page_info["hasNextPage"]
