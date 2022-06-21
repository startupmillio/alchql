import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

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
async def test_query_page_info_full(session, raise_graphql):
    await add_test_data(session)

    query = """
    query {
      editors {
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
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )
    assert not result.errors


@pytest.mark.asyncio
async def test_query_page_info(session, raise_graphql):
    await add_test_data(session)

    query = """
    query {
      editors {
        pageInfo {
          startCursor
          hasNextPage
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
    assert not result.errors
