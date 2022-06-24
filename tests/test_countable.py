from unittest import mock

import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from alchql.connection import from_query
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


class CountableConnectionCreator:
    @classmethod
    def create_type(cls, connection_name, **kwargs):
        class CountableConnection(graphene.relay.Connection):
            count = graphene.Int()
            total_count = graphene.Int()

            class Meta:
                name = connection_name
                node = kwargs["node"]

            @staticmethod
            def resolve_count(root, info, **args):
                return len(root.edges)

        return CountableConnection


async def get_query():
    class EditorType(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (AsyncNode,)
            connection_class = CountableConnectionCreator
            filter_fields = {
                Editor.name: [OP_ILIKE],
            }

    class Query(graphene.ObjectType):
        node = graphene.relay.Node.Field()
        editors = FilterConnectionField(EditorType, sort=EditorType.sort_argument())

    return Query


@pytest.mark.asyncio
async def test_counters(session, raise_graphql):
    await add_test_data(session)

    query = """
    query {
      editors {
        count
        totalCount
      }
    }
    """

    schema = graphene.Schema(query=await get_query())

    with mock.patch.object(from_query, "DEFAULT_LIMIT", 50):
        result = await schema.execute_async(
            query,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Editor]),
            ],
        )

    assert not result.errors
    assert result.data == {"editors": {"count": 50, "totalCount": 100}}
