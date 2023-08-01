import graphene
import pytest
import sqlalchemy as sa
from graphene import Context
from sqlalchemy.ext.asyncio import AsyncSession

from alchql.fields import FilterConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import SQLAlchemyObjectType
from .models import Editor, Reporter


async def add_test_data(session: AsyncSession):
    await session.execute(
        sa.insert(Reporter).values(
            [
                {
                    Reporter.first_name: "John",
                    Reporter.last_name: "Doe",
                    Reporter.email: "email",
                },
                {
                    Reporter.first_name: "John1",
                    Reporter.last_name: "Doe1",
                    Reporter.email: "email1",
                },
            ]
        )
    )


async def get_query():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        node = graphene.relay.Node.Field()
        reporters = FilterConnectionField(
            ReporterType, sort=ReporterType.sort_argument()
        )

    return Query


@pytest.mark.asyncio
async def test_query_fragments(session):
    await add_test_data(session)

    query = """
    query {
      reporters {
        edges {
          node {
            firstName
            ...ReporterFragment1
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
    fragment ReporterFragment1 on ReporterType {
      lastName
      ...ReporterFragment2
    }
    fragment ReporterFragment2 on ReporterType {
      email
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
    assert result.data == {
        "reporters": {
            "edges": [
                {"node": {"email": "email", "firstName": "John", "lastName": "Doe"}},
                {"node": {"email": "email1", "firstName": "John1", "lastName": "Doe1"}},
            ],
            "pageInfo": {
                "endCursor": "YXJyYXljb25uZWN0aW9uOjE=",
                "hasNextPage": False,
                "hasPreviousPage": False,
                "startCursor": "YXJyYXljb25uZWN0aW9uOjA=",
            },
        }
    }
