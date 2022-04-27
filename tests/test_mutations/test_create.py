import graphene
import pytest
from graphene import Context

from graphene_sqlalchemy_core.fields import SQLAlchemyConnectionField
from graphene_sqlalchemy_core.middlewares import LoaderMiddleware
from graphene_sqlalchemy_core.node import AsyncNode
from graphene_sqlalchemy_core.sql_mutation import (
    SQLAlchemyCreateMutation,
)
from graphene_sqlalchemy_core.types import SQLAlchemyObjectType
from tests.models import Pet
from tests.test_query import add_test_data


@pytest.mark.asyncio
async def test_get_create_mutation(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)
            batching = True

    class MutationInsertPet(SQLAlchemyCreateMutation):
        class Meta:
            model = Pet
            output = PetType

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        insert_pet = MutationInsertPet.Field()

    query = """
        query {
            allPets {
                edges {
                    node {
                        id
                        name
                    }
                }
            }
        }
    """

    schema = graphene.Schema(
        query=Query,
        mutation=Mutation,
    )
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )
    assert not result.errors

    all_pets = [i["node"] for i in result.data["allPets"]["edges"]]

    assert [i["name"] for i in all_pets] == [
        "Garfield",
        "Lassie",
    ]

    query = """
        mutation UpdatePet($value: InputPet!) {
            insertPet(value: $value) {
                name
            }
        }
    """

    result = await schema.execute_async(
        query,
        variables={
            "value": {
                "name": "asd",
                "petKind": "CAT",
                "hairKind": "SHORT",
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )

    assert not result.errors


@pytest.mark.asyncio
async def test_create_mutation_always_queries_primary_keys(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)
            batching = True

    class MutationCreatePet(SQLAlchemyCreateMutation):
        class Meta:
            model = Pet
            output = PetType

        @classmethod
        async def mutate(cls, *args, **kwargs):
            result = await super().mutate(*args, **kwargs)
            assert result.id is not None
            return result

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        insert_pet = MutationCreatePet.Field()

    schema = graphene.Schema(
        query=Query,
        mutation=Mutation,
    )

    query = """
        mutation InsertPet($value: InputPet!) {
            insertPet(value: $value) {
                name
            }
        }
    """

    result = await schema.execute_async(
        query,
        variables={
            "value": {
                "name": "asd",
                "petKind": "CAT",
                "hairKind": "SHORT",
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )

    assert not result.errors
