import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from alchql.fields import SQLAlchemyConnectionField
from alchql.gql_id import ResolvedGlobalId
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.sql_mutation import (
    SQLAlchemyCreateMutation,
)
from alchql.types import SQLAlchemyObjectType
from tests import models as m
from tests.test_query import add_test_data


async def get_all_pets(session, schema):
    result = await schema.execute_async(
        """
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
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([m.Pet]),
        ],
    )
    assert not result.errors

    all_pets = [i["node"] for i in result.data["allPets"]["edges"]]

    return all_pets


@pytest.mark.asyncio
async def test_get_create_mutation(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = m.Pet
            interfaces = (AsyncNode,)

    class MutationInsertPet(SQLAlchemyCreateMutation):
        class Meta:
            model = m.Pet
            output = PetType

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        insert_pet = MutationInsertPet.Field()

    schema = graphene.Schema(
        query=Query,
        mutation=Mutation,
    )

    all_pets = await get_all_pets(session, schema)

    assert {i["name"] for i in all_pets} == {
        "Garfield",
        "Lassie",
    }

    result = await schema.execute_async(
        """
            mutation UpdatePet($value: InputCreatePet_c!) {
                insertPet(value: $value) {
                    name
                }
            }
        """,
        variables={
            "value": {
                "name": "Odin",
                "petKind": "CAT",
                "hairKind": "SHORT",
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([m.Pet]),
        ],
    )

    assert not result.errors

    all_pets = await get_all_pets(session, schema)

    assert {i["name"] for i in all_pets} == {
        "Garfield",
        "Lassie",
        "Odin",
    }


@pytest.mark.asyncio
async def test_create_mutation_always_queries_primary_keys(session):
    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = m.Pet
            interfaces = (AsyncNode,)

    class MutationCreatePet(SQLAlchemyCreateMutation):
        class Meta:
            model = m.Pet
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
        mutation InsertPet($value: InputCreatePet_c!) {
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
            LoaderMiddleware([m.Pet]),
        ],
    )

    assert not result.errors


@pytest.mark.asyncio
async def test_create_mutation_fk_with_relation(session):
    reporter_id = (await session.execute(sa.insert(m.Reporter))).lastrowid

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = m.Reporter
            interfaces = (AsyncNode,)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = m.Pet
            interfaces = (AsyncNode,)

    class MutationCreatePet(SQLAlchemyCreateMutation):
        class Meta:
            model = m.Pet
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
        mutation InsertPet($value: InputCreatePet_c!) {
            insertPet(value: $value) {
                name
                reporterId
                reporter{
                    id
                }
            }
        }
    """

    result = await schema.execute_async(
        query,
        variables={
            "value": {
                "name": "valid",
                "petKind": "CAT",
                "hairKind": "SHORT",
                "reporterId": ResolvedGlobalId(
                    ReporterType.__name__, reporter_id
                ).encode(),
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([m.Pet, m.Reporter]),
        ],
    )

    assert not result.errors


@pytest.mark.asyncio
async def test_create_mutation_fk_without_relation(session, raise_graphql):
    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = m.Pet
            interfaces = (AsyncNode,)

    class MutationCreatePet(SQLAlchemyCreateMutation):
        class Meta:
            model = m.Pet
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
        mutation InsertPet($value: InputCreatePet_c!) {
            insertPet(value: $value) {
                name
                reporterId
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
                "reporterId": "YWRzOjQ=",
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([m.Pet]),
        ],
    )

    assert not result.errors
