import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from alchql.fields import SQLAlchemyConnectionField
from alchql.gql_id import ResolvedGlobalId, encode_gql_id
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.sql_mutation import SQLAlchemyUpdateMutation
from alchql.types import SQLAlchemyObjectType
from tests.models import Pet
from tests.test_query import add_test_data


@pytest.mark.asyncio
async def test_get_update_mutation_case1(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet
            output = PetType

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        update_pet = MutationUpdatePet.Field()

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
            mutation UpdatePet($value: InputUpdatePet!, $updatePetId: ID!) {
                updatePet(value: $value, id: $updatePetId) {
                    id
                    name
                }
            }
        """

    id_to_update = all_pets[0]["id"]
    new_name = "bear"

    result = await schema.execute_async(
        query,
        variables={
            "value": {"name": new_name},
            "updatePetId": id_to_update,
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )

    assert not result.errors
    assert result.data["updatePet"]["id"] == id_to_update
    assert result.data["updatePet"]["name"] == new_name


@pytest.mark.asyncio
async def test_get_update_mutation_case2(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet

        Output = PetType

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        update_pet = MutationUpdatePet.Field()

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
            mutation UpdatePet($value: InputUpdatePet!, $updatePetId: ID!) {
                updatePet(value: $value, id: $updatePetId) {
                    id
                    name
                }
            }
        """

    id_to_update = all_pets[0]["id"]
    new_name = "bear"

    result = await schema.execute_async(
        query,
        variables={
            "value": {"name": new_name},
            "updatePetId": id_to_update,
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )

    assert not result.errors
    assert result.data["updatePet"]["id"] == id_to_update
    assert result.data["updatePet"]["name"] == new_name


@pytest.mark.asyncio
async def test_get_update_mutation_case3(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet

        id = graphene.ID()
        name = graphene.String()

        def resolve_id(self, info):
            return ResolvedGlobalId(self.__class__.__name__, self.id).encode()

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        update_pet = MutationUpdatePet.Field()

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
        mutation UpdatePet($value: InputUpdatePet!, $updatePetId: ID!) {
            updatePet(value: $value, id: $updatePetId) {
                id
                name
            }
        }
    """

    id_to_update = all_pets[0]["id"]
    new_name = "bear"

    result = await schema.execute_async(
        query,
        variables={
            "value": {"name": new_name},
            "updatePetId": id_to_update,
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )

    assert not result.errors
    assert result.data["updatePet"]["id"] == "TXV0YXRpb25VcGRhdGVQZXQ6MQ=="
    assert result.data["updatePet"]["name"] == new_name


@pytest.mark.asyncio
async def test_get_update_mutation_empty_value(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet
            output = PetType

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        update_pet = MutationUpdatePet.Field()

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
        mutation UpdatePet($value: InputPet!, $updatePetId: ID!) {
            updatePet(value: $value, id: $updatePetId) {
                id
                name
            }
        }
    """

    id_to_update = all_pets[0]["id"]

    with pytest.raises(Exception) as e:
        result = await schema.execute_async(
            query,
            variables={
                "value": {},
                "updatePetId": id_to_update,
            },
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Pet]),
            ],
        )
        assert result.errors
        raise result.errors[0]


@pytest.mark.asyncio
async def test_update_mutation_always_queries_primary_keys(session):
    await add_test_data(session)

    global id_to_update

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet
            output = PetType

        @classmethod
        async def mutate(cls, *args, **kwargs):
            result = await super().mutate(*args, **kwargs)
            assert result.id == id_to_update
            return result

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        update_pet = MutationUpdatePet.Field()

    schema = graphene.Schema(
        query=Query,
        mutation=Mutation,
    )

    query = """
        mutation UpdatePet($value: InputUpdatePet!, $updatePetId: ID!) {
            updatePet(value: $value, id: $updatePetId) {
                name
            }
        }
    """

    id_to_update = (await session.execute(sa.select(Pet.id))).scalars().first()
    gql_id_to_update = encode_gql_id(PetType.__name__, id_to_update)
    new_name = "New name"

    result = await schema.execute_async(
        query,
        variables={
            "value": {"name": new_name},
            "updatePetId": gql_id_to_update,
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )
    assert not result.errors
    assert result.data["updatePet"]["name"] == new_name
