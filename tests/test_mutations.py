import graphene
import pytest
from graphene import Context
from graphql_relay import to_global_id

from graphene_sqlalchemy_core.fields import SQLAlchemyConnectionField
from graphene_sqlalchemy_core.loaders_middleware import LoaderMiddleware
from graphene_sqlalchemy_core.node import AsyncNode
from graphene_sqlalchemy_core.sql_mutation import SQLAlchemyUpdateMutation
from graphene_sqlalchemy_core.types import SQLAlchemyObjectType
from .models import Pet
from .test_query import add_test_data


async def _run_cases(session, Query, Mutation):
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
    new_name = "pedobear"

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
async def test_get_update_mutation_case1(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)
            batching = True

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
    new_name = "pedobear"

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
            batching = True

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
            mutation UpdatePet($value: InputPet!, $updatePetId: ID!) {
                updatePet(value: $value, id: $updatePetId) {
                    id
                    name
                }
            }
        """

    id_to_update = all_pets[0]["id"]
    new_name = "pedobear"

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
            batching = True

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet

        id = graphene.ID()
        name = graphene.String()

        def resolve_id(self, info):
            return to_global_id(self.__class__.__name__, self.id)

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
    new_name = "pedobear"

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
            batching = True

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
        await schema.execute_async(
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

    assert str(e.value) == "No value provided"
