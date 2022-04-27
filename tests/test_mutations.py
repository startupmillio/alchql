import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from graphene_sqlalchemy_core.fields import SQLAlchemyConnectionField
from graphene_sqlalchemy_core.gql_id import ResolvedGlobalId, encode_gql_id
from graphene_sqlalchemy_core.middlewares import LoaderMiddleware
from graphene_sqlalchemy_core.node import AsyncNode
from graphene_sqlalchemy_core.sql_mutation import (
    SQLAlchemyCreateMutation,
    SQLAlchemyDeleteMutation,
    SQLAlchemyUpdateMutation,
)
from graphene_sqlalchemy_core.types import SQLAlchemyObjectType
from .models import Pet
from .test_query import add_test_data


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
            batching = True

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
            batching = True

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
        mutation UpdatePet($value: InputCreatePet!) {
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
        mutation InsertPet($value: InputCreatePet!) {
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
async def test_multiple_mutations(session):
    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)
            batching = True

    class MutationCreatePet(SQLAlchemyCreateMutation):
        class Meta:
            model = Pet
            output = PetType
            required_fields = ("name",)

    class MutationUpdatePet(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet
            output = PetType

    class MutationUpdatePetName(SQLAlchemyUpdateMutation):
        class Meta:
            model = Pet
            output = PetType
            only_fields = ["name"]

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        create_pet = MutationCreatePet.Field()
        update_pet = MutationUpdatePet.Field()
        update_pet_name = MutationUpdatePetName.Field()

    schema = graphene.Schema(
        query=Query,
        mutation=Mutation,
    )

    query = """
        mutation UpdatePet($value: InputCreatePet_8!) {
            createPet(value: $value) {
                id
                name
                petKind
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
    assert not result.errors, result.errors[0].message
    pet_id = result.data["createPet"]["id"]

    query = """
        mutation UpdatePet($value: InputUpdatePet!, $updatePetId: ID!) {
            updatePet(value: $value, id: $updatePetId) {
                id
                name
            }
        }
    """

    result = await schema.execute_async(
        query,
        variables={
            "updatePetId": pet_id,
            "value": {
                "name": "dsa",
                "hairKind": "LONG",
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )
    assert not result.errors
    assert result.data["updatePet"]["name"] == "dsa"

    query = """
        mutation UpdatePet($value: InputUpdatePet_2!, $updatePetId: ID!) {
            updatePetName(value: $value, id: $updatePetId) {
                id
                name
            }
        }
    """

    result = await schema.execute_async(
        query,
        variables={
            "updatePetId": pet_id,
            "value": {
                "name": "eeee",
            },
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )
    assert not result.errors, result.errors[0].message
    assert result.data["updatePetName"]["name"] == "eeee"


@pytest.mark.asyncio
async def test_delete_mutation(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)
            batching = True

    class MutationDeletePet(SQLAlchemyDeleteMutation):
        class Meta:
            model = Pet
            output = PetType

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        delete_pet = MutationDeletePet.Field()

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
        mutation UpdatePet($updatePetId: ID!) {
            deletePet(id: $updatePetId) {
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
            "updatePetId": id_to_update,
        },
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Pet]),
        ],
    )

    assert not result.errors
    # TODO: problems with sqlite
    assert result.data["deletePet"] is None
