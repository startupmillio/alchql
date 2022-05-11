import graphene
import pytest
from graphene import Context

from alchql.fields import SQLAlchemyConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.sql_mutation import (
    SQLAlchemyCreateMutation,
    SQLAlchemyUpdateMutation,
)
from alchql.types import SQLAlchemyObjectType
from tests.models import Pet


@pytest.mark.asyncio
async def test_multiple_mutations(session):
    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

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
        mutation UpdatePet($value: InputCreatePet!) {
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
    assert not result.errors
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
    assert not result.errors
    assert result.data["updatePetName"]["name"] == "eeee"
