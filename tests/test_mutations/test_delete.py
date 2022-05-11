import graphene
import pytest
from graphene import Context

from alchql.fields import SQLAlchemyConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.sql_mutation import (
    SQLAlchemyDeleteMutation,
)
from alchql.types import SQLAlchemyObjectType
from tests.models import Pet
from tests.test_query import add_test_data


@pytest.mark.asyncio
async def test_delete_mutation(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

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
