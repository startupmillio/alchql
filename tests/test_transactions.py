import json

import graphene
import pytest
import sqlalchemy as sa
from graphene import Context
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from alchql.app import SessionQLApp
from alchql.fields import SQLAlchemyConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.sql_mutation import (
    SQLAlchemyCreateMutation,
)
from alchql.types import SQLAlchemyObjectType
from tests import models as m
from .models import Base, HairKind


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


async def add_test_data(session):
    await session.execute(sa.delete(m.Pet))
    reporter_id = (
        await session.execute(
            sa.insert(m.Reporter).values(
                {
                    m.Reporter.first_name: "John",
                    m.Reporter.last_name: "Doe",
                    m.Reporter.favorite_pet_kind: "cat",
                }
            )
        )
    ).inserted_primary_key[0]

    await session.execute(
        sa.insert(m.Pet).values(
            {
                m.Pet.name: "Garfield",
                m.Pet.pet_kind: "cat",
                m.Pet.hair_kind: HairKind.SHORT,
                m.Pet.reporter_id: reporter_id,
            }
        )
    )

    await session.execute(
        sa.insert(m.Article).values(
            {
                m.Article.headline: "Hi!",
                m.Article.reporter_id: reporter_id,
            }
        )
    )

    reporter_id = (
        await session.execute(
            sa.insert(m.Reporter).values(
                {
                    m.Reporter.first_name: "Jane",
                    m.Reporter.last_name: "Roe",
                    m.Reporter.favorite_pet_kind: "dog",
                }
            )
        )
    ).inserted_primary_key[0]

    await session.execute(
        sa.insert(m.Pet).values(
            {
                m.Pet.name: "Lassie",
                m.Pet.pet_kind: "dog",
                m.Pet.hair_kind: HairKind.LONG,
                m.Pet.reporter_id: reporter_id,
            }
        )
    )

    await session.execute(
        sa.insert(m.Editor).values(
            {
                m.Editor.name: "Jack",
            }
        )
    )


@pytest.mark.asyncio
async def test_session_rollback():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        convert_unicode=True,
        echo=False,
    )
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with AsyncSession(engine) as session, session.begin():
        await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = m.Pet
            interfaces = (AsyncNode,)

    class MutationInsertPet(SQLAlchemyCreateMutation):
        class Meta:
            model = m.Pet
            output = PetType

        @classmethod
        async def mutate(cls, root, info, value: dict):
            await super().mutate(root, info, value)
            raise Exception("Test error")

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

    class Mutation(graphene.ObjectType):
        insert_pet = MutationInsertPet.Field()

    schema = graphene.Schema(
        query=Query,
        mutation=Mutation,
    )

    async with AsyncSession(engine) as session:
        all_pets = await get_all_pets(session, schema)

    start_names = {i["name"] for i in all_pets}

    assert start_names == {
        "Garfield",
        "Lassie",
    }

    query = """
        mutation UpdatePet($value: MutationInsertPetInputType!) {
            insertPet(value: $value) {
                name
            }
        }
    """
    variables = {
        "value": {
            "name": "Odin",
            "petKind": "CAT",
            "hairKind": "SHORT",
        },
    }

    async def receive():
        return {
            "type": "http.request",
            "body": b'{"query": "%s", "variables": %s}'
            % (query.replace("\n", "").encode(), json.dumps(variables).encode()),
        }

    result = {}

    async def send(data):
        nonlocal result
        if data["type"] == "http.response.body":
            result = json.loads(data["body"])

    app = SessionQLApp(
        schema=schema,
        engine=engine,
    )

    await app(
        scope={
            "type": "http",
            "method": "POST",
            "headers": [(b"content-type", b"application/json")],
        },
        receive=receive,
        send=send,
    )

    assert result["errors"][0]["message"] == "Test error"

    all_pets = await get_all_pets(session, schema)
    assert {i["name"] for i in all_pets} == start_names


@pytest.mark.asyncio
@pytest.mark.skip
async def test_session_readonly(raise_graphql):
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        # "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/test",
        convert_unicode=True,
        echo=False,
    )
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.drop_all)
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with AsyncSession(engine) as session, session.begin():
        await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = m.Pet
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_pets = SQLAlchemyConnectionField(PetType.connection)

        async def resolve_all_pets(self, _info, *args, **kwargs):
            s = _info.context.session

            await s.execute(
                sa.insert(m.Pet).values(
                    {
                        m.Pet.name: "Test",
                        m.Pet.pet_kind: "cat",
                        m.Pet.hair_kind: "long",
                    }
                )
            )

            _result = (await s.execute(sa.select(m.Pet))).scalars().fetchall()
            return _result

    schema = graphene.Schema(query=Query)

    async with AsyncSession(engine) as session:
        all_pets = (await session.execute(sa.select(m.Pet.name))).scalars()

    start_names = {i for i in all_pets}

    assert start_names == {
        "Garfield",
        "Lassie",
    }

    query = """
        query {
            allPets {
                edges {
                    node { 
                        name 
                    }
                }
            }
        }
    """

    async def receive():
        return {
            "type": "http.request",
            "body": b'{"query": "%s"}' % query.replace("\n", "").encode(),
        }

    result = {}

    async def send(data):
        nonlocal result
        if data["type"] == "http.response.body":
            result = json.loads(data["body"])

    app = SessionQLApp(
        schema=schema,
        engine=engine,
        middleware=[
            LoaderMiddleware([m.Pet]),
        ],
    )

    await app(
        scope={
            "type": "http",
            "method": "POST",
            "headers": [(b"content-type", b"application/json")],
        },
        receive=receive,
        send=send,
    )

    assert not result.get("errors"), result["errors"][0]

    async with AsyncSession(engine) as session:
        all_pets = (await session.execute(sa.select(m.Pet.name))).scalars()

    assert {i for i in all_pets} == start_names
