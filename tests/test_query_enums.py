import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from .models import HairKind, Pet, Reporter
from .test_query import add_test_data, to_std_dicts
from alchql.middlewares import LoaderMiddleware
from alchql.types import SQLAlchemyObjectType


@pytest.mark.asyncio
async def test_query_pet_kinds(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

        async def resolve_pets(self, _info):
            s = _info.context.session
            q = sa.select(Pet).where(Pet.reporter_id == self.id)
            _result = await s.execute(q)

            return _result.scalars().all()

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)
        reporters = graphene.List(ReporterType)
        pets = graphene.List(
            PetType, kind=graphene.Argument(PetType.enum_for_field("pet_kind"))
        )

        async def resolve_reporter(self, _info):
            s = _info.context.session
            _result = await s.execute(sa.select(Reporter))
            return _result.scalars().first()

        async def resolve_reporters(self, _info):
            s = _info.context.session
            _result = await s.execute(sa.select(Reporter))
            return _result.scalars().all()

        async def resolve_pets(self, _info, kind):
            s = _info.context.session
            q = sa.select(Pet)
            if kind:
                q = q.where(Pet.pet_kind == kind.value)

            _result = await s.execute(q)

            return _result.scalars().all()

    query = """
        query ReporterQuery {
          reporter {
            firstName
            lastName
            email
            favoritePetKind
            pets {
              name
              petKind
            }
          }
          reporters {
            firstName
            favoritePetKind
          }
          pets(kind: DOG) {
            name
            petKind
          }
        }
    """
    expected = {
        "reporter": {
            "firstName": "John",
            "lastName": "Doe",
            "email": None,
            "favoritePetKind": "CAT",
            "pets": [{"name": "Garfield", "petKind": "CAT"}],
        },
        "reporters": [
            {
                "firstName": "John",
                "favoritePetKind": "CAT",
            },
            {
                "firstName": "Jane",
                "favoritePetKind": "DOG",
            },
        ],
        "pets": [{"name": "Lassie", "petKind": "DOG"}],
    }
    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(
        query,
        context=Context(session=session),
        middleware=[
            LoaderMiddleware([Reporter, Pet]),
        ],
    )
    assert not result.errors
    assert result.data == expected


@pytest.mark.asyncio
async def test_query_more_enums(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(PetType)

        async def resolve_pet(self, _info):
            _result = await session.execute(sa.select(Pet))
            return _result.scalars().first()

    query = """
        query PetQuery {
          pet {
            name,
            petKind
            hairKind
          }
        }
    """
    expected = {"pet": {"name": "Garfield", "petKind": "CAT", "hairKind": "SHORT"}}
    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(query)
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


@pytest.mark.asyncio
async def test_enum_as_argument(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(
            PetType, kind=graphene.Argument(PetType.enum_for_field("pet_kind"))
        )

        async def resolve_pet(self, info, kind=None):
            q = sa.select(Pet)
            if kind:
                q = q.where(Pet.pet_kind == kind.value)
            return (await session.execute(q)).scalars().first()

    query = """
        query PetQuery($kind: PetKind) {
          pet(kind: $kind) {
            name,
            petKind
            hairKind
          }
        }
    """

    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(query, variables={"kind": "CAT"})
    assert not result.errors
    expected = {"pet": {"name": "Garfield", "petKind": "CAT", "hairKind": "SHORT"}}
    assert result.data == expected
    result = await schema.execute_async(query, variables={"kind": "DOG"})
    assert not result.errors
    expected = {"pet": {"name": "Lassie", "petKind": "DOG", "hairKind": "LONG"}}
    result = to_std_dicts(result.data)
    assert result == expected


@pytest.mark.asyncio
async def test_py_enum_as_argument(session):
    await add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(
            PetType,
            kind=graphene.Argument(PetType._meta.fields["hair_kind"].type.of_type),
        )

        async def resolve_pet(self, _info, kind=None):
            query = sa.select(Pet)
            if kind:
                # enum arguments are expected to be strings, not PyEnums
                query = query.where(Pet.hair_kind == HairKind(kind))
            return (await session.execute(query)).scalars().first()

    query = """
        query PetQuery($kind: HairKind) {
          pet(kind: $kind) {
            name,
            petKind
            hairKind
          }
        }
    """

    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(query, variables={"kind": "SHORT"})
    assert not result.errors
    expected = {"pet": {"name": "Garfield", "petKind": "CAT", "hairKind": "SHORT"}}
    assert result.data == expected
    result = await schema.execute_async(query, variables={"kind": "LONG"})
    assert not result.errors
    expected = {"pet": {"name": "Lassie", "petKind": "DOG", "hairKind": "LONG"}}
    result = to_std_dicts(result.data)
    assert result == expected
