import pytest
from graphene import Context, Enum as GrapheneEnum, ObjectType, Schema
from sqlalchemy.types import Enum as SQLAlchemyEnum

from alchql import gql_types
from alchql.fields import BatchSQLAlchemyConnectionField, SQLAlchemyConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.registry import Registry
from alchql.types import SQLAlchemyObjectType
from alchql.utils import EnumValue
from .models import Editor, Pet


def test_register_object_type():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    reg.register(PetType)
    assert reg.get_type_for_model(Pet) is PetType


def test_register_incorrect_object_type():
    reg = Registry()

    class Spam:
        pass

    re_err = "Expected SQLAlchemyObjectType, but got: .*Spam"
    with pytest.raises(TypeError, match=re_err):
        reg.register(Spam)


def test_register_orm_field():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    reg.register_orm_field(PetType, "name", Pet.name)
    assert reg.get_orm_field_for_graphene_field(PetType, "name") is Pet.name


def test_register_orm_field_incorrect_types():
    reg = Registry()

    class Spam:
        pass

    re_err = "Expected SQLAlchemyObjectType, but got: .*Spam"
    with pytest.raises(TypeError, match=re_err):
        reg.register_orm_field(Spam, "name", Pet.name)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    re_err = "Expected a field name, but got: .*Spam"
    with pytest.raises(TypeError, match=re_err):
        reg.register_orm_field(PetType, Spam, Pet.name)


def test_register_enum():
    reg = Registry()

    sa_enum = SQLAlchemyEnum("cat", "dog")
    graphene_enum = GrapheneEnum("PetKind", [("CAT", 1), ("DOG", 2)])

    reg.register_enum(sa_enum, graphene_enum)
    assert reg.get_graphene_enum_for_sa_enum(sa_enum) is graphene_enum


def test_register_enum_incorrect_types():
    reg = Registry()

    sa_enum = SQLAlchemyEnum("cat", "dog")
    graphene_enum = GrapheneEnum("PetKind", [("CAT", 1), ("DOG", 2)])

    re_err = r"Expected Graphene Enum, but got: Enum\('cat', 'dog'\)"
    with pytest.raises(TypeError, match=re_err):
        reg.register_enum(sa_enum, sa_enum)

    re_err = r"Expected SQLAlchemyEnumType, but got: .*PetKind.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_enum(graphene_enum, graphene_enum)


def test_register_sort_enum():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    sort_enum = GrapheneEnum(
        "PetSort",
        [("ID", EnumValue("id", Pet.id)), ("NAME", EnumValue("name", Pet.name))],
    )

    reg.register_sort_enum(PetType, sort_enum)
    assert reg.get_sort_enum_for_object_type(PetType) is sort_enum


def test_register_sort_enum_incorrect_types():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    sort_enum = GrapheneEnum(
        "PetSort",
        [("ID", EnumValue("id", Pet.id)), ("NAME", EnumValue("name", Pet.name))],
    )

    re_err = r"Expected SQLAlchemyObjectType, but got: .*PetSort.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_sort_enum(sort_enum, sort_enum)

    re_err = r"Expected Graphene Enum, but got: .*PetType.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_sort_enum(PetType, PetType)


@pytest.mark.asyncio
async def test_sqlalchemy_redefine_field4(session):
    class EditorType(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)

        pname = gql_types.String(model_field=Editor.name)

    class AnotherEditorType(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)

        dname = gql_types.String(model_field=Editor.name)

    class Query(ObjectType):
        editor = SQLAlchemyConnectionField(EditorType.connection, sort=None)
        another_editor = SQLAlchemyConnectionField(
            AnotherEditorType.connection, sort=None
        )

    editor = Editor(name="first_name")
    session.add(editor)
    await session.commit()

    schema = Schema(query=Query, types=[EditorType, AnotherEditorType])
    result = await schema.execute_async(
        """
        query {
            editor {
                edges {
                    node {
                        pname
                    }
                }
            }
            anotherEditor {
                edges {
                    node {
                        dname
                    }
                }
            }
        }
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Editor]),
        ],
    )

    assert not result.errors
    assert result.data["editor"]["edges"][0]["node"]["pname"] == "first_name"
    assert result.data["anotherEditor"]["edges"][0]["node"]["dname"] == "first_name"
