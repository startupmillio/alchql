import pytest
from graphene import NonNull, ObjectType
from graphene.relay import Connection, Node

from .models import Editor as EditorModel, Pet as PetModel
from alchql.fields import SQLAlchemyConnectionField
from alchql.types import SQLAlchemyObjectType


class Pet(SQLAlchemyObjectType):
    class Meta:
        model = PetModel
        interfaces = (Node,)


class Editor(SQLAlchemyObjectType):
    class Meta:
        model = EditorModel


##
# SQLAlchemyConnectionField
##


def test_nonnull_sqlalachemy_connection():
    field = SQLAlchemyConnectionField(NonNull(Pet.connection))
    assert isinstance(field.type, NonNull)
    assert issubclass(field.type.of_type, Connection)
    assert field.type.of_type._meta.node is Pet


def test_required_sqlalachemy_connection():
    field = SQLAlchemyConnectionField(Pet.connection, required=True)
    assert isinstance(field.type, NonNull)
    assert issubclass(field.type.of_type, Connection)
    assert field.type.of_type._meta.node is Pet


def test_type_assert_sqlalchemy_object_type():
    with pytest.raises(AssertionError, match="only accepts SQLAlchemyObjectType"):
        SQLAlchemyConnectionField(ObjectType).type


def test_type_assert_object_has_connection():
    with pytest.raises(AssertionError, match="doesn't have a connection"):
        SQLAlchemyConnectionField(Editor).type


##
# UnsortedSQLAlchemyConnectionField
##


def test_sort_added_by_default():
    field = SQLAlchemyConnectionField(Pet.connection)
    assert "sort" in field.args
    assert field.args["sort"] == Pet.sort_argument()


def test_sort_can_be_removed():
    field = SQLAlchemyConnectionField(Pet.connection, sort=None)
    assert "sort" not in field.args


def test_custom_sort():
    field = SQLAlchemyConnectionField(Pet.connection, sort=Editor.sort_argument())
    assert field.args["sort"] == Editor.sort_argument()


def test_sort_init_raises():
    with pytest.raises(TypeError, match="Cannot create sort"):
        SQLAlchemyConnectionField(Connection)
