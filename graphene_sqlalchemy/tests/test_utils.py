import pytest
import sqlalchemy as sa
from graphene import Context, Enum, List, ObjectType, Schema, String

from .models import Base, Editor, Pet
from ..utils import (
    get_session,
    sort_argument_for_model,
    sort_enum_for_model,
    to_enum_value_name,
    to_type_name,
)


@pytest.mark.asyncio
async def test_get_session():
    session = "My SQLAlchemy session"

    class Query(ObjectType):
        x = String()

        def resolve_x(self, info):
            return get_session(info.context)

    query = """
        query ReporterQuery {
            x
        }
    """

    schema = Schema(query=Query)
    result = await schema.execute_async(query, context_value=Context(session=session))
    assert not result.errors
    assert result.data["x"] == session


def test_to_type_name():
    assert to_type_name("make_camel_case") == "MakeCamelCase"
    assert to_type_name("AlreadyCamelCase") == "AlreadyCamelCase"
    assert to_type_name("A_Snake_and_a_Camel") == "ASnakeAndACamel"


def test_to_enum_value_name():
    assert to_enum_value_name("make_enum_value_name") == "MAKE_ENUM_VALUE_NAME"
    assert to_enum_value_name("makeEnumValueName") == "MAKE_ENUM_VALUE_NAME"
    assert to_enum_value_name("HTTPStatus400Message") == "HTTP_STATUS400_MESSAGE"
    assert to_enum_value_name("ALREADY_ENUM_VALUE_NAME") == "ALREADY_ENUM_VALUE_NAME"
