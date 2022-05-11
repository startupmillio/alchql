import pytest
from graphene import Context, ObjectType, Schema, String

from alchql.gql_fields import camel_to_snake
from alchql.utils import (
    to_enum_value_name,
    to_type_name,
)


@pytest.mark.asyncio
async def test_get_session():
    session = "My SQLAlchemy session"

    class Query(ObjectType):
        x = String()

        def resolve_x(self, info):
            return info.context.session

    query = """
        query ReporterQuery {
            x
        }
    """

    schema = Schema(query=Query)
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
    )
    assert not result.errors
    assert result.data["x"] == session


def test_to_type_name():
    assert to_type_name("make_camel_case") == "MakeCamelCase"
    assert to_type_name("AlreadyCamelCase") == "AlreadyCamelCase"
    assert to_type_name("A_Snake_and_a_Camel") == "ASnakeAndACamel"


def test_camel_to_snake():
    assert camel_to_snake("howYouLikeThat_Eq") == "how_you_like_that__eq"
    assert camel_to_snake("howyoulikethat") == "howyoulikethat"
    assert camel_to_snake("howYouLike000that") == "how_you_like_000that"
    assert camel_to_snake("h000YouLikeThat") == "h000_you_like_that"
    assert camel_to_snake("artistId_Eq") == "artist_id__eq"
    assert camel_to_snake("sort") == "sort"
    assert camel_to_snake("priceMomentum12mo") == "price_momentum_12mo"
    assert camel_to_snake("s3PresignedUrl") == "s3_presigned_url"
    assert camel_to_snake("thumbnailS3PresignedUrl") == "thumbnail_s3_presigned_url"


def test_to_enum_value_name():
    assert to_enum_value_name("make_enum_value_name") == "MAKE_ENUM_VALUE_NAME"
    assert to_enum_value_name("makeEnumValueName") == "MAKE_ENUM_VALUE_NAME"
    assert to_enum_value_name("HTTPStatus400Message") == "HTTP_STATUS400_MESSAGE"
    assert to_enum_value_name("ALREADY_ENUM_VALUE_NAME") == "ALREADY_ENUM_VALUE_NAME"
