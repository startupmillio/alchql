from unittest import mock

import pytest
import sqlalchemy as sa
from graphene import (
    Context,
    Dynamic,
    Field,
    GlobalID,
    Int,
    List,
    Node,
    NonNull,
    ObjectType,
    Schema,
    String,
)
from graphene.relay import Connection
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import declarative_base, relationship

from alchql import gql_types
from alchql.converter import convert_sqlalchemy_composite
from alchql.fields import (
    BatchSQLAlchemyConnectionField,
    SQLAlchemyConnectionField,
    UnsortedSQLAlchemyConnectionField,
)
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import (
    ORMField,
    SQLAlchemyObjectType,
    SQLAlchemyObjectTypeOptions,
)
from .models import Article, CompositeFullName, Pet, Reporter


def test_should_raise_if_no_model():
    re_err = r"valid SQLAlchemy Model"
    with pytest.raises(Exception, match=re_err):

        class Character1(SQLAlchemyObjectType):
            pass


def test_should_raise_if_model_is_invalid():
    re_err = r"valid SQLAlchemy Model"
    with pytest.raises(Exception, match=re_err):

        class Character(SQLAlchemyObjectType):
            class Meta:
                model = 1


@pytest.mark.asyncio
async def test_sqlalchemy_node(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    reporter_id_field = ReporterType._meta.fields["id"]
    assert isinstance(reporter_id_field, GlobalID)

    reporter_id = (await session.execute(sa.insert(Reporter))).lastrowid
    reporter: Reporter = (
        await session.execute(
            sa.select(Reporter.__table__.c).where(Reporter.id == reporter_id)
        )
    ).first()

    info = mock.Mock(context=type("Context", (), {"session": session}))
    reporter_node = await ReporterType.get_node(info, reporter.id)
    assert reporter.id == reporter_node.id
    assert reporter.first_name == reporter_node.first_name
    assert reporter.last_name == reporter_node.last_name
    assert reporter.email == reporter_node.email
    assert reporter.favorite_pet_kind == reporter_node.favorite_pet_kind


@pytest.mark.asyncio
async def test_sqlalchemy_node_with_exclude(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)
            exclude_fields = ["first_name", "last_name"]

        other = gql_types.String()

    reporter_id_field = ReporterType._meta.fields["id"]
    assert isinstance(reporter_id_field, GlobalID)

    reporter_id = (await session.execute(sa.insert(Reporter))).lastrowid
    reporter: Reporter = (
        await session.execute(
            sa.select(Reporter.__table__.c).where(Reporter.id == reporter_id)
        )
    ).first()

    info = mock.Mock(context=type("Context", (), {"session": session}))
    reporter_node = await ReporterType.get_node(info, reporter.id)
    assert reporter.id == reporter_node.id
    assert reporter.email == reporter_node.email
    assert reporter.favorite_pet_kind == reporter_node.favorite_pet_kind
    assert reporter_node.other is None


def test_connection():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    assert issubclass(ReporterType.connection, Connection)


def test_sqlalchemy_default_fields():
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return String()

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    assert set(ReporterType._meta.fields.keys()) == {
        # Columns
        "column_prop",  # SQLAlchemy retuns column properties first
        "id",
        "first_name",
        "last_name",
        "email",
        "favorite_pet_kind",
        # Composite
        "composite_prop",
        # Hybrid
        "hybrid_prop",
        # Relationship
        "pets",
        "articles",
        "favorite_article",
    }

    # column
    first_name_field = ReporterType._meta.fields["first_name"]
    assert first_name_field.type == String
    assert first_name_field.description == "First name"

    # column_property
    column_prop_field = ReporterType._meta.fields["column_prop"]
    assert column_prop_field.type == Int
    # "doc" is ignored by column_property
    assert column_prop_field.description is None

    # composite
    full_name_field = ReporterType._meta.fields["composite_prop"]
    assert full_name_field.type == String
    # "doc" is ignored by composite
    assert full_name_field.description is None

    # hybrid_property
    hybrid_prop = ReporterType._meta.fields["hybrid_prop"]
    assert hybrid_prop.type == String
    # "doc" is ignored by hybrid_property
    assert hybrid_prop.description is None

    # relationship
    favorite_article_field = ReporterType._meta.fields["favorite_article"]
    assert isinstance(favorite_article_field, Dynamic)
    assert favorite_article_field.type().type == ArticleType
    assert favorite_article_field.type().description is None


def test_sqlalchemy_override_fields():
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return String()

    class ReporterMixin(object):
        # columns
        first_name = ORMField(required=True)
        last_name = ORMField(description="Overridden")

    class ReporterType(SQLAlchemyObjectType, ReporterMixin):
        class Meta:
            model = Reporter
            interfaces = (Node,)

        # columns
        email = ORMField(deprecation_reason="Overridden")
        email_v2 = ORMField(model_attr="email", type_=Int)

        # column_property
        column_prop = ORMField(type_=String)

        # composite
        composite_prop = ORMField()

        # hybrid_property
        hybrid_prop = ORMField(description="Overridden")

        # relationships
        favorite_article = ORMField(description="Overridden")
        pets = ORMField(description="Overridden")

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (Node,)
            use_connection = False

    assert set(ReporterType._meta.fields.keys()) == {
        # Fields from ReporterMixin
        "first_name",
        "last_name",
        # Fields from ReporterType
        "email",
        "email_v2",
        "column_prop",
        "composite_prop",
        "hybrid_prop",
        "favorite_article",
        "pets",
        "articles",
        # Then the automatic SQLAlchemy fields
        "id",
        "favorite_pet_kind",
    }

    first_name_field = ReporterType._meta.fields["first_name"]
    assert isinstance(first_name_field.type, NonNull)
    assert first_name_field.type.of_type == String
    assert first_name_field.description == "First name"
    assert first_name_field.deprecation_reason is None

    last_name_field = ReporterType._meta.fields["last_name"]
    assert last_name_field.type == String
    assert last_name_field.description == "Overridden"
    assert last_name_field.deprecation_reason is None

    email_field = ReporterType._meta.fields["email"]
    assert email_field.type == String
    assert email_field.description == "Email"
    assert email_field.deprecation_reason == "Overridden"

    email_field_v2 = ReporterType._meta.fields["email_v2"]
    assert email_field_v2.type == Int
    assert email_field_v2.description == "Email"
    assert email_field_v2.deprecation_reason is None

    hybrid_prop_field = ReporterType._meta.fields["hybrid_prop"]
    assert hybrid_prop_field.type == String
    assert hybrid_prop_field.description == "Overridden"
    assert hybrid_prop_field.deprecation_reason is None

    column_prop_field_v2 = ReporterType._meta.fields["column_prop"]
    assert column_prop_field_v2.type == String
    assert column_prop_field_v2.description is None
    assert column_prop_field_v2.deprecation_reason is None

    composite_prop_field = ReporterType._meta.fields["composite_prop"]
    assert composite_prop_field.type == String
    assert composite_prop_field.description is None
    assert composite_prop_field.deprecation_reason is None

    favorite_article_field = ReporterType._meta.fields["favorite_article"]
    assert isinstance(favorite_article_field, Dynamic)
    assert favorite_article_field.type().type == ArticleType
    assert favorite_article_field.type().description == "Overridden"

    articles_field = ReporterType._meta.fields["articles"]
    assert isinstance(articles_field, Dynamic)
    assert isinstance(articles_field.type(), UnsortedSQLAlchemyConnectionField)
    # assert articles_field.type().deprecation_reason == "Overridden"

    pets_field = ReporterType._meta.fields["pets"]
    assert isinstance(pets_field, Dynamic)
    assert isinstance(pets_field.type().type, List)
    assert pets_field.type().type.of_type == PetType
    assert pets_field.type().description == "Overridden"


def test_invalid_model_attr():
    err_msg = (
        "Cannot map ORMField to a model attribute.\n" "Field: 'ReporterType.first_name'"
    )
    with pytest.raises(ValueError, match=err_msg):

        class ReporterType(SQLAlchemyObjectType):
            class Meta:
                model = Reporter

            first_name = ORMField(model_attr="does_not_exist")


def test_only_fields():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            only_fields = ("id", "last_name")

        first_name = ORMField()  # Takes precedence
        last_name = ORMField()  # Noop

    assert list(ReporterType._meta.fields.keys()) == ["first_name", "last_name", "id"]


def test_exclude_fields():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            exclude_fields = ("id", "first_name")

        first_name = ORMField()  # Takes precedence
        last_name = ORMField()  # Noop

    assert set(ReporterType._meta.fields.keys()) == {
        "first_name",
        "last_name",
        "column_prop",
        "email",
        "favorite_pet_kind",
        "composite_prop",
        "hybrid_prop",
        "pets",
        "articles",
        "favorite_article",
    }


def test_only_and_exclude_fields():
    re_err = r"'only_fields' and 'exclude_fields' cannot be both set"
    with pytest.raises(Exception, match=re_err):

        class ReporterType(SQLAlchemyObjectType):
            class Meta:
                model = Reporter
                only_fields = ("id", "last_name")
                exclude_fields = ("id", "last_name")


def test_sqlalchemy_redefine_field():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

        first_name = Int()

    first_name_field = ReporterType._meta.fields["first_name"]
    assert isinstance(first_name_field, Field)
    assert first_name_field.type == Int


def test_sqlalchemy_redefine_field2():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

        first_name = gql_types.Int(model_field=Reporter.first_name)

    first_name_field = ReporterType._meta.fields["first_name"]
    assert isinstance(first_name_field, Field)
    assert first_name_field.type == gql_types.Int
    assert first_name_field.type != Int


@pytest.mark.asyncio
async def test_sqlalchemy_redefine_field3(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)

        fname = gql_types.String(model_field=Reporter.first_name, name="nameObj")

    class Query(ObjectType):
        reporter = SQLAlchemyConnectionField(ReporterType.connection, sort=None)

    reporter = Reporter(
        first_name="first_name",
        last_name="last_name",
        email="email",
        favorite_pet_kind="cat",
    )
    session.add(reporter)
    await session.commit()

    schema = Schema(query=Query, types=[ReporterType])
    result = await schema.execute_async(
        """
        query {
            reporter {
                edges{
                    node{
                        id
                        nameObj
                    }
                }
            }
        }
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Reporter]),
        ],
    )

    assert not result.errors
    assert result.data["reporter"]["edges"][0]["node"]["nameObj"] == "first_name"


@pytest.mark.asyncio
async def test_sqlalchemy_redefine_field4(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship
            interfaces = (AsyncNode,)

        fname = gql_types.String(model_field=Reporter.first_name)

    class Query(ObjectType):
        reporter = SQLAlchemyConnectionField(ReporterType.connection, sort=None)

    reporter = Reporter(
        first_name="first_name",
        last_name="last_name",
        email="email",
        favorite_pet_kind="cat",
    )
    session.add(reporter)
    await session.commit()

    schema = Schema(query=Query, types=[ReporterType])
    result = await schema.execute_async(
        """
        query {
            reporter {
                edges{
                    node{
                        id
                        fname
                    }
                }
            }
        }
        """,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Reporter]),
        ],
    )

    assert not result.errors
    assert result.data["reporter"]["edges"][0]["node"]["fname"] == "first_name"


@pytest.mark.asyncio
async def test_resolvers(session):
    """Test that the correct resolver functions are called"""

    class ReporterMixin(object):
        def resolve_id(root, _info):
            return "ID"

    class ReporterType(ReporterMixin, SQLAlchemyObjectType):
        class Meta:
            model = Reporter

        email = ORMField()
        email_v2 = ORMField(model_attr="email")
        favorite_pet_kind = Field(String)
        favorite_pet_kind_v2 = Field(String)

        def resolve_last_name(root, _info):
            return root.last_name.upper()

        def resolve_email_v2(root, _info):
            return root.email + "_V2"

        def resolve_favorite_pet_kind_v2(root, _info):
            return str(root.favorite_pet_kind) + "_V2"

    class Query(ObjectType):
        reporter = Field(ReporterType)

        async def resolve_reporter(self, _info):
            return (await session.execute(sa.select(Reporter))).scalars().first()

    reporter = Reporter(
        first_name="first_name",
        last_name="last_name",
        email="email",
        favorite_pet_kind="cat",
    )
    session.add(reporter)
    await session.commit()

    schema = Schema(query=Query)
    result = await schema.execute_async(
        """
        query {
            reporter {
                id
                firstName
                lastName
                email
                emailV2
                favoritePetKind
                favoritePetKindV2
            }
        }
    """
    )

    assert not result.errors
    # Custom resolver on a base class
    assert result.data["reporter"]["id"] == "ID"
    # Default field + default resolver
    assert result.data["reporter"]["firstName"] == "first_name"
    # Default field + custom resolver
    assert result.data["reporter"]["lastName"] == "LAST_NAME"
    # ORMField + default resolver
    assert result.data["reporter"]["email"] == "email"
    # ORMField + custom resolver
    assert result.data["reporter"]["emailV2"] == "email_V2"
    # Field + default resolver
    assert result.data["reporter"]["favoritePetKind"] == "cat"
    # Field + custom resolver
    assert result.data["reporter"]["favoritePetKindV2"] == "cat_V2"


# Test Custom SQLAlchemyObjectType Implementation


def test_custom_objecttype_registered():
    class CustomSQLAlchemyObjectType(SQLAlchemyObjectType):
        class Meta:
            abstract = True

    class CustomReporterType(CustomSQLAlchemyObjectType):
        class Meta:
            model = Reporter

    assert issubclass(CustomReporterType, ObjectType)
    assert CustomReporterType._meta.model == Reporter
    assert len(CustomReporterType._meta.fields) == 11


# Test Custom SQLAlchemyObjectType with Custom Options
def test_objecttype_with_custom_options():
    class CustomOptions(SQLAlchemyObjectTypeOptions):
        custom_option = None

    class SQLAlchemyObjectTypeWithCustomOptions(SQLAlchemyObjectType):
        class Meta:
            abstract = True

        @classmethod
        def __init_subclass_with_meta__(cls, custom_option=None, **options):
            _meta = CustomOptions(cls)
            _meta.custom_option = custom_option
            super().__init_subclass_with_meta__(_meta=_meta, **options)

    class ReporterWithCustomOptions(SQLAlchemyObjectTypeWithCustomOptions):
        class Meta:
            model = Reporter
            custom_option = "custom_option"

    assert issubclass(ReporterWithCustomOptions, ObjectType)
    assert ReporterWithCustomOptions._meta.model == Reporter
    assert ReporterWithCustomOptions._meta.custom_option == "custom_option"


# Tests for connection_field_factory


class _TestSQLAlchemyConnectionField(SQLAlchemyConnectionField):
    pass


def test_default_connection_field_factory():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    assert isinstance(
        ReporterType._meta.fields["articles"].type(), UnsortedSQLAlchemyConnectionField
    )


def test_custom_connection_field_factory():
    _Base = declarative_base()

    def test_connection_field_factory(relationship, registry):
        model = relationship.mapper.entity
        _type = registry.get_type_for_model(model)
        return _TestSQLAlchemyConnectionField(_type._meta.connection)

    class _Article(_Base):
        __tablename__ = "articles"

        id = Column(Integer(), primary_key=True)
        reporter_id = Column(Integer(), ForeignKey("reporters.id"))

    class _Reporter(_Base):
        __tablename__ = "reporters"

        id = Column(Integer(), primary_key=True)
        articles = relationship(_Article, backref="reporter")

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = _Reporter
            interfaces = (Node,)
            connection_field_factory = test_connection_field_factory

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = _Article
            interfaces = (Node,)

    assert isinstance(
        ReporterType._meta.fields["articles"].type(),
        _TestSQLAlchemyConnectionField,
    )
