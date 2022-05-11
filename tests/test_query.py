import graphene
import pytest
import sqlalchemy as sa
from graphene import Context

from .models import Article, Base, CompositeFullName, Editor, HairKind, Pet, Reporter
from .utils import to_std_dicts
from alchql.converter import convert_sqlalchemy_composite
from alchql.fields import SQLAlchemyConnectionField
from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import ORMField, SQLAlchemyObjectType


async def add_test_data(session):
    reporter_id = (
        await session.execute(
            sa.insert(Reporter).values(
                {
                    Reporter.first_name: "John",
                    Reporter.last_name: "Doe",
                    Reporter.favorite_pet_kind: "cat",
                }
            )
        )
    ).lastrowid

    await session.execute(
        sa.insert(Pet).values(
            {
                Pet.name: "Garfield",
                Pet.pet_kind: "cat",
                Pet.hair_kind: HairKind.SHORT,
                Pet.reporter_id: reporter_id,
            }
        )
    )

    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Hi!",
                Article.reporter_id: reporter_id,
            }
        )
    )

    reporter_id = (
        await session.execute(
            sa.insert(Reporter).values(
                {
                    Reporter.first_name: "Jane",
                    Reporter.last_name: "Roe",
                    Reporter.favorite_pet_kind: "dog",
                }
            )
        )
    ).lastrowid

    await session.execute(
        sa.insert(Pet).values(
            {
                Pet.name: "Lassie",
                Pet.pet_kind: "dog",
                Pet.hair_kind: HairKind.LONG,
                Pet.reporter_id: reporter_id,
            }
        )
    )

    await session.execute(
        sa.insert(Editor).values(
            {
                Editor.name: "Jack",
            }
        )
    )


@pytest.mark.asyncio
async def test_query_fields(session):
    await add_test_data(session)

    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.String()

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)
        reporters = graphene.List(ReporterType)

        async def resolve_reporter(self, _info):
            _result = await session.execute(sa.select(Reporter))
            return _result.scalars().first()

        async def resolve_reporters(self, _info):
            _result = await session.execute(sa.select(Reporter))
            return _result.scalars().all()

    query = """
        query {
          reporter {
            firstName
            columnProp
            hybridProp
            compositeProp
          }
          reporters {
            firstName
          }
        }
    """
    expected = {
        "reporter": {
            "firstName": "John",
            "hybridProp": "John",
            "columnProp": 2,
            "compositeProp": "John Doe",
        },
        "reporters": [
            {"firstName": "John"},
            {"firstName": "Jane"},
        ],
    }
    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(query)
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


@pytest.mark.asyncio
async def test_query_node(session):
    await add_test_data(session)

    class ReporterNode(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (AsyncNode,)

        @classmethod
        def get_node(cls, info, id):
            return Reporter(id=2, first_name="Cookie Monster")

    class ArticleNode(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        reporter = graphene.Field(ReporterNode)
        all_articles = SQLAlchemyConnectionField(ArticleNode.connection)

        async def resolve_reporter(self, _info):
            return (await session.execute(sa.select(Reporter))).scalars().first()

    query = """
        query {
          reporter {
            id
            firstName
            articles {
              edges {
                node {
                  headline
                }
              }
            }
          }
          allArticles {
            edges {
              node {
                headline
              }
            }
          }
          myArticle: node(id:"QXJ0aWNsZU5vZGU6MQ==") {
            id
            ... on ReporterNode {
                firstName
            }
            ... on ArticleNode {
                headline
            }
          }
        }
    """
    expected = {
        "reporter": {
            "id": "UmVwb3J0ZXJOb2RlOjE=",
            "firstName": "John",
            "articles": {"edges": [{"node": {"headline": "Hi!"}}]},
        },
        "allArticles": {"edges": [{"node": {"headline": "Hi!"}}]},
        "myArticle": {"id": "QXJ0aWNsZU5vZGU6MQ==", "headline": "Hi!"},
    }
    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Article, Reporter]),
        ],
    )
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


@pytest.mark.asyncio
async def test_orm_field(session):
    await add_test_data(session)

    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.String()

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (AsyncNode,)

        first_name_v2 = ORMField(model_attr="first_name")
        hybrid_prop_v2 = ORMField(model_attr="hybrid_prop")
        column_prop_v2 = ORMField(model_attr="column_prop")
        composite_prop = ORMField()
        favorite_article_v2 = ORMField(model_attr="favorite_article")

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)

        async def resolve_reporter(self, _info):
            return (await session.execute(sa.select(Reporter))).scalars().first()

    query = """
        query {
          reporter {
            firstNameV2
            hybridPropV2
            columnPropV2
            compositeProp
            favoriteArticleV2 {
              headline
            }
          }
        }
    """
    expected = {
        "reporter": {
            "firstNameV2": "John",
            "hybridPropV2": "John",
            "columnPropV2": 2,
            "compositeProp": "John Doe",
            "favoriteArticleV2": {"headline": "Hi!"},
        },
    }
    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Article, Reporter]),
        ],
    )
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


@pytest.mark.asyncio
async def test_custom_identifier(session):
    await add_test_data(session)

    class EditorNode(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()
        all_editors = SQLAlchemyConnectionField(EditorNode.connection)

    query = """
        query {
          allEditors {
            edges {
                node {
                    id
                    name
                }
            }
          },
          node(id: "RWRpdG9yTm9kZTox") {
            ...on EditorNode {
              name
            }
          }
        }
    """
    expected = {
        "allEditors": {"edges": [{"node": {"id": "RWRpdG9yTm9kZTox", "name": "Jack"}}]},
        "node": {"name": "Jack"},
    }

    schema = graphene.Schema(query=Query)
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware([Article, Reporter]),
        ],
    )
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


@pytest.mark.asyncio
async def test_mutation(session):
    await add_test_data(session)

    class EditorNode(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (AsyncNode,)

    class ReporterNode(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (AsyncNode,)

        @classmethod
        async def get_node(cls, id, info):
            return Reporter(id=2, first_name="Cookie Monster")

    class ArticleNode(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (AsyncNode,)

    class CreateArticle(graphene.Mutation):
        class Arguments:
            headline = graphene.String()
            reporter_id = graphene.ID()

        ok = graphene.Boolean()
        article = graphene.Field(ArticleNode)

        async def mutate(self, info, headline, reporter_id):
            s = info.context.session
            _result = await s.execute(
                sa.insert(Article).values(
                    dict(headline=headline, reporter_id=reporter_id)
                )
            )
            _result2 = await s.execute(
                sa.select(Article).where(Article.id == _result.lastrowid)
            )
            new_article = _result2.scalars().first()

            return CreateArticle(article=new_article, ok=True)

    class Query(graphene.ObjectType):
        node = AsyncNode.Field()

    class Mutation(graphene.ObjectType):
        create_article = CreateArticle.Field()

    query = """
        mutation {
          createArticle(
            headline: "My Article"
            reporterId: "1"
          ) {
            ok
            article {
                headline
                reporter {
                    id
                    firstName
                }
            }
          }
        }
    """
    expected = {
        "createArticle": {
            "ok": True,
            "article": {
                "headline": "My Article",
                "reporter": {"id": "UmVwb3J0ZXJOb2RlOjE=", "firstName": "John"},
            },
        }
    }

    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = await schema.execute_async(
        query,
        context_value=Context(session=session),
        middleware=[
            LoaderMiddleware(Base.registry.mappers),
        ],
    )
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected
