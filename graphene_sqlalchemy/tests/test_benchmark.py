import graphene
import pytest
from graphene import Context, relay

from .models import Article, HairKind, Pet, Reporter
from .utils import SessionMiddleware, is_sqlalchemy_version_less_than
from ..fields import BatchSQLAlchemyConnectionField
from ..loaders_middleware import LoaderMiddleware
from ..node import AsyncNode
from ..types import SQLAlchemyObjectType

if is_sqlalchemy_version_less_than('1.2'):
    pytest.skip('SQL batching only works for SQLAlchemy 1.2+', allow_module_level=True)


def get_schema():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (AsyncNode,)
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (AsyncNode,)
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)
            connection_field_factory = BatchSQLAlchemyConnectionField.from_relationship

    class Query(graphene.ObjectType):
        articles = graphene.Field(graphene.List(ArticleType))
        reporters = graphene.Field(graphene.List(ReporterType))

        async def resolve_articles(self, info):
            return info.context.session.query(Article).all()

        async def resolve_reporters(self, info):
            return info.context.session.query(Reporter).all()

    return graphene.Schema(query=Query)


async def benchmark_query(session_factory, benchmark, query):
    schema = get_schema()

    @benchmark
    async def execute_query():
        result = await schema.execute_async(
          query,
          context_value=Context(),
          middleware=[
            LoaderMiddleware([Article, Reporter, Pet]),
            SessionMiddleware(session_factory()),
          ]
        )
        assert not result.errors


@pytest.mark.asyncio
async def test_one_to_one(session_factory, benchmark):
    session = session_factory()

    reporter_1 = Reporter(
      first_name='Reporter_1',
    )
    session.add(reporter_1)
    reporter_2 = Reporter(
      first_name='Reporter_2',
    )
    session.add(reporter_2)

    article_1 = Article(headline='Article_1')
    article_1.reporter = reporter_1
    session.add(article_1)

    article_2 = Article(headline='Article_2')
    article_2.reporter = reporter_2
    session.add(article_2)

    session.commit()
    session.close()

    await benchmark_query(session_factory, benchmark, """
      query {
        reporters {
          firstName
          favoriteArticle {
            headline
          }
        }
      }
    """)


@pytest.mark.asyncio
async def test_many_to_one(session_factory, benchmark):
    session = session_factory()

    reporter_1 = Reporter(
      first_name='Reporter_1',
    )
    session.add(reporter_1)
    reporter_2 = Reporter(
      first_name='Reporter_2',
    )
    session.add(reporter_2)

    article_1 = Article(headline='Article_1')
    article_1.reporter = reporter_1
    session.add(article_1)

    article_2 = Article(headline='Article_2')
    article_2.reporter = reporter_2
    session.add(article_2)

    session.commit()
    session.close()

    await benchmark_query(session_factory, benchmark, """
      query {
        articles {
          headline
          reporter {
            firstName
          }
        }
      }
    """)


@pytest.mark.asyncio
async def test_one_to_many(session_factory, benchmark):
    session = session_factory()

    reporter_1 = Reporter(
      first_name='Reporter_1',
    )
    session.add(reporter_1)
    reporter_2 = Reporter(
      first_name='Reporter_2',
    )
    session.add(reporter_2)

    article_1 = Article(headline='Article_1')
    article_1.reporter = reporter_1
    session.add(article_1)

    article_2 = Article(headline='Article_2')
    article_2.reporter = reporter_1
    session.add(article_2)

    article_3 = Article(headline='Article_3')
    article_3.reporter = reporter_2
    session.add(article_3)

    article_4 = Article(headline='Article_4')
    article_4.reporter = reporter_2
    session.add(article_4)

    session.commit()
    session.close()

    await benchmark_query(session_factory, benchmark, """
      query {
        reporters {
          firstName
          articles(first: 2) {
            edges {
              node {
                headline
              }
            }
          }
        }
      }
    """)


@pytest.mark.asyncio
async def test_many_to_many(session_factory, benchmark):
    session = session_factory()

    reporter_1 = Reporter(
      first_name='Reporter_1',
    )
    session.add(reporter_1)
    reporter_2 = Reporter(
      first_name='Reporter_2',
    )
    session.add(reporter_2)

    pet_1 = Pet(name='Pet_1', pet_kind='cat', hair_kind=HairKind.LONG)
    session.add(pet_1)

    pet_2 = Pet(name='Pet_2', pet_kind='cat', hair_kind=HairKind.LONG)
    session.add(pet_2)

    reporter_1.pets.append(pet_1)
    reporter_1.pets.append(pet_2)

    pet_3 = Pet(name='Pet_3', pet_kind='cat', hair_kind=HairKind.LONG)
    session.add(pet_3)

    pet_4 = Pet(name='Pet_4', pet_kind='cat', hair_kind=HairKind.LONG)
    session.add(pet_4)

    reporter_2.pets.append(pet_3)
    reporter_2.pets.append(pet_4)

    session.commit()
    session.close()

    await benchmark_query(session_factory, benchmark, """
      query {
        reporters {
          firstName
          pets(first: 2) {
            edges {
              node {
                name
              }
            }
          }
        }
      }
    """)
