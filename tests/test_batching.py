import contextlib
import logging

import graphene
import pytest
import sqlalchemy as sa
from graphene import Context, relay

from graphene_sqlalchemy_core.middlewares import LoaderMiddleware
from graphene_sqlalchemy_core.types import SQLAlchemyObjectType
from .models import Article, HairKind, Pet, Reporter, association_table
from .utils import to_std_dicts


class MockLoggingHandler(logging.Handler):
    """Intercept and store log messages in a list."""

    def __init__(self, *args, **kwargs):
        self.messages = []
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages.append(record.getMessage())


@contextlib.contextmanager
def mock_sqlalchemy_logging_handler():
    logging.basicConfig()
    sql_logger = logging.getLogger("sqlalchemy.engine")
    previous_level = sql_logger.level

    sql_logger.setLevel(logging.INFO)
    mock_logging_handler = MockLoggingHandler()
    mock_logging_handler.setLevel(logging.INFO)
    sql_logger.addHandler(mock_logging_handler)

    yield mock_logging_handler

    sql_logger.setLevel(previous_level)


def get_schema():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (relay.Node,)
            batching = True

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (relay.Node,)
            batching = True

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (relay.Node,)
            batching = True

    class Query(graphene.ObjectType):
        articles = graphene.Field(graphene.List(ArticleType))
        reporters = graphene.Field(graphene.List(ReporterType))

        async def resolve_articles(self, info):
            session = info.context.session
            result = await session.execute(sa.select(Article))
            return result.scalars()

        async def resolve_reporters(self, info):
            session = info.context.session
            result = await session.execute(sa.select(Reporter))
            return result.scalars().all()

    return graphene.Schema(query=Query)


@pytest.mark.asyncio
async def test_many_to_one(session, raise_graphql):
    await session.execute(sa.insert(Reporter).values({"first_name": "Reporter_1"}))
    await session.execute(sa.insert(Reporter).values({"first_name": "Reporter_2"}))

    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_1",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_1"
                ),
            }
        )
    )
    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_2",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_2"
                ),
            }
        )
    )

    schema = get_schema()

    with mock_sqlalchemy_logging_handler() as sqlalchemy_logging_handler:
        # Starts new session to fully reset the engine / connection logging level
        # session = session_factory()
        result = await schema.execute_async(
            """
              query {
                articles {
                  headline
                  reporter {
                    firstName
                    articles {
                      edges {
                        node {
                          headline
                        }
                      }
                    }
                  }
                }
              }
            """,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Article, Reporter]),
            ],
        )
        # messages = sqlalchemy_logging_handler.messages

    assert not result.errors
    # assert len(messages) == 5

    # assert messages == [
    #     'BEGIN (implicit)',
    #
    #     'SELECT articles.id AS articles_id, '
    #     'articles.headline AS articles_headline, '
    #     'articles.pub_date AS articles_pub_date, '
    #     'articles.reporter_id AS articles_reporter_id \n'
    #     'FROM articles',
    #     '()',
    #
    #     'SELECT reporters.id AS reporters_id, '
    #     '(SELECT CAST(count(reporters.id) AS INTEGER) AS anon_2 \nFROM reporters) AS anon_1, '
    #     'reporters.first_name AS reporters_first_name, '
    #     'reporters.last_name AS reporters_last_name, '
    #     'reporters.email AS reporters_email, '
    #     'reporters.favorite_pet_kind AS reporters_favorite_pet_kind \n'
    #     'FROM reporters \n'
    #     'WHERE reporters.id IN (?, ?)',
    #     '(1, 2)',
    # ]

    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == {
        "articles": [
            {
                "headline": "Article_1",
                "reporter": {
                    "articles": {"edges": [{"node": {"headline": "Article_1"}}]},
                    "firstName": "Reporter_1",
                },
            },
            {
                "headline": "Article_2",
                "reporter": {
                    "articles": {"edges": [{"node": {"headline": "Article_2"}}]},
                    "firstName": "Reporter_2",
                },
            },
        ]
    }


@pytest.mark.asyncio
async def test_one_to_one(session):
    await session.execute(
        sa.insert(Reporter),
        [
            {Reporter.first_name.key: "Reporter_1"},
            {Reporter.first_name.key: "Reporter_2"},
        ],
    )

    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_1",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_1"
                ),
            }
        )
    )
    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_2",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_2"
                ),
            }
        )
    )

    schema = get_schema()

    with mock_sqlalchemy_logging_handler() as sqlalchemy_logging_handler:
        # Starts new session to fully reset the engine / connection logging level
        result = await schema.execute_async(
            """
              query {
                reporters {
                  firstName
                  favoriteArticle {
                    headline
                  }
                }
              }
            """,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Article, Reporter]),
            ],
        )
        # messages = sqlalchemy_logging_handler.messages

    assert not result.errors, result.errors
    # assert len(messages) == 5

    # assert messages == [
    #     'BEGIN (implicit)',
    #
    #     'SELECT (SELECT CAST(count(reporters.id) AS INTEGER) AS anon_2 \nFROM reporters) AS anon_1, '
    #     'reporters.id AS reporters_id, '
    #     'reporters.first_name AS reporters_first_name, '
    #     'reporters.last_name AS reporters_last_name, '
    #     'reporters.email AS reporters_email, '
    #     'reporters.favorite_pet_kind AS reporters_favorite_pet_kind \n'
    #     'FROM reporters',
    #     '()',
    #
    #     'SELECT articles.reporter_id AS articles_reporter_id, '
    #     'articles.id AS articles_id, '
    #     'articles.headline AS articles_headline, '
    #     'articles.pub_date AS articles_pub_date \n'
    #     'FROM articles \n'
    #     'WHERE articles.reporter_id IN (?, ?)',
    #     '(1, 2)'
    # ]

    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == {
        "reporters": [
            {
                "firstName": "Reporter_1",
                "favoriteArticle": {
                    "headline": "Article_1",
                },
            },
            {
                "firstName": "Reporter_2",
                "favoriteArticle": {
                    "headline": "Article_2",
                },
            },
        ],
    }


@pytest.mark.asyncio
async def test_one_to_many(session, raise_graphql):
    await session.execute(
        sa.insert(Reporter),
        [
            {Reporter.first_name.key: "Reporter_1"},
            {Reporter.first_name.key: "Reporter_2"},
        ],
    )

    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_1",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_1"
                ),
            }
        )
    )
    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_2",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_1"
                ),
            }
        )
    )

    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_3",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_2"
                ),
            }
        )
    )

    await session.execute(
        sa.insert(Article).values(
            {
                Article.headline: "Article_4",
                Article.reporter_id: sa.select(Reporter.id).where(
                    Reporter.first_name == "Reporter_2"
                ),
            }
        )
    )

    schema = get_schema()

    with mock_sqlalchemy_logging_handler() as sqlalchemy_logging_handler:
        # Starts new session to fully reset the engine / connection logging level
        result = await schema.execute_async(
            """
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
            """,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Article, Reporter]),
            ],
        )
        # messages = sqlalchemy_logging_handler.messages

    assert not result.errors, result.errors
    # assert len(messages) == 5

    # assert messages == [
    #     'BEGIN (implicit)',
    #
    #     'SELECT (SELECT CAST(count(reporters.id) AS INTEGER) AS anon_2 \nFROM reporters) AS anon_1, '
    #     'reporters.id AS reporters_id, '
    #     'reporters.first_name AS reporters_first_name, '
    #     'reporters.last_name AS reporters_last_name, '
    #     'reporters.email AS reporters_email, '
    #     'reporters.favorite_pet_kind AS reporters_favorite_pet_kind \n'
    #     'FROM reporters',
    #     '()',
    #
    #     'SELECT articles.reporter_id AS articles_reporter_id, '
    #     'articles.id AS articles_id, '
    #     'articles.headline AS articles_headline, '
    #     'articles.pub_date AS articles_pub_date \n'
    #     'FROM articles \n'
    #     'WHERE articles.reporter_id IN (?, ?)',
    #     '(1, 2)'
    # ]

    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == {
        "reporters": [
            {
                "firstName": "Reporter_1",
                "articles": {
                    "edges": [
                        {
                            "node": {
                                "headline": "Article_1",
                            },
                        },
                        {
                            "node": {
                                "headline": "Article_2",
                            },
                        },
                    ],
                },
            },
            {
                "firstName": "Reporter_2",
                "articles": {
                    "edges": [
                        {
                            "node": {
                                "headline": "Article_3",
                            },
                        },
                        {
                            "node": {
                                "headline": "Article_4",
                            },
                        },
                    ],
                },
            },
        ],
    }


@pytest.mark.asyncio
async def test_many_to_many(session):
    await session.execute(
        sa.insert(Reporter),
        [
            {Reporter.first_name.key: "Reporter_1"},
            {Reporter.first_name.key: "Reporter_2"},
        ],
    )
    reporters = dict(
        (await session.execute(sa.select(Reporter.first_name, Reporter.id))).fetchall()
    )

    await session.execute(
        sa.insert(Pet),
        [
            {
                Pet.name.key: "Pet_1",
                Pet.pet_kind.key: "cat",
                Pet.hair_kind.key: HairKind.LONG,
            },
            {
                Pet.name.key: "Pet_2",
                Pet.pet_kind.key: "cat",
                Pet.hair_kind.key: HairKind.LONG,
            },
            {
                Pet.name.key: "Pet_3",
                Pet.pet_kind.key: "cat",
                Pet.hair_kind.key: HairKind.LONG,
            },
            {
                Pet.name.key: "Pet_4",
                Pet.pet_kind.key: "cat",
                Pet.hair_kind.key: HairKind.LONG,
            },
        ],
    )
    pets = dict((await session.execute(sa.select(Pet.name, Pet.id))).fetchall())

    await session.execute(
        sa.insert(association_table),
        [
            {
                association_table.c.pet_id.key: pets["Pet_1"],
                association_table.c.reporter_id.key: reporters["Reporter_1"],
            },
            {
                association_table.c.pet_id.key: pets["Pet_2"],
                association_table.c.reporter_id.key: reporters["Reporter_1"],
            },
            {
                association_table.c.pet_id.key: pets["Pet_3"],
                association_table.c.reporter_id.key: reporters["Reporter_2"],
            },
            {
                association_table.c.pet_id.key: pets["Pet_4"],
                association_table.c.reporter_id.key: reporters["Reporter_2"],
            },
        ],
    )

    schema = get_schema()

    with mock_sqlalchemy_logging_handler() as sqlalchemy_logging_handler:
        # Starts new session to fully reset the engine / connection logging level
        result = await schema.execute_async(
            """
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
            """,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Article, Reporter, Pet]),
            ],
        )
        # messages = sqlalchemy_logging_handler.messages

    # assert len(messages) == 5

    # assert messages == [
    #     'BEGIN (implicit)',
    #
    #     'SELECT (SELECT CAST(count(reporters.id) AS INTEGER) AS anon_2 \nFROM reporters) AS anon_1, '
    #     'reporters.id AS reporters_id, '
    #     'reporters.first_name AS reporters_first_name, '
    #     'reporters.last_name AS reporters_last_name, '
    #     'reporters.email AS reporters_email, '
    #     'reporters.favorite_pet_kind AS reporters_favorite_pet_kind \n'
    #     'FROM reporters',
    #     '()',
    #
    #     'SELECT reporters_1.id AS reporters_1_id, '
    #     'pets.id AS pets_id, '
    #     'pets.name AS pets_name, '
    #     'pets.pet_kind AS pets_pet_kind, '
    #     'pets.hair_kind AS pets_hair_kind, '
    #     'pets.reporter_id AS pets_reporter_id \n'
    #     'FROM reporters AS reporters_1 '
    #     'JOIN association AS association_1 ON reporters_1.id = association_1.reporter_id '
    #     'JOIN pets ON pets.id = association_1.pet_id \n'
    #     'WHERE reporters_1.id IN (?, ?) '
    #     'ORDER BY pets.id',
    #     '(1, 2)'
    # ]

    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == {
        "reporters": [
            {
                "firstName": "Reporter_1",
                "pets": {
                    "edges": [
                        {
                            "node": {
                                "name": "Pet_1",
                            },
                        },
                        {
                            "node": {
                                "name": "Pet_2",
                            },
                        },
                    ],
                },
            },
            {
                "firstName": "Reporter_2",
                "pets": {
                    "edges": [
                        {
                            "node": {
                                "name": "Pet_3",
                            },
                        },
                        {
                            "node": {
                                "name": "Pet_4",
                            },
                        },
                    ],
                },
            },
        ],
    }
