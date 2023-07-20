import contextlib
import logging
from unittest.mock import patch

import graphene
import pytest
import sqlalchemy as sa
from graphene import Context
from sqlalchemy.ext.asyncio import AsyncSession

from alchql.middlewares import LoaderMiddleware
from alchql.node import AsyncNode
from alchql.types import SQLAlchemyObjectType
from .models import Article, association_table, HairKind, Pet, Reporter
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
            interfaces = (AsyncNode,)

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (AsyncNode,)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (AsyncNode,)

    class Query(graphene.ObjectType):
        articles = graphene.Field(graphene.List(ArticleType))
        reporters = graphene.Field(graphene.List(ReporterType))
        pets = graphene.Field(graphene.List(PetType))

        async def resolve_articles(self, info):
            session = info.context.session
            result = await session.execute(sa.select(Article))
            return result.scalars().all()

        async def resolve_reporters(self, info):
            session = info.context.session
            result = await session.execute(sa.select(Reporter))
            return result.scalars().all()

        async def resolve_pets(self, info):
            session = info.context.session
            result = await session.execute(sa.select(Pet))
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

    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
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

    assert execute.call_count == 3

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

    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
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

    assert not result.errors, result.errors
    assert execute.call_count == 2

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

    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
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

    assert not result.errors, result.errors
    assert execute.call_count == 2

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
async def test_one_to_many_sorted(session, raise_graphql):
    await session.execute(
        sa.insert(Reporter),
        [
            {Reporter.first_name.key: "Reporter_1"},
            {Reporter.first_name.key: "Reporter_2"},
            {Reporter.first_name.key: "Reporter_3"},
        ],
    )

    reporter_1_id = (
        await session.execute(
            sa.select(Reporter.id).where(Reporter.first_name == "Reporter_1")
        )
    ).scalar()
    reporter_2_id = (
        await session.execute(
            sa.select(Reporter.id).where(Reporter.first_name == "Reporter_2")
        )
    ).scalar()

    await session.execute(
        sa.insert(Article).values(
            [
                {Article.headline: "Article_1", Article.reporter_id: reporter_1_id},
                {Article.headline: "Article_2", Article.reporter_id: reporter_1_id},
                {Article.headline: "Article_3", Article.reporter_id: reporter_2_id},
                {Article.headline: "Article_4", Article.reporter_id: reporter_2_id},
            ]
        )
    )

    schema = get_schema()

    # Passing sort inside the query
    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
        result = await schema.execute_async(
            """
            query {
                reporters {
                    firstName
                    articles(first: 2, sort: HEADLINE_DESC) {
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

    assert not result.errors, result.errors
    assert execute.call_count == 2

    result = to_std_dicts(result.data)
    expected_result = {
        "reporters": [
            {
                "firstName": "Reporter_1",
                "articles": {
                    "edges": [
                        {"node": {"headline": "Article_2"}},
                        {"node": {"headline": "Article_1"}},
                    ],
                },
            },
            {
                "firstName": "Reporter_2",
                "articles": {
                    "edges": [
                        {"node": {"headline": "Article_4"}},
                        {"node": {"headline": "Article_3"}},
                    ],
                },
            },
            {
                "firstName": "Reporter_3",
                "articles": {
                    "edges": [],
                },
            },
        ],
    }
    assert result == expected_result

    # Passing sort in variables
    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
        result = await schema.execute_async(
            """
            query($sort: [ArticleTypeSortEnum]) {
                reporters {
                    firstName
                    articles(first: 2, sort: $sort) {
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
            variables={"sort": "HEADLINE_DESC"},
        )

    assert not result.errors, result.errors
    assert execute.call_count == 2

    result = to_std_dicts(result.data)
    assert expected_result == result


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

    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
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

    assert execute.call_count == 2

    assert not result.errors, result.errors[0]
    result = to_std_dicts(result.data)
    assert result == {
        "reporters": [
            {
                "firstName": "Reporter_1",
                "pets": {
                    "edges": [
                        {"node": {"name": "Pet_1"}},
                        {"node": {"name": "Pet_2"}},
                    ],
                },
            },
            {
                "firstName": "Reporter_2",
                "pets": {
                    "edges": [
                        {"node": {"name": "Pet_3"}},
                        {"node": {"name": "Pet_4"}},
                    ],
                },
            },
        ],
    }

    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
        result = await schema.execute_async(
            """
              query {
                pets {
                  name
                  reporters(first: 2) {
                    edges {
                      node {
                        firstName
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

    assert not result.errors, result.errors[0]
    assert execute.call_count == 2

    result = to_std_dicts(result.data)
    assert result == {
        "pets": [
            {
                "name": "Pet_1",
                "reporters": {"edges": [{"node": {"firstName": "Reporter_1"}}]},
            },
            {
                "name": "Pet_2",
                "reporters": {"edges": [{"node": {"firstName": "Reporter_1"}}]},
            },
            {
                "name": "Pet_3",
                "reporters": {"edges": [{"node": {"firstName": "Reporter_2"}}]},
            },
            {
                "name": "Pet_4",
                "reporters": {"edges": [{"node": {"firstName": "Reporter_2"}}]},
            },
        ]
    }


@pytest.mark.asyncio
async def test_many_to_many_sorted(session):
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

    pet_reporter = {
        "Pet_1": "Reporter_1",
        "Pet_2": "Reporter_1",
        "Pet_3": "Reporter_2",
        "Pet_4": "Reporter_2",
    }

    await session.execute(
        sa.insert(Pet),
        [
            {
                Pet.name.key: pet_name,
                Pet.pet_kind.key: "cat",
                Pet.hair_kind.key: HairKind.LONG,
            }
            for pet_name in pet_reporter
        ],
    )
    pets = dict((await session.execute(sa.select(Pet.name, Pet.id))).fetchall())

    await session.execute(
        sa.insert(association_table),
        [
            {
                association_table.c.pet_id.key: pets[pet_name],
                association_table.c.reporter_id.key: reporters[reporter_name],
            }
            for pet_name, reporter_name in pet_reporter.items()
        ],
    )

    expected_result = {
        "reporters": [
            {
                "firstName": "Reporter_1",
                "pets": {
                    "edges": [
                        {"node": {"name": "Pet_2"}},
                        {"node": {"name": "Pet_1"}},
                    ]
                },
            },
            {
                "firstName": "Reporter_2",
                "pets": {
                    "edges": [
                        {"node": {"name": "Pet_4"}},
                        {"node": {"name": "Pet_3"}},
                    ]
                },
            },
        ],
    }
    schema = get_schema()

    # Passing sort inside the query
    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
        result = await schema.execute_async(
            """
              query {
                reporters {
                  firstName
                  pets(first: 2, sort: NAME_DESC) {
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

    assert not result.errors, result.errors[0]
    assert execute.call_count == 2

    result = to_std_dicts(result.data)
    assert result == expected_result

    # Passing sort in variables
    with patch.object(AsyncSession, "execute", wraps=session.execute) as execute:
        result = await schema.execute_async(
            """
              query($sort: [PetTypeSortEnum]) {
                reporters {
                  firstName
                  pets(first: 2, sort: $sort) {
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
            variables={"sort": "NAME_DESC"},
        )

    assert not result.errors, result.errors[0]
    assert execute.call_count == 2

    result = to_std_dicts(result.data)
    assert result == expected_result


@pytest.mark.asyncio
async def test_only_ids(session, raise_graphql):
    await session.execute(
        sa.insert(Reporter),
        [
            {Reporter.first_name.key: "Reporter_1"},
            {Reporter.first_name.key: "Reporter_2"},
            {Reporter.first_name.key: "Reporter_3"},
        ],
    )

    reporter_1_id = (
        await session.execute(
            sa.select(Reporter.id).where(Reporter.first_name == "Reporter_1")
        )
    ).scalar()
    reporter_2_id = (
        await session.execute(
            sa.select(Reporter.id).where(Reporter.first_name == "Reporter_2")
        )
    ).scalar()

    await session.execute(
        sa.insert(Article).values(
            [
                {Article.headline: "Article_1", Article.reporter_id: reporter_1_id},
                {Article.headline: "Article_2", Article.reporter_id: reporter_1_id},
                {Article.headline: "Article_3", Article.reporter_id: reporter_2_id},
                {Article.headline: "Article_4", Article.reporter_id: reporter_2_id},
            ]
        )
    )

    schema = get_schema()

    old_exec = AsyncSession.execute

    async def execute_mock(self, command):
        return await old_exec(self, command)

    with patch.object(AsyncSession, "execute", execute_mock) as execute:
        result = await schema.execute_async(
            """
            query {
                reporters {
                    articles(first: 2, sort: HEADLINE_DESC) {
                        edges {
                            node {
                                id
                            }
                        }
                    }
                }
            }
            """,
            context_value=Context(session=session),
            middleware=[
                LoaderMiddleware([Reporter, Article]),
            ],
        )

    assert not result.errors

    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == {
        "reporters": [
            {
                "articles": {
                    "edges": [
                        {"node": {"id": "QXJ0aWNsZVR5cGU6Mg=="}},
                        {"node": {"id": "QXJ0aWNsZVR5cGU6MQ=="}},
                    ]
                }
            },
            {
                "articles": {
                    "edges": [
                        {"node": {"id": "QXJ0aWNsZVR5cGU6NA=="}},
                        {"node": {"id": "QXJ0aWNsZVR5cGU6Mw=="}},
                    ]
                }
            },
            {"articles": {"edges": []}},
        ]
    }
