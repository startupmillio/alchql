#!/usr/bin/env python
from functools import partial

import uvicorn as uvicorn
from fastapi import FastAPI
from graphene import Context
from starlette_graphene3 import GraphQLApp, make_graphiql_handler

from database import Base, db_session, init_db
from examples.fastapi_sqlalchemy.session_ql import SessionQLApp
from graphene_sqlalchemy.loaders_middleware import LoaderMiddleware
from schema import schema

app = FastAPI()

example_query = """
{
  allEmployees(sort: [NAME_ASC, ID_ASC]) {
    edges {
      node {
        id
        name
        department {
          id
          name
        }
        role {
          id
          name
        }
      }
    }
  }
}
"""


class GContext(Context):
    def __init__(self, request, **kwargs):
        super().__init__(request=request, **kwargs)

    def get(self, name, default=None):
        return getattr(self, name, default)


app.add_route(
    "/graphql",
    SessionQLApp(
        schema=schema,
        on_get=make_graphiql_handler(),
        middleware=[LoaderMiddleware(Base.registry.mappers)],
        # context_value=partial(GContext, session=db_session),
        context_value=GContext,
        # db_url="sqlite:///database.sqlite3",
        db_url="postgresql+asyncpg://godunov:godunov@127.0.0.1:5432/test_godunov",
    ),
)


@app.on_event("shutdown")
async def shutdown_session():
    db_session.remove()


if __name__ == "__main__":
    init_db()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info",
        reload=False,
        log_config=None,
    )
