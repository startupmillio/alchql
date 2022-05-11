#!/usr/bin/env python

import uvicorn as uvicorn
from fastapi import FastAPI

from database import Base, db_session, engine, init_db
from alchql.app import SessionQLApp
from alchql.middlewares import LoaderMiddleware
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

app.add_route(
    "/graphql",
    SessionQLApp(
        schema=schema,
        middleware=[
            LoaderMiddleware(Base.registry.mappers),
        ],
        engine=engine,
    ),
)


@app.on_event("shutdown")
async def shutdown_session():
    db_session.remove()


@app.on_event("startup")
async def startup_session():
    await init_db()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info",
        reload=False,
    )
