#!/usr/bin/env python
from flask import Flask, request
from flask_graphql import GraphQLView
from graphene import Context

from database import Base, db_session, init_db
from graphene_sqlalchemy.loaders_middleware import LoaderMiddleware
from schema import schema

app = Flask(__name__)
app.debug = True

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


class GraphQLView(GraphQLView):
    def get_context(self):
        return Context(request=request, session=db_session)


app.add_url_rule(
    "/graphql",
    view_func=GraphQLView.as_view(
        "graphql",
        schema=schema,
        graphiql=True,
        middleware=[
            LoaderMiddleware([Base.registry.mappers])
        ],
    ),
)


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


if __name__ == "__main__":
    init_db()

    app.run(debug=False)
