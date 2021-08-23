from typing import List

import sqlalchemy as sa

from graphene_sqlalchemy import get_session
from graphene_sqlalchemy.loader_fk import generate_loader_by_foreign_key


class LoaderMiddleware:
    def __init__(self, models: List):
        self.loaders = {}
        for model in models:
            inspected_model = sa.inspect(model)
            for relationship in inspected_model.relationships.values():
                key = (relationship.parent.entity, relationship.mapper.entity)
                self.loaders[key] = generate_loader_by_foreign_key(relationship)

    def resolve(self, next_, root, info, **args):
        if root is None:
            session = get_session(info.context)

            info.context.loaders = {k: v(session) for k, v in self.loaders.items()}

        return next_(root, info, **args)
