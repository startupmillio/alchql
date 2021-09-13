from inspect import isawaitable
from typing import List, Sequence, Type, Union

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeMeta, Mapper

from graphene_sqlalchemy import get_session
from graphene_sqlalchemy.loader_fk import generate_loader_by_foreign_key


class LoaderMiddleware:
    def __init__(self, models: Sequence[Union[Mapper, Type[DeclarativeMeta]]]):
        self.loaders = {}
        for model in models:
            if isinstance(model, Mapper):
                model = model.entity

            inspected_model = sa.inspect(model)
            for relationship in inspected_model.relationships.values():
                key = (relationship.parent.entity, relationship.mapper.entity)
                self.loaders[key] = generate_loader_by_foreign_key(relationship)

    async def resolve(self, next_, root, info, **args):
        if root is None:
            session = get_session(info.context)

            info.context.loaders = {k: v(session) for k, v in self.loaders.items()}

        result = next_(root, info, **args)
        if isawaitable(result):
            return await result
        return result
