import re
from inspect import isawaitable
from typing import Sequence, Type, Union

import sqlalchemy as sa
from graphql import GraphQLResolveInfo
from sqlalchemy.orm import DeclarativeMeta, Mapper

from alchql.loader_fk import (
    generate_loader_by_foreign_key,
    generate_loader_by_relationship,
)


class LoaderMiddleware:
    def __init__(self, models: Sequence[Union[Mapper, Type[DeclarativeMeta]]]):
        self.loaders = {}
        for model in models:
            if isinstance(model, Mapper):
                model = model.entity

            inspected_model = sa.inspect(model)
            for fk in inspected_model.mapped_table.foreign_keys:
                key = (
                    fk.constraint.table,
                    fk.constraint.referred_table,
                    re.sub(r"_(?:id|pk)$", "", fk.parent.key),
                )
                self.loaders[key] = generate_loader_by_foreign_key(fk)

                key = (
                    fk.constraint.referred_table,
                    fk.constraint.table,
                    str(fk.constraint.table.fullname),
                )
                self.loaders[key] = generate_loader_by_foreign_key(fk, reverse=True)

            for relationship in inspected_model.relationships.values():
                key = (
                    relationship.parent.entity,
                    relationship.mapper.entity,
                    relationship.key,
                )
                self.loaders[key] = generate_loader_by_relationship(relationship)

    async def resolve(self, next_, root, info: GraphQLResolveInfo, **args):
        if root is None:
            session = info.context.session

            info.context.loaders = {k: v(session) for k, v in self.loaders.items()}

        result = next_(root, info, **args)
        if isawaitable(result):
            return await result
        return result
