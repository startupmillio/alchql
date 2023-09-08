import re

from graphene import ResolveInfo
from sqlalchemy import ForeignKey
from sqlalchemy.orm import RelationshipProperty


def get_batch_resolver(relationship_prop: RelationshipProperty, single=False):
    async def resolve(root, info: ResolveInfo, **args):
        key = (
            relationship_prop.parent.entity,
            relationship_prop.mapper.entity,
            relationship_prop.key,
        )
        _loader = info.context.loaders[key]

        _loader.info = info

        key = getattr(root, next(iter(relationship_prop.local_columns)).key)
        if not key:
            p = None
        else:
            p = await _loader.load(key)

        if single:
            return p[0] if p else None

        return p

    return resolve


def get_fk_resolver(fk: ForeignKey, single=False):
    async def resolve(root, info: ResolveInfo, **args):
        key = (
            fk.constraint.table,
            fk.constraint.referred_table,
            re.sub(r"_(?:id|pk)$", "", fk.parent.key),
        )
        _loader = info.context.loaders[key]

        _loader.info = info

        key = getattr(root, fk.parent.key)
        if not key:
            p = None
        else:
            p = await _loader.load(key)

        if single:
            return p[0] if p else None

        return p

    return resolve


def get_fk_resolver_reverse(fk: ForeignKey, single=False):
    async def resolve(root, info: ResolveInfo, **args):
        key = (
            fk.constraint.referred_table,
            fk.constraint.table,
            str(fk.constraint.table.fullname),
        )
        _loader = info.context.loaders[key]

        _loader.info = info

        key = getattr(root, fk.column.key)
        if not key:
            p = None
        else:
            p = await _loader.load(key)

        if single:
            return p[0] if p else None

        return p

    return resolve
