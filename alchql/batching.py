import re

from graphene import Dynamic, Field
from graphene.types.objecttype import ObjectTypeMeta
from graphql import GraphQLResolveInfo
from sqlalchemy import ForeignKey
from sqlalchemy.orm import RelationshipProperty

from .gql_fields import camel_to_snake


def set_object_type(root, info: GraphQLResolveInfo):
    field_name = info.field_name
    root_type = type(root)
    if not hasattr(root_type, "_meta"):
        return
    types = getattr(info.context, "object_types", {})
    if types.get(info.field_name) is not None:
        return

    connection_field_type = root_type._meta.fields[camel_to_snake(field_name)]
    type_ = None
    if isinstance(connection_field_type, Field):
        type_ = connection_field_type.type
    elif isinstance(connection_field_type, Dynamic):
        if isinstance(connection_field_type.type(), ObjectTypeMeta):
            type_ = connection_field_type.type().type
        elif type(connection_field_type.type()) == Field:
            type_ = connection_field_type.type().type

    if not type_:
        return

    types[info.field_name] = type_
    setattr(info.context, "object_types", types)


def get_batch_resolver(relationship_prop: RelationshipProperty, single=False):
    async def resolve(root, info: GraphQLResolveInfo, **args):
        key = (
            relationship_prop.parent.entity,
            relationship_prop.mapper.entity,
            relationship_prop.key,
        )
        _loader = info.context.loaders[key]

        _loader.info = info
        set_object_type(root, info)

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
    async def resolve(root, info: GraphQLResolveInfo, **args):
        key = (
            fk.constraint.table,
            fk.constraint.referred_table,
            re.sub(r"_(?:id|pk)$", "", fk.parent.key),
        )
        _loader = info.context.loaders[key]

        _loader.info = info
        set_object_type(root, info)

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
    async def resolve(root, info: GraphQLResolveInfo, **args):
        key = (
            fk.constraint.referred_table,
            fk.constraint.table,
            str(fk.constraint.table.fullname),
        )
        _loader = info.context.loaders[key]

        _loader.info = info
        set_object_type(root, info)

        key = getattr(root, fk.column.key)
        if not key:
            p = None
        else:
            p = await _loader.load(key)

        if single:
            return p[0] if p else None

        return p

    return resolve
