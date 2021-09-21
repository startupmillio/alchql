import logging

from graphene import Field, Dynamic

from graphene.types.objecttype import ObjectTypeMeta

from graphene_sqlalchemy.gql_fields import get_fields, camel_to_snake


def get_object_type(root, field_name):
    connection_field_type = type(root)._meta.fields[camel_to_snake(field_name)]

    if isinstance(connection_field_type, Field):
        return connection_field_type.type
    elif isinstance(connection_field_type, Dynamic) and isinstance(
        connection_field_type.type(), ObjectTypeMeta
    ):
        return connection_field_type.type().type


def get_batch_resolver(relationship_prop, single=False):
    async def resolve(root, info, **args):
        key = (relationship_prop.parent.entity, relationship_prop.mapper.entity, relationship_prop.key)
        _loader = info.context.loaders[key]

        _loader.info = info
        object_type = get_object_type(root, info.field_name)
        if object_type is not None:
            setattr(info.context, "object_type", object_type)

        key = getattr(root, next(iter(relationship_prop.local_columns)).key)
        if not key:
            p = None
        else:
            p = await _loader.load(key)

        if single:
            return p[0] if p else None

        return p

    return resolve
