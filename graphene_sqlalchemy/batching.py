import logging

from graphene import Field, Dynamic

from graphene.types.objecttype import ObjectTypeMeta

from graphene_sqlalchemy.gql_fields import get_fields, camel_to_snake


def get_batch_resolver(relationship_prop, single=False):
    async def resolve(root, info, **args):
        key = (relationship_prop.parent.entity, relationship_prop.mapper.entity)
        _loader = info.context.loaders[key]

        _loader.info = info
        connection_field_type = type(root)._meta.fields[camel_to_snake(info.field_name)]

        if isinstance(connection_field_type, Field):
            setattr(info.context, "object_type", connection_field_type.type)
        elif isinstance(connection_field_type, Dynamic):
            if isinstance(connection_field_type.type(), ObjectTypeMeta):
                setattr(info.context, "object_type", connection_field_type.type().type)

        key = getattr(root, next(iter(relationship_prop.local_columns)).key)
        if not key:
            p = None
        else:
            p = await _loader.load(key)

        try:
            fields = get_fields(relationship_prop.mapper.entity, info)
        except Exception as e:
            logging.error(e)
            fields = relationship_prop.mapper.entity.__table__.columns

        _loader.fields.update(fields)

        if single:
            return p[0] if p else None

        return p

    return resolve
