import logging

from graphene_sqlalchemy.gql_fields import get_fields


def get_batch_resolver(relationship_prop, single=False):
    async def resolve(root, info, **args):
        key = (relationship_prop.parent.entity, relationship_prop.mapper.entity)
        _loader = info.context.loaders[key]

        p = await _loader.load(
            getattr(root, next(iter(relationship_prop.local_columns)).key)
        )

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
