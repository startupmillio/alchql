from graphene_sqlalchemy.gql_fields import get_fields


def get_batch_resolver(relationship_prop, single=False):
    def resolve(root, info, **args):
        key = (relationship_prop.parent.entity, relationship_prop.mapper.entity)
        _loader = info.context.loaders[key]

        p = _loader.load(
            getattr(root, next(iter(relationship_prop.local_columns)).key)
        )

        try:
            fields = get_fields(relationship_prop.mapper.entity, info)
        except Exception as e:
            fields = relationship_prop.mapper.entity.__table__.columns

        _loader.fields.update(fields)

        if single:
            p = p.then(lambda x: x[0] if x else None)

        return p

    return resolve
