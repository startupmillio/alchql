def get_batch_resolver(relationship_prop, single=False):
    def resolve(root, info, **args):
        loaders = info.context.loaders
        key = (relationship_prop.parent.entity, relationship_prop.mapper.entity)
        _loader = loaders[key]

        p = _loader.load(
            getattr(root, next(iter(relationship_prop.local_columns)).key)
        )

        if single:
            p = p.then(lambda x: x[0] if x else None)

        return p

    return resolve
