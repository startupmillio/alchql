def get_batch_resolver(relationship_prop):
    def resolve(root, info, **args):
        loaders = info.context.loaders
        key = (relationship_prop.parent.entity, relationship_prop.mapper.entity)
        _loader = loaders[key]

        return _loader.load(
            getattr(root, next(iter(relationship_prop.local_columns)).key)
        )

    return resolve
