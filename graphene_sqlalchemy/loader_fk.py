from collections import defaultdict

import sqlalchemy as sa
from promise import Promise
from promise.dataloader import DataLoader


def generate_loader_by_foreign_key(relation):
    class Loader(DataLoader):
        def __init__(self, session, *args, **kwargs):
            self.session = session
            super().__init__(*args, **kwargs)

        def batch_load_fn(self, keys) -> Promise:
            f = next(iter(relation.remote_side))
            q = sa.select([relation.mapper.entity]).where(f.in_(keys))

            results_by_ids = defaultdict(list)

            for result in self.session.execute(q):
                results_by_ids[result[f]].append(relation.mapper.entity(**result))

            return Promise.resolve([results_by_ids.get(id, []) for id in keys])

    return Loader
