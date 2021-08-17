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
            f = next(iter(relation.local_columns))
            target = relation.mapper.entity

            q = sa.select([target, f.label("_batch_key")])
            if relation.primaryjoin is not None:
                q = q.where(relation.primaryjoin)
            if relation.secondaryjoin is not None:
                q = q.where(relation.secondaryjoin)

            q = q.where(f.in_(keys))

            if relation.order_by:
                for ob in relation.order_by:
                    q = q.order_by(ob)

            results_by_ids = defaultdict(list)

            for result in self.session.execute(q):
                _result = dict(**result)
                _batch_key = _result.pop("_batch_key")
                results_by_ids[_batch_key].append(target(**_result))

            return Promise.resolve([results_by_ids.get(id, []) for id in keys])

    return Loader
