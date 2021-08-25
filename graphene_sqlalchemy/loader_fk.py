from collections import defaultdict

import sqlalchemy as sa
from aiodataloader import DataLoader

from .sa_version import __sa_version__


def generate_loader_by_foreign_key(relation):
    class Loader(DataLoader):
        def __init__(self, session, *args, **kwargs):
            self.session = session
            self.fields = set()
            super().__init__(*args, **kwargs)

        async def batch_load_fn(self, keys):
            f = next(iter(relation.local_columns))
            target = relation.mapper.entity

            if __sa_version__ > (1, 4):
                q = sa.select(
                    *(self.fields or target.__table__.columns), f.label("_batch_key")
                )
            else:
                raise Exception(f"Invalid sqlalchemy version: {__sa_version__}")
                # q = sa.select(
                #     list(self.fields or target.__table__.columns)
                #     + [f.label("_batch_key")]
                # )

            if relation.primaryjoin is not None:
                q = q.where(relation.primaryjoin)
            if relation.secondaryjoin is not None:
                q = q.where(relation.secondaryjoin)

            q = q.where(f.in_(keys))

            if relation.order_by:
                for ob in relation.order_by:
                    q = q.order_by(ob)

            results_by_ids = defaultdict(list)

            for result in self.session.execute(q.distinct()):
                _data = dict(**result)
                _batch_key = _data.pop("_batch_key")
                results_by_ids[_batch_key].append(target(**_data))

            return [results_by_ids.get(id, []) for id in keys]

    return Loader
