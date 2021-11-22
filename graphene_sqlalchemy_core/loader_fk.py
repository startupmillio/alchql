import enum
from collections import defaultdict

import sqlalchemy as sa
from aiodataloader import DataLoader

from .query_helper import QueryHelper
from .utils import EnumValue, filter_requested_fields_for_object


def generate_loader_by_foreign_key(relation):
    class Loader(DataLoader):
        def __init__(self, session, info=None, *args, **kwargs):
            self.session = session
            self.info = info
            self.fields = set()
            super().__init__(*args, **kwargs)

        async def batch_load_fn(self, keys):
            f = next(iter(relation.local_columns))
            target = relation.mapper.entity

            object_types = getattr(self.info.context, "object_types", {})
            object_type = object_types.get(self.info.field_name)

            filters = QueryHelper.get_filters(self.info)
            gql_field = QueryHelper.get_current_field(self.info)
            sort_args = []
            sort = []
            if (
                "sort" in gql_field.arguments
                and gql_field.arguments["sort"] is not None
            ):
                sort = gql_field.arguments["sort"]
                if not isinstance(sort, list):
                    sort = [sort]

                if hasattr(object_type, "sort_argument"):
                    sort_type = object_type.sort_argument().type.of_type
                    new_sort = []
                    for s in sort:
                        new_sort.append(getattr(sort_type, s).value)

                    sort = new_sort

                sort_args = []
                # ensure consistent handling of graphene Enums, enum values and
                # plain strings
                for item in sort:
                    if isinstance(item, (EnumValue, enum.Enum)):
                        sort_args.append(item.value)
                    else:
                        sort_args.append(item)

            selected_fields = QueryHelper.get_selected_fields(
                self.info, model=target, sort=sort
            )
            if not selected_fields:
                selected_fields = self.fields or target.__table__.columns

            q = sa.select(
                *selected_fields,
                f.label("_batch_key"),
            )

            if object_type and hasattr(object_type, "set_select_from"):
                q = await object_type.set_select_from(self.info, q, gql_field.values)
                if list(q._group_by_clause):
                    q = q.group_by(f)

            if filters:
                q = q.where(sa.and_(*filters))

            if relation.primaryjoin is not None:
                q = q.where(relation.primaryjoin)
            if relation.secondaryjoin is not None:
                q = q.where(relation.secondaryjoin)

            q = q.where(f.in_(keys))

            if relation.order_by:
                for ob in relation.order_by:
                    q = q.order_by(ob)

            q = q.order_by(*sort_args)

            results_by_ids = defaultdict(list)

            conversion_type = object_type or target
            for result in await self.session.execute(q.distinct()):
                _data = dict(**result)
                _batch_key = _data.pop("_batch_key")
                _data = filter_requested_fields_for_object(_data, conversion_type)
                results_by_ids[_batch_key].append(conversion_type(**_data))

            return [results_by_ids.get(id, []) for id in keys]

    return Loader
