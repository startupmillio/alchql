import enum
from collections import defaultdict

import sqlalchemy as sa
from aiodataloader import DataLoader
from graphene import ResolveInfo
from sqlalchemy import ForeignKey, Table
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import RelationshipProperty

from .query_helper import QueryHelper
from .utils import EnumValue, filter_requested_fields_for_object, table_to_class


def get_join(relation: RelationshipProperty):
    if relation.primaryjoin is not None:
        l = relation.primaryjoin.left.table
        r = relation.primaryjoin.right.table

        base_table = l
        join_table = r
        join_table2 = None

        if relation.secondaryjoin is not None:
            l2 = relation.secondaryjoin.right.table
            r2 = relation.secondaryjoin.left.table
            if l == l2:
                base_table = l
                join_table = r
                join_table2 = r2
            elif r == r2:
                base_table = r
                join_table = l
                join_table2 = l2
            elif l == r2:
                base_table = l
                join_table = r
                join_table2 = l2
            elif r == l2:
                base_table = r
                join_table = l
                join_table2 = r2

        sf = sa.join(
            base_table,
            join_table,
            relation.primaryjoin,
        )

        if join_table2 is not None:
            sf = sf.join(
                join_table2,
                relation.secondaryjoin,
            )

        return sf


def generate_loader_by_relationship(relation: RelationshipProperty):
    class Loader(DataLoader):
        def __init__(
            self,
            session: AsyncSession,
            info: ResolveInfo = None,
            *args,
            **kwargs,
        ):
            self.session = session
            self.info = info
            self.fields = set()
            super().__init__(*args, **kwargs)

        async def batch_load_fn(self, keys):
            f = next(iter(relation.local_columns))
            target = relation.mapper.entity

            object_types = getattr(self.info.context, "object_types", {})
            setattr(self.info.context, "keys", keys)
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
                        if isinstance(s, (EnumValue, enum.Enum)):
                            new_sort.append(s.value)
                        else:
                            new_sort.append(getattr(sort_type, s).value)

                    sort = new_sort

                for item in sort:
                    sort_args.append(item.nullslast())

            selected_fields = QueryHelper.get_selected_fields(
                self.info, model=target, sort=sort
            )
            if not selected_fields:
                selected_fields = self.fields or target.__table__.columns

            q = sa.select(
                *selected_fields,
                f.label("_batch_key"),
            )

            join = get_join(relation)
            if join is not None:
                q = q.select_from(join)

            q = q.where(f.in_(keys))

            if object_type and hasattr(object_type, "set_select_from"):
                setattr(self.info.context, "keys", keys)
                q = await object_type.set_select_from(self.info, q, gql_field.values)
                if list(q._group_by_clause):
                    q = q.group_by(f)

            if filters:
                q = q.where(sa.and_(*filters))

            q = q.order_by(*sort_args)

            if relation.order_by:
                for ob in relation.order_by:
                    q = q.order_by(ob.nullslast())

            results_by_ids = defaultdict(list)

            conversion_type = object_type or target
            for result in map(dict, await self.session.execute(q.distinct())):
                _batch_key = result.pop("_batch_key")
                _data = filter_requested_fields_for_object(result, conversion_type)
                results_by_ids[_batch_key].append(conversion_type(**_data))

            return [results_by_ids.get(id, []) for id in keys]

    return Loader


def generate_loader_by_foreign_key(fk: ForeignKey, reverse=False):
    class Loader(DataLoader):
        def __init__(
            self,
            session: AsyncSession,
            info: ResolveInfo = None,
            *args,
            **kwargs,
        ):
            self.session = session
            self.info = info
            self.fields = set()
            super().__init__(*args, **kwargs)

        async def batch_load_fn(self, keys):
            if not reverse:
                target_field = fk.column
                target: Table = target_field.table
            else:
                target_field = fk.parent
                target: Table = target_field.table

            object_types = getattr(self.info.context, "object_types", {})
            setattr(self.info.context, "keys", keys)
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
                        if isinstance(s, (EnumValue, enum.Enum)):
                            new_sort.append(s.value)
                        else:
                            new_sort.append(getattr(sort_type, s).value)

                    sort = new_sort

                for item in sort:
                    sort_args.append(item.nullslast())

            selected_fields = QueryHelper.get_selected_fields(
                self.info, model=target, sort=sort
            )

            if not selected_fields:
                selected_fields = self.fields or target.columns

            q = sa.select(
                *selected_fields,
                target_field.label("_batch_key"),
            ).where(target_field.in_(keys))

            if object_type and hasattr(object_type, "set_select_from"):
                setattr(self.info.context, "keys", keys)
                q = await object_type.set_select_from(self.info, q, gql_field.values)

            if filters:
                q = q.where(sa.and_(*filters))

            q = q.order_by(*sort_args)

            results_by_ids = defaultdict(list)

            conversion_type = object_type or table_to_class(target)
            for result in map(dict, await self.session.execute(q.distinct())):
                _batch_key = result.pop("_batch_key")
                _data = filter_requested_fields_for_object(result, conversion_type)
                results_by_ids[_batch_key].append(conversion_type(**_data))

            return [results_by_ids.get(id, []) for id in keys]

    return Loader
