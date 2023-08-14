import enum
from collections import defaultdict

import sqlalchemy as sa
from aiodataloader import DataLoader
from graphene import ResolveInfo
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeMeta, RelationshipProperty
from sqlalchemy.sql import Select

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
            elif r == l2:
                base_table = r
                join_table = l
                join_table2 = r2
            elif r == r2:
                base_table = r
                join_table = l
                join_table2 = l2
            elif l == r2:
                base_table = l
                join_table = r
                join_table2 = l2

        join = sa.join(
            base_table,
            join_table,
            relation.primaryjoin,
        )

        if join_table2 is not None:
            join = join.join(
                join_table2,
                relation.secondaryjoin,
            )

        return join


class BaseLoader(DataLoader):
    target_field: sa.Column
    target: DeclarativeMeta

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

    @staticmethod
    def get_sort_args(gql_field, object_type):
        sort_args = []

        if gql_field.arguments.get("sort") is not None:
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
                sort_args.append(item)

        return sort_args

    def prepare_query(self, q: Select) -> Select:
        return q

    async def batch_load_fn(self, keys):
        object_types = getattr(self.info.context, "object_types", {})
        object_type = object_types.get(self.info.field_name)

        if object_type is None:
            parent_type = self.info.parent_type
            field = parent_type.fields[self.info.field_name]
            object_type = field.type.graphene_type

        filters = QueryHelper.get_filters(self.info)
        gql_field = QueryHelper.get_current_field(self.info)
        sort_args = self.get_sort_args(gql_field, object_type)

        selected_fields = QueryHelper.get_selected_fields(
            self.info, model=self.target, object_type=object_type, sort=sort_args
        )
        if not selected_fields:
            selected_fields = self.fields or self.target.__table__.columns

        q = (
            sa.select(
                *selected_fields,
                self.target_field.label("_batch_key"),
            )
            .where(self.target_field.in_(keys))
            .order_by(*sort_args)
        )
        q = self.prepare_query(q)

        if object_type and hasattr(object_type, "set_select_from"):
            setattr(self.info.context, "keys", keys)
            q = await object_type.set_select_from(self.info, q, gql_field.values)
            if list(q._group_by_clause):
                q = q.group_by(self.target_field)

        if filters:
            q = q.where(sa.and_(*filters))

        results_by_ids = defaultdict(list)

        conversion_type = object_type or self.target
        results = map(dict, await self.session.execute(q))

        for result in results:
            _batch_key = result.pop("_batch_key")
            _data = filter_requested_fields_for_object(result, conversion_type)
            results_by_ids[_batch_key].append(conversion_type(**_data))

        return [results_by_ids.get(key, []) for key in keys]


def generate_loader_by_relationship(relation: RelationshipProperty):
    _target_field = next(iter(relation.local_columns))
    _target = relation.mapper.entity

    class RelationLoader(BaseLoader):
        target = _target
        target_field = _target_field

        def prepare_query(self, q: Select) -> Select:
            join = get_join(relation)
            if join is not None:
                q = q.select_from(join)
            if relation.order_by:
                for n, ob in enumerate(relation.order_by):
                    q = q.order_by(ob)
                    """fix for bug: for SELECT DISTINCT, ORDER BY expressions must appear in select list"""
                    q = q.add_columns(ob.label(f"order_by_{n}"))
            q = q.distinct()

            return q

    return RelationLoader


def generate_loader_by_foreign_key(fk: ForeignKey, reverse=False):
    if not reverse:
        _target_field = fk.column
        _target = table_to_class(_target_field.table)
    else:
        _target_field = fk.parent
        _target = table_to_class(_target_field.table)

    class FkLoader(BaseLoader):
        target = _target
        target_field = _target_field

    return FkLoader
