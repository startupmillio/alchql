import enum
from functools import partial
from inspect import isawaitable

import graphene
import sqlalchemy as sa
from graphene import Argument, NonNull
from graphene.relay import Connection, ConnectionField
from graphene.relay.connection import connection_adapter, page_info_adapter
from graphql_relay.connection.arrayconnection import connection_from_array_slice
from sqlalchemy.orm import InstrumentedAttribute

from .consts import OPERATORS_MAPPING, OP_IN
from .query_helper import QueryHelper
from .registry import get_global_registry
from .sqlalchemy_converter import convert_sqlalchemy_type
from .batching import get_batch_resolver
from .slice import connection_from_query
from .utils import EnumValue, get_query, get_session, FilterItem


class UnsortedSQLAlchemyConnectionField(ConnectionField):
    @property
    def type(self):
        from .types import SQLAlchemyObjectType

        type_ = super(ConnectionField, self).type
        nullable_type = get_nullable_type(type_)
        if issubclass(nullable_type, Connection):
            return type_
        assert issubclass(nullable_type, SQLAlchemyObjectType), (
            "SQLALchemyConnectionField only accepts SQLAlchemyObjectType types, not {}"
        ).format(nullable_type.__name__)
        assert nullable_type.connection, "The type {} doesn't have a connection".format(
            nullable_type.__name__
        )
        assert type_ == nullable_type, (
            "Passing a SQLAlchemyObjectType instance is deprecated. "
            "Pass the connection type instead accessible via SQLAlchemyObjectType.connection"
        )
        return nullable_type.connection

    @property
    def model(self):
        return get_nullable_type(self.type)._meta.node._meta.model

    @classmethod
    def get_query(cls, model, info, **args):
        return get_query(model, info)

    @classmethod
    async def resolve_connection(cls, connection_type, model, info, args, resolved):

        query = cls.get_query(model, info, **args)
        session = get_session(info.context)

        if resolved is None:
            q_aliased = query.with_only_columns(*sa.inspect(model).primary_key).alias()
            q = sa.select([sa.func.count()]).select_from(q_aliased)
            if QueryHelper.get_filters(info):
                _len = await session.execute(q).scalar()
            elif QueryHelper.has_last_arg(info):
                raise TypeError('Cannot set "last" without filters applied')
            else:
                _len = 100

            connection = await connection_from_query(
                query,
                model,
                session,
                args,
                slice_start=0,
                list_length=_len,
                list_slice_length=_len,
                connection_type=connection_type,
                page_info_type=page_info_adapter,
                edge_type=connection_type.Edge,
            )

            if hasattr(connection, "total_count"):
                connection.total_count = _len
        else:
            if isawaitable(resolved):
                resolved = await resolved
            connection = connection_from_array_slice(
                array_slice=resolved,
                args=args,
                connection_type=partial(connection_adapter, cls=connection_type),
                edge_type=connection_type.Edge,
                page_info_type=page_info_adapter,
            )
            if hasattr(connection, "total_count"):
                connection.total_count = len(resolved)
        return connection

    @classmethod
    async def connection_resolver(
        cls, resolver, connection_type, model, root, info, **args
    ):
        types = getattr(info.context, "object_types", {})
        types[info.field_name] = connection_type.Edge.node.type
        setattr(info.context, "object_types", types)
        resolved = resolver(root, info, **args)

        on_resolve = partial(cls.resolve_connection, connection_type, model, info, args)
        result = on_resolve(resolved)

        if isawaitable(result):
            return await result
        return result

    def wrap_resolve(self, parent_resolver):
        return partial(
            self.connection_resolver,
            parent_resolver,
            get_nullable_type(self.type),
            self.model,
        )


# TODO Rename this to SortableSQLAlchemyConnectionField
class SQLAlchemyConnectionField(UnsortedSQLAlchemyConnectionField):
    def __init__(self, type_, *args, **kwargs):
        nullable_type = get_nullable_type(type_)
        if "sort" not in kwargs and issubclass(nullable_type, Connection):
            # Let super class raise if type is not a Connection
            try:
                kwargs.setdefault("sort", nullable_type.Edge.node._type.sort_argument())
            except (AttributeError, TypeError):
                raise TypeError(
                    'Cannot create sort argument for {}. A model is required. Set the "sort" argument'
                    " to None to disabling the creation of the sort query argument".format(
                        nullable_type.__name__
                    )
                )
        elif "sort" in kwargs and kwargs["sort"] is None:
            del kwargs["sort"]
        super(SQLAlchemyConnectionField, self).__init__(type_, *args, **kwargs)

    @classmethod
    def get_query(cls, model, info, sort=None, **args):
        query = get_query(model, info)
        if sort is not None:
            if not isinstance(sort, list):
                sort = [sort]
            sort_args = []
            # ensure consistent handling of graphene Enums, enum values and
            # plain strings
            for item in sort:
                if isinstance(item, enum.Enum):
                    sort_args.append(item.value)
                elif isinstance(item, EnumValue):
                    sort_args.append(item.value)
                else:
                    sort_args.append(item)
            query = query.order_by(*sort_args)
        return query


class FilterConnectionField(SQLAlchemyConnectionField):
    def __init__(self, type_, *args, **kwargs):
        if hasattr(type_._meta, "filter_fields"):
            FilterConnectionField.set_filter_fields(type_, kwargs)

        if hasattr(type, "sort_argument"):
            kwargs["sort"] = type_.sort_argument()

        super(SQLAlchemyConnectionField, self).__init__(
            type_.connection, *args, **kwargs
        )

    @staticmethod
    def set_filter_fields(type_, kwargs):
        filters = {}
        tablename = type_._meta.model.__tablename__

        registry = get_global_registry()

        for field, operators in type_._meta.filter_fields.items():
            if not isinstance(field, InstrumentedAttribute):
                continue

            if field.parent.tables[0].name == tablename:
                gql_field = type_._meta.fields.get(field.key)
                if gql_field and gql_field.name is not None:
                    field_key = gql_field.name
                else:
                    field_key = field.key
            else:
                field_key = f"{field.parent.tables[0].name}_{field.key}"

            field_type = convert_sqlalchemy_type(
                getattr(field, "type", None), field, registry
            )

            for operator in operators:
                if operator == OP_IN:
                    field_type = graphene.List(of_type=field_type)
                filter_name = f"{field_key}__{operator}"
                kwargs[filter_name] = Argument(type_=field_type)
                filters[filter_name] = FilterItem(
                    field_type=field_type,
                    filter_func=getattr(field, OPERATORS_MAPPING[operator][0]),
                )

        setattr(type_, "parsed_filters", filters)

    @classmethod
    def get_query(cls, model, info, sort=None, **args):
        object_types = getattr(info.context, "object_types", {})
        object_type = object_types.get(info.field_name)

        filters = QueryHelper.get_filters(info)
        select_fields = QueryHelper.get_selected_fields(info, model)
        gql_field = QueryHelper.get_current_field(info)
        query = sa.select(*select_fields)

        if object_type and hasattr(object_type, "set_select_from"):
            query = object_type.set_select_from(info, query, gql_field.values)
        if filters:
            query = query.where(sa.and_(*filters))
        if sort is not None:
            if not isinstance(sort, list):
                sort = [sort]
            sort_args = []
            # ensure consistent handling of graphene Enums, enum values and
            # plain strings
            for item in sort:
                if isinstance(item, (EnumValue, enum.Enum)):
                    sort_args.append(item.value)
                else:
                    sort_args.append(item)

            if hasattr(model, "id"):
                sort_args.append(model.id)

            query = query.order_by(*sort_args)
        return query


class ModelField(graphene.Field):
    def __init__(self, *args, model_field=None, use_label=True, **kwargs):
        super(ModelField, self).__init__(*args, **kwargs)
        self.model_field = model_field
        self.use_label = use_label


class BatchSQLAlchemyConnectionField(FilterConnectionField):
    """
    This is currently experimental.
    The API and behavior may change in future versions.
    Use at your own risk.
    """

    def wrap_resolve(self, parent_resolver):
        return partial(
            self.connection_resolver,
            self.resolver,
            get_nullable_type(self.type),
            self.model,
        )

    @classmethod
    def from_relationship(cls, relationship, registry, **field_kwargs):
        model = relationship.mapper.entity
        model_type = registry.get_type_for_model(model)
        resolver = get_batch_resolver(relationship)

        if hasattr(model_type._meta, "filter_fields"):
            BatchSQLAlchemyConnectionField.set_filter_fields(model_type, field_kwargs)

        if hasattr(model_type, "sort_argument"):
            field_kwargs["sort"] = model_type.sort_argument()

        return cls(model_type, resolver=resolver, **field_kwargs)


def default_connection_field_factory(relationship, registry, **field_kwargs):
    model = relationship.mapper.entity
    model_type = registry.get_type_for_model(model)
    return __connectionFactory(model_type, **field_kwargs)


# TODO Remove in next major version
__connectionFactory = UnsortedSQLAlchemyConnectionField


def get_nullable_type(_type):
    if isinstance(_type, NonNull):
        return _type.of_type
    return _type
