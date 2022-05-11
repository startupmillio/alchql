import enum
from functools import partial
from inspect import isawaitable
from typing import Type

import graphene
import sqlalchemy as sa
from graphene import Argument, NonNull
from graphene.relay import Connection, ConnectionField
from graphene.relay.connection import connection_adapter, page_info_adapter
from graphene.types.utils import get_type
from graphql import GraphQLResolveInfo
from graphql_relay import connection_from_array_slice
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute, RelationshipProperty

from .batching import get_batch_resolver, get_fk_resolver_reverse
from .consts import OPERATORS_MAPPING, OP_EQ, OP_IN
from .query_helper import QueryHelper
from .registry import Registry, get_global_registry
from .slice import connection_from_query
from .sqlalchemy_converter import convert_sqlalchemy_type
from .utils import EnumValue, FilterItem, GlobalFilters, get_query


DEFAULT_LIMIT = 10000


class UnsortedSQLAlchemyConnectionField(ConnectionField):
    @property
    def type(self):
        from .types import SQLAlchemyObjectType

        type_ = super(ConnectionField, self).type
        nullable_type = get_nullable_type(type_)
        if issubclass(nullable_type, Connection):
            return type_
        assert issubclass(nullable_type, SQLAlchemyObjectType), (
            f"SQLALchemyConnectionField only accepts SQLAlchemyObjectType types, "
            f"not {nullable_type.__name__}"
        )
        assert (
            nullable_type.connection
        ), f"The type {nullable_type.__name__} doesn't have a connection"
        assert type_ == nullable_type, (
            "Passing a SQLAlchemyObjectType instance is deprecated. "
            "Pass the connection type instead accessible via SQLAlchemyObjectType.connection"
        )
        return nullable_type.connection

    @property
    def model(self):
        return get_nullable_type(self.type)._meta.node._meta.model

    @classmethod
    async def get_query(
        cls, model: Type[DeclarativeMeta], info: GraphQLResolveInfo, **args
    ):
        return get_query(model, info)

    @classmethod
    async def resolve_connection(
        cls,
        connection_type,
        model: Type[DeclarativeMeta],
        info: GraphQLResolveInfo,
        args,
        resolved,
    ):
        await cls._set_default_limit(args)
        query = await cls.get_query(model, info, **args)
        session = info.context.session
        if resolved is None:
            only_q = query.with_only_columns(
                *sa.inspect(model).primary_key,
            ).order_by(None)
            if not QueryHelper.get_filters(info) and QueryHelper.has_last_arg(info):
                raise TypeError('Cannot set "last" without filters applied')

            # get max count
            q = sa.select([sa.func.count()]).select_from(only_q.alias())
            q_res = await session.execute(q)
            _len = q_res.scalar()

            connection = await connection_from_query(
                query,
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
        cls,
        resolver,
        connection_type,
        model: Type[DeclarativeMeta],
        root,
        info: GraphQLResolveInfo,
        **args,
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

    @classmethod
    async def _set_default_limit(cls, args):
        if args.get("first") is None and args.get("last") is None:
            args["first"] = DEFAULT_LIMIT


class SQLAlchemyConnectionField(UnsortedSQLAlchemyConnectionField):
    def __init__(self, type_, *args, **kwargs):
        nullable_type = get_nullable_type(type_)
        if "sort" not in kwargs and issubclass(nullable_type, Connection):
            # Let super class raise if type is not a Connection
            try:
                kwargs.setdefault("sort", nullable_type.Edge.node._type.sort_argument())
            except (AttributeError, TypeError):
                raise TypeError(
                    f"Cannot create sort argument for {nullable_type.__name__}. "
                    'A model is required. Set the "sort" argument to None '
                    "to disabling the creation of the sort query argument"
                )
        elif "sort" in kwargs and kwargs["sort"] is None:
            del kwargs["sort"]

        super().__init__(type_, *args, **kwargs)

    @classmethod
    async def get_query(
        cls, model: Type[DeclarativeMeta], info: GraphQLResolveInfo, sort=None, **args
    ):
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
        type_ = get_type(type_)
        FilterConnectionField.set_filter_fields(type_, kwargs)

        if hasattr(type, "sort_argument"):
            kwargs["sort"] = type_.sort_argument()

        super().__init__(type_.connection, *args, **kwargs)

    @staticmethod
    def set_filter_fields(type_, kwargs):
        filters = {}
        tablename = type_._meta.model.__tablename__

        registry = get_global_registry()

        kwargs[GlobalFilters.ID__EQ] = Argument(type_=graphene.ID)
        filters[GlobalFilters.ID__EQ] = FilterItem(
            filter_func=getattr(
                sa.inspect(type_._meta.model).primary_key[0],
                OPERATORS_MAPPING[OP_EQ][0],
            ),
            field_type=graphene.ID,
        )
        kwargs[GlobalFilters.ID__IN] = Argument(
            type_=graphene.List(of_type=graphene.ID)
        )
        filters[GlobalFilters.ID__IN] = FilterItem(
            filter_func=getattr(
                sa.inspect(type_._meta.model).primary_key[0],
                OPERATORS_MAPPING[OP_IN][0],
            ),
            field_type=graphene.List(of_type=graphene.ID),
        )

        for field, operators in getattr(type_._meta, "filter_fields", {}).items():
            if isinstance(field, str) and isinstance(operators, FilterItem):
                kwargs[field] = Argument(
                    type_=operators.field_type,
                    default_value=operators.default_value,
                    description=operators.description,
                    name=operators.name,
                    required=operators.required,
                )
                filters[field] = operators

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

            if field.prop.columns[0].foreign_keys:
                field_type = graphene.ID

            for operator in operators:
                if operator == OP_IN:
                    operator_field_type = graphene.List(of_type=field_type)
                else:
                    operator_field_type = field_type

                filter_name = f"{field_key}__{operator}"
                kwargs[filter_name] = Argument(type_=operator_field_type)
                filters[filter_name] = FilterItem(
                    field_type=operator_field_type,
                    filter_func=getattr(field, OPERATORS_MAPPING[operator][0]),
                    value_func=OPERATORS_MAPPING[operator][1],
                )

        setattr(type_, "parsed_filters", filters)

    @classmethod
    async def get_query(
        cls, model: Type[DeclarativeMeta], info: GraphQLResolveInfo, sort=None, **args
    ):
        object_types = getattr(info.context, "object_types", {})
        object_type = object_types.get(info.field_name)

        filters = QueryHelper.get_filters(info)
        select_fields = QueryHelper.get_selected_fields(info, model, sort)
        gql_field = QueryHelper.get_current_field(info)
        query = sa.select(*select_fields)

        if object_type and hasattr(object_type, "set_select_from"):
            query = await object_type.set_select_from(info, query, gql_field.values)
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
                    sort_args.append(item.value.nullslast())
                else:
                    sort_args.append(item.nullslast())

            if hasattr(model, "id"):
                sort_args.append(model.id)

            query = query.order_by(*sort_args)
        return query


class ModelField(graphene.Field):
    def __init__(self, *args, model_field=None, use_label=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.model_field = model_field
        self.use_label = use_label


class RelationModelField(ModelField):
    @staticmethod
    def get_default_resolver(gql_type, model_field, key_field=None):
        def resolver(self, info, *args, **kwargs):
            target_value = getattr(self, model_field.key)
            if target_value is None:
                return
            if key_field is not None:
                assert key_field in gql_type._keys[0]
                target_key_field = key_field
            else:
                target_key_field = gql_type._keys[0]
            return gql_type(**{target_key_field: target_value})

        return resolver

    def __init__(self, type_, *args, model_field, key_field=None, **kwargs):
        if "resolver" not in kwargs:
            kwargs["resolver"] = self.get_default_resolver(
                type_, model_field, key_field
            )
        kwargs["use_label"] = False
        super().__init__(type_, *args, model_field=model_field, **kwargs)


class BatchSQLAlchemyConnectionField(FilterConnectionField, ModelField):
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

    @classmethod
    def from_fk(cls, fk: ForeignKey, registry, **field_kwargs):
        model_type = registry.get_type_for_model(fk.constraint.table)

        if not model_type:
            return

        resolver = get_fk_resolver_reverse(fk, single=False)

        if hasattr(model_type._meta, "filter_fields"):
            BatchSQLAlchemyConnectionField.set_filter_fields(model_type, field_kwargs)

        if hasattr(model_type, "sort_argument"):
            field_kwargs["sort"] = model_type.sort_argument()

        return cls(model_type, resolver=resolver, **field_kwargs)


def default_connection_field_factory(
    relationship: RelationshipProperty,
    registry: Registry,
    **field_kwargs,
):
    model = relationship.mapper.entity
    model_type = registry.get_type_for_model(model)
    return UnsortedSQLAlchemyConnectionField(model_type, **field_kwargs)


def get_nullable_type(_type):
    if isinstance(_type, NonNull):
        return _type.of_type
    return _type
