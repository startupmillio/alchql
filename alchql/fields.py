import enum
from functools import partial
from inspect import isawaitable
from typing import Type

import graphene
import sqlalchemy as sa
from graphene.relay import Connection, ConnectionField
from graphene.types import ResolveInfo
from graphene.types.utils import get_type
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute, RelationshipProperty
from sqlalchemy.sql.elements import Label

from .batching import get_batch_resolver, get_fk_resolver_reverse
from .connection.from_array_slice import connection_from_array_slice
from .connection.from_query import connection_from_query
from .consts import OPERATORS_MAPPING, OP_EQ, OP_IN
from .query_helper import QueryHelper
from .registry import Registry, get_global_registry
from .sqlalchemy_converter import convert_sqlalchemy_type
from .utils import EnumValue, FilterItem, get_curr_object_type, GlobalFilters, get_query


class UnsortedSQLAlchemyConnectionField(ConnectionField):
    @property
    def type(self):
        from .types import SQLAlchemyObjectType

        type_ = super(ConnectionField, self).type
        nullable_type = get_nullable_type(type_)
        if issubclass(nullable_type, Connection):
            return type_

        if not issubclass(nullable_type, SQLAlchemyObjectType):
            raise AssertionError(
                f"SQLALchemyConnectionField only accepts SQLAlchemyObjectType types, "
                f"not {nullable_type.__name__}"
            )
        if not nullable_type.connection:
            raise AssertionError(
                f"The type {nullable_type.__name__} doesn't have a connection"
            )
        if type_ != nullable_type:
            raise AssertionError(
                "Passing a SQLAlchemyObjectType instance is deprecated. "
                "Pass the connection type instead accessible via SQLAlchemyObjectType.connection"
            )
        return nullable_type.connection

    @property
    def model(self):
        return get_nullable_type(self.type)._meta.node._meta.model

    @classmethod
    async def get_query(
        cls, model: Type[DeclarativeMeta], info: ResolveInfo, cls_name=None, **args
    ):
        return get_query(model, info, cls_name)

    @classmethod
    async def resolve_connection(
        cls,
        connection_type,
        model: Type[DeclarativeMeta],
        info: ResolveInfo,
        args,
        resolved,
    ):
        if resolved is None:
            edge_type = connection_type.Edge
            node_type = edge_type.node.type

            query = await cls.get_query(
                model=model,
                info=info,
                cls_name=node_type.__name__,
                **args,
            )

            connection = await connection_from_query(
                query,
                info=info,
                model=model,
                args=args,
                connection_type=connection_type,
            )
        else:
            if isawaitable(resolved):
                resolved = await resolved

            connection = connection_from_array_slice(
                array_slice=resolved,
                args=args,
                connection_type=connection_type,
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
        info: ResolveInfo,
        **args,
    ):
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
        cls,
        model: Type[DeclarativeMeta],
        info: ResolveInfo,
        sort=None,
        cls_name=None,
        **args,
    ):
        query = get_query(model, info, cls_name)
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

    @classmethod
    def set_filter_fields(cls, type_, kwargs):
        filters = {}
        kwargs[GlobalFilters.ID__EQ] = graphene.Argument(type_=graphene.ID)
        filters[GlobalFilters.ID__EQ] = FilterItem(
            filter_func=getattr(
                sa.inspect(type_._meta.model).primary_key[0],
                OPERATORS_MAPPING[OP_EQ][0],
            ),
            field_type=graphene.ID,
        )
        kwargs[GlobalFilters.ID__IN] = graphene.Argument(
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
                kwargs[field] = graphene.Argument(
                    type_=operators.field_type,
                    default_value=operators.default_value,
                    description=operators.description,
                    name=operators.name,
                    required=operators.required,
                )
                filters[field] = operators
                continue

            if isinstance(field, InstrumentedAttribute):
                field_key, field_type = cls.process_instrumented_field(field, type_)
            elif isinstance(field, Label):
                field_key, field_type = cls.process_label_field(field)
            elif isinstance(field, sa.Column):
                field_key, field_type = cls.process_column_field(field, type_)
            else:
                continue

            for operator in operators:
                if operator == OP_IN:
                    operator_field_type = graphene.List(of_type=field_type)
                else:
                    operator_field_type = field_type

                filter_name = f"{field_key}__{operator}"
                kwargs[filter_name] = graphene.Argument(type_=operator_field_type)
                filters[filter_name] = FilterItem(
                    field_type=operator_field_type,
                    filter_func=getattr(field, OPERATORS_MAPPING[operator][0]),
                    value_func=OPERATORS_MAPPING[operator][1],
                )

        setattr(type_, "parsed_filters", filters)

    @staticmethod
    def process_instrumented_field(field, type_):
        registry = get_global_registry()
        tablename = type_._meta.model.__tablename__

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

        return field_key, field_type

    @staticmethod
    def process_label_field(field):
        registry = get_global_registry()
        field_key = field.key

        field_type = convert_sqlalchemy_type(
            getattr(field, "type", None), field, registry
        )

        if field.foreign_keys:
            field_type = graphene.ID

        return field_key, field_type

    @staticmethod
    def process_column_field(field, type_):
        registry = get_global_registry()
        tablename = type_._meta.model.__tablename__

        if field.table.name == tablename:
            gql_field = type_._meta.fields.get(field.key)
            if gql_field and gql_field.name is not None:
                field_key = gql_field.name
            else:
                field_key = field.key
        else:
            field_key = f"{field.table.name}_{field.key}"

        field_type = convert_sqlalchemy_type(
            getattr(field, "type", None), field, registry
        )

        if field.foreign_keys:
            field_type = graphene.ID

        return field_key, field_type

    @classmethod
    async def get_query(
        cls, model: Type[DeclarativeMeta], info: ResolveInfo, sort=None, **args
    ):
        object_type = get_curr_object_type(info)

        filters = QueryHelper.get_filters(info)

        select_fields = QueryHelper.get_selected_fields(info, model, object_type, sort)
        query = sa.select(*select_fields).select_from(model)

        if object_type and hasattr(object_type, "set_select_from"):
            gql_field = QueryHelper.get_current_field(info)
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
                    sort_args.append(item.value)
                else:
                    sort_args.append(item)

            sort_args.extend(sa.inspect(model).primary_key)

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
    def from_fk(cls, fk: ForeignKey, registry: Registry, **field_kwargs):
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
    if isinstance(_type, graphene.NonNull):
        return _type.of_type
    return _type
