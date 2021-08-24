import enum
from functools import partial

import sqlalchemy as sa
from graphene import NonNull
from graphene.relay import Connection, ConnectionField
from graphene.relay.connection import page_info_adapter
from graphql_relay.connection.arrayconnection import connection_from_array_slice
from promise import Promise, is_thenable

from .batching import get_batch_resolver
from .sa_version import __sa_version__
from .slice import connection_from_query
from .utils import EnumValue, get_query, get_session


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
    def resolve_connection(cls, connection_type, model, info, args, resolved):
        query = cls.get_query(model, info, **args)
        session = get_session(info.context)

        if resolved is None:
            if __sa_version__ > (1, 4):
                q_aliased = query.with_only_columns(
                    *sa.inspect(model).primary_key
                ).alias()
                q = sa.select([sa.func.count()]).select_from(q_aliased)
            else:
                raise Exception("Use SQLAlchemy version > 1.4")

                # q = sa.select([sa.func.count()]).select_from(
                #     query.with_only_columns(sa.inspect(model).primary_key)
                # )

            _len = session.execute(q).scalar()

            connection = connection_from_query(
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
        else:
            connection = connection_from_array_slice(
                array_slice=resolved,
                args=args,
                connection_type=connection_type,
                edge_type=connection_type.Edge,
                page_info_type=page_info_adapter,
            )
        return connection

    @classmethod
    def connection_resolver(cls, resolver, connection_type, model, root, info, **args):
        resolved = resolver(root, info, **args)

        on_resolve = partial(cls.resolve_connection, connection_type, model, info, args)
        if is_thenable(resolved):
            return Promise.resolve(resolved).then(on_resolve)

        return on_resolve(resolved)

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


class BatchSQLAlchemyConnectionField(UnsortedSQLAlchemyConnectionField):
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
        return cls(model_type.connection, resolver=resolver, **field_kwargs)


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
