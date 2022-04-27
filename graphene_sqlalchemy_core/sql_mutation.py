from enum import Enum
from inspect import isawaitable
from typing import Callable, Dict, Iterable, Type

import graphene
import sqlalchemy
import sqlalchemy as sa
from graphene import Argument, Field, Interface, ObjectType
from graphene.types.mutation import MutationOptions
from graphene.types.objecttype import ObjectTypeOptions
from graphene.types.utils import yank_fields_from_attrs
from graphene.utils.get_unbound_function import get_unbound_function
from graphene.utils.props import props
from graphql import GraphQLResolveInfo
from sqlalchemy.orm import DeclarativeMeta

from .get_input_type import get_input_fields, get_input_type
from .gql_fields import get_fields
from .gql_id import ResolvedGlobalId
from .types import SQLAlchemyObjectType
from .utils import get_query


class SQLMutationOptions(ObjectTypeOptions):
    model: DeclarativeMeta = None
    arguments: Dict[str, Argument] = None
    output: Type[SQLAlchemyObjectType] = None
    resolver: Callable = None
    interfaces: Iterable[Type[Interface]] = ()


class _BaseMutation(ObjectType):
    _meta: SQLMutationOptions

    @classmethod
    def Field(
        cls, name=None, description=None, deprecation_reason=None, required=False
    ):
        """Mount instance of mutation Field."""
        return graphene.Field(
            cls._meta.output,
            args=cls._meta.arguments,
            resolver=cls._meta.resolver,
            name=name,
            description=description or cls._meta.description,
            deprecation_reason=deprecation_reason,
            required=required,
        )

    @classmethod
    async def get_query(cls, info: GraphQLResolveInfo):
        return get_query(cls._meta.model, info, cls.__name__)

    @classmethod
    async def get_node(cls, info: GraphQLResolveInfo, id: int):
        session = info.context.session

        pk = sqlalchemy.inspect(cls._meta.model).primary_key[0]
        q = (await cls.get_query(info)).where(pk == id)
        result = cls(**(await session.execute(q)).first())
        return result


class SQLAlchemyUpdateMutation(_BaseMutation):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        model: Type[DeclarativeMeta],
        interfaces=(),
        resolver=None,
        output=None,
        arguments=None,
        only_fields=(),
        exclude_fields=(),
        required_fields=(),
        input_fields: dict = None,
        _meta=None,
        **options,
    ):
        if not _meta:
            _meta = MutationOptions(cls)

        output = output or getattr(cls, "Output", None)
        fields = {}

        for interface in interfaces:
            assert issubclass(
                interface, Interface
            ), f'All interfaces of {cls.__name__} must be a subclass of Interface. Received "{interface}".'
            fields.update(interface._meta.fields)

        if not output:
            # If output is defined, we don't need to get the fields
            fields = {}
            for base in reversed(cls.__mro__):
                fields.update(yank_fields_from_attrs(base.__dict__, _as=Field))
            output = cls

        if not arguments:
            input_class = getattr(cls, "Arguments", None)
            if input_class:
                arguments = props(input_class)
            else:
                if not input_fields:
                    input_fields = get_input_fields(
                        model,
                        only_fields=only_fields,
                        exclude_fields=exclude_fields,
                        required_fields=required_fields,
                    )
                input_type = get_input_type(
                    model, input_fields=input_fields, operation="Update"
                )
                arguments = {
                    "id": graphene.ID(required=True),
                    "value": graphene.Argument(input_type, required=True),
                }

        if not resolver:
            mutate = getattr(cls, "mutate", None)
            assert mutate, "All mutations must define a mutate method in it"
            resolver = get_unbound_function(mutate)

        if _meta.fields:
            _meta.fields.update(fields)
        else:
            _meta.fields = fields
        _meta.interfaces = interfaces
        _meta.output = output
        _meta.resolver = resolver
        _meta.arguments = arguments
        _meta.model = model

        super().__init_subclass_with_meta__(_meta=_meta, **options)

    @classmethod
    async def mutate(cls, root, info: GraphQLResolveInfo, id: str, value: dict):
        session = info.context.session
        model = cls._meta.model
        output = cls._meta.output

        table = sa.inspect(model).mapped_table
        pk = table.primary_key.columns[0]

        type_name, id_ = ResolvedGlobalId.decode(id)

        try:
            field_set = get_fields(model, info, type_name)
        except Exception as e:
            field_set = []

        if not value:
            raise Exception("No value provided")

        q = sa.update(model).values(value).where(pk == id_)

        if field_set and getattr(session.bind, "name", "") != "sqlite":
            row = (await session.execute(q.returning(*field_set))).first()
            result = output(**row)
        else:
            await session.execute(q)
            result = output.get_node(info, id_)

            if isawaitable(result):
                result = await result

        return result


class SQLAlchemyCreateMutation(_BaseMutation):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        model: Type[DeclarativeMeta],
        interfaces=(),
        resolver=None,
        output=None,
        arguments=None,
        only_fields=(),
        exclude_fields=(),
        required_fields=(),
        input_fields: dict = None,
        _meta=None,
        **options,
    ):
        if not _meta:
            _meta = MutationOptions(cls)

        output = output or getattr(cls, "Output", None)
        fields = {}

        for interface in interfaces:
            assert issubclass(
                interface, Interface
            ), f'All interfaces of {cls.__name__} must be a subclass of Interface. Received "{interface}".'
            fields.update(interface._meta.fields)

        if not output:
            # If output is defined, we don't need to get the fields
            fields = {}
            for base in reversed(cls.__mro__):
                fields.update(yank_fields_from_attrs(base.__dict__, _as=Field))
            output = cls

        if not arguments:
            input_class = getattr(cls, "Arguments", None)
            if input_class:
                arguments = props(input_class)
            else:
                if not input_fields:
                    input_fields = get_input_fields(
                        model,
                        only_fields=only_fields,
                        exclude_fields=exclude_fields,
                        required_fields=required_fields,
                    )
                input_type = get_input_type(
                    model, input_fields=input_fields, operation="Create"
                )
                arguments = {
                    "value": graphene.Argument(input_type, required=True),
                }

        if not resolver:
            mutate = getattr(cls, "mutate", None)
            assert mutate, "All mutations must define a mutate method in it"
            resolver = get_unbound_function(mutate)

        if _meta.fields:
            _meta.fields.update(fields)
        else:
            _meta.fields = fields
        _meta.interfaces = interfaces
        _meta.output = output
        _meta.resolver = resolver
        _meta.arguments = arguments
        _meta.model = model

        super().__init_subclass_with_meta__(_meta=_meta, **options)

    @classmethod
    async def mutate(cls, root, info: GraphQLResolveInfo, value: dict):
        session = info.context.session
        model = cls._meta.model
        output = cls._meta.output

        try:
            field_set = get_fields(model, info, output.__name__)
        except Exception as e:
            field_set = []

        if not value:
            raise Exception("No value provided")

        q = sa.insert(model).values(
            {k: v.value if isinstance(v, Enum) else v for k, v in value.items()}
        )

        if field_set and getattr(session.bind, "name", "") != "sqlite":
            row = (await session.execute(q.returning(*field_set))).first()
            result = output(**row)
        else:
            id_ = (await session.execute(q)).inserted_primary_key[0]
            result = output.get_node(info, id_)

            if isawaitable(result):
                result = await result

        return result


class SQLAlchemyDeleteMutation(_BaseMutation):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        model: Type[DeclarativeMeta],
        interfaces=(),
        resolver=None,
        output=None,
        arguments=None,
        only_fields=(),
        exclude_fields=(),
        required_fields=(),
        _meta=None,
        **options,
    ):
        if not _meta:
            _meta = MutationOptions(cls)

        output = output or getattr(cls, "Output", None)
        fields = {}

        for interface in interfaces:
            assert issubclass(
                interface, Interface
            ), f'All interfaces of {cls.__name__} must be a subclass of Interface. Received "{interface}".'
            fields.update(interface._meta.fields)

        if not output:
            # If output is defined, we don't need to get the fields
            fields = {}
            for base in reversed(cls.__mro__):
                fields.update(yank_fields_from_attrs(base.__dict__, _as=Field))
            output = cls

        if not arguments:
            input_class = getattr(cls, "Arguments", None)
            if input_class:
                arguments = props(input_class)
            else:
                arguments = {
                    "id": graphene.ID(required=True),
                }

        if not resolver:
            mutate = getattr(cls, "mutate", None)
            assert mutate, "All mutations must define a mutate method in it"
            resolver = get_unbound_function(mutate)

        if _meta.fields:
            _meta.fields.update(fields)
        else:
            _meta.fields = fields
        _meta.interfaces = interfaces
        _meta.output = output
        _meta.resolver = resolver
        _meta.arguments = arguments
        _meta.model = model

        super().__init_subclass_with_meta__(_meta=_meta, **options)

    @classmethod
    async def mutate(cls, root, info: GraphQLResolveInfo, id: str):
        session = info.context.session
        model = cls._meta.model
        output = cls._meta.output

        table = sa.inspect(model).mapped_table
        pk = table.primary_key.columns[0]

        type_name, id_ = ResolvedGlobalId.decode(id)

        try:
            field_set = get_fields(model, info, type_name)
        except Exception as e:
            field_set = []

        q = sa.delete(model).where(pk == id_)

        if field_set and getattr(session.bind, "name", "") != "sqlite":
            row = (await session.execute(q.returning(*field_set))).first()
            result = output(**row)
        else:
            id_ = (await session.execute(q)).lastrowid
            result = output.get_node(info, id_)

            if isawaitable(result):
                result = await result

        return result
