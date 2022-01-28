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
from graphql_relay import from_global_id
from sqlalchemy.orm import DeclarativeMeta

from .utils import get_query
from .get_input_type import get_input_type
from .gql_fields import get_fields


class SQLMutationOptions(ObjectTypeOptions):
    model: DeclarativeMeta = None
    arguments: Dict[str, Argument] = None
    output: Type[ObjectType] = None
    resolver: Callable = None
    interfaces: Iterable[Type[Interface]] = ()


class SQLAlchemyUpdateMutation(ObjectType):
    _meta: Type[SQLMutationOptions]
    """
    Object Type Definition (mutation field)

    Mutation is a convenience type that helps us build a Field which takes Arguments and returns a
    mutation Output ObjectType.

    .. code:: python

        from graphene import Mutation, ObjectType, String, Boolean, Field

        class CreatePerson(Mutation):
            class Arguments:
                name = String()

            def mutate(parent, info, name):
                person = Person(name=name)
                ok = True
                return CreatePerson(person=person, ok=ok)

        class Mutation(ObjectType):
            create_person = CreatePerson.Field()

    Meta class options (optional):
        model (DeclarativeMeta): SQLAlchemy model type
        output (graphene.ObjectType): Or ``Output`` inner class with attributes on Mutation class.
            Or attributes from Mutation class. Fields which can be returned from this mutation
            field.
        resolver (Callable resolver method): Or ``mutate`` method on Mutation class. Perform data
            change and return output.
        arguments (Dict[str, graphene.Argument]): Or ``Arguments`` inner class with attributes on
            Mutation class. Arguments to use for the mutation Field.
        name (str): Name of the GraphQL type (must be unique in schema). Defaults to class
            name.
        description (str): Description of the GraphQL type in the schema. Defaults to class
            docstring.
        interfaces (Iterable[graphene.Interface]): GraphQL interfaces to extend with the payload
            object. All fields from interface will be included in this object's schema.
        fields (Dict[str, graphene.Field]): Dictionary of field name to Field. Not recommended to
            use (prefer class attributes or ``Meta.output``).
    """

    @classmethod
    def __init_subclass_with_meta__(
        cls,
        interfaces=(),
        resolver=None,
        output=None,
        arguments=None,
        model: DeclarativeMeta = None,
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
                input_type = get_input_type(model)
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

        super(SQLAlchemyUpdateMutation, cls).__init_subclass_with_meta__(
            _meta=_meta, **options
        )

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
    async def mutate(cls, root, info, id, value):
        session = info.context.session
        model = cls._meta.model
        output = cls._meta.output

        table = sa.inspect(model).mapped_table
        pk = table.primary_key.columns[0]

        type_name, id_ = from_global_id(id)

        try:
            field_set = get_fields(model, info, type_name)
        except Exception as e:
            field_set = []

        if not value:
            raise Exception("No value provided")

        q = sa.update(model).values(value).where(pk == int(id_))

        if field_set and session.bind.name != "sqlite":
            row = (await session.execute(q.returning(*field_set))).first()
            result = output(**row)
        else:
            await session.execute(q)
            result = output.get_node(info, id_)

            if isawaitable(result):
                result = await result

        return result

    @classmethod
    async def get_query(cls, info):
        return get_query(cls._meta.model, info, cls.__name__)

    @classmethod
    async def get_node(cls, info, id):
        session = info.context.session

        pk = sqlalchemy.inspect(cls._meta.model).primary_key[0]
        q = (await cls.get_query(info)).where(pk == id)
        result = cls(**(await session.execute(q)).first())
        return result
