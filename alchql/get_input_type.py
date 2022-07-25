from typing import Iterable, List, Type, Union

import graphene
import sqlalchemy as sa
from graphene.types.enum import EnumMeta
from graphql import StringValueNode
from sqlalchemy.orm import DeclarativeMeta

from .gql_id import ResolvedGlobalId
from .sqlalchemy_converter import convert_sqlalchemy_type


class ArgID(graphene.Scalar):
    @staticmethod
    def coerce_id(value):
        global_id = ResolvedGlobalId.decode(value)

        # registry = get_global_registry()
        # type_ = registry.get_type_for_model(table)
        # if type_ and type_.__name__ != global_id.type:
        #     raise Exception(
        #         f"Invalid GlobalID type: {global_id.type} != {type_.__name__}"
        #     )

        return global_id.id

    serialize = coerce_id
    parse_value = coerce_id

    @staticmethod
    def parse_literal(ast):
        if isinstance(ast, StringValueNode):
            return ast.value


def convert_sqlalchemy_type_mutation(column):
    field = convert_sqlalchemy_type(
        getattr(column, "type", None),
        column,
    )
    if field == graphene.Int and column.foreign_keys:
        field = ArgID()

    return field


def get_input_fields(
    model: Type[DeclarativeMeta],
    only_fields: Iterable[str] = (),
    exclude_fields: Iterable[str] = (),
    required_fields: Iterable[str] = (),
) -> dict:
    only_fields = set(only_fields)
    exclude_fields = set(exclude_fields)
    required_fields = set(required_fields)

    if only_fields and exclude_fields:
        raise ValueError(
            "The options 'only_fields' and 'exclude_fields' cannot be both set on the same type."
        )

    table = sa.inspect(model).persist_selectable
    fields = {}
    for name, column in dict(table.columns).items():
        if name not in required_fields:
            if only_fields and name not in only_fields:
                continue
            if exclude_fields and name in exclude_fields:
                continue

        field = convert_sqlalchemy_type_mutation(column)

        if callable(field):
            field = field()
        if isinstance(field, EnumMeta):
            field = field()

        if name in required_fields:
            getattr(field, "kwargs", {})["required"] = True

        fields[name] = field

    return fields


def get_input_type(name: str, input_fields: dict) -> Type:
    return type(name, (graphene.InputObjectType,), input_fields)
