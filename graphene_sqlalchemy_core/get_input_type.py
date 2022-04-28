import hashlib
import json
from typing import List, Type

import graphene
import sqlalchemy as sa
from graphene.types.enum import EnumMeta
from graphql import StringValueNode
from sqlalchemy.orm import DeclarativeMeta

from .gql_id import ResolvedGlobalId
from .registry import get_global_registry
from .sqlalchemy_converter import convert_sqlalchemy_type

# There we contain unique type names
initialized_types = {}


def get_unique_input_type_name(
    model_name: str, input_fields: dict, operation: str
) -> str:
    input_type_name = f"Input{operation}{model_name}"
    model_hash = hashlib.md5(
        json.dumps(
            {
                key: f"{type(value)}{getattr(value, 'kwargs', '') or ''}"
                for key, value in input_fields.items()
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    if initialized_types.setdefault(input_type_name, model_hash) == model_hash:
        return input_type_name

    input_type_name = input_type_name + "_"
    for i in model_hash:
        input_type_name += i
        if initialized_types.setdefault(input_type_name, model_hash) == model_hash:
            return input_type_name


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
    only_fields: List = (),
    exclude_fields: List = (),
    required_fields: List = (),
) -> dict:
    if only_fields and exclude_fields:
        raise ValueError(
            "The options 'only_fields' and 'exclude_fields' cannot be both set on the same type."
        )

    table = sa.inspect(model).mapped_table

    fields = {}
    for name, column in dict(table.columns).items():
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


def get_input_type(
    model: Type[DeclarativeMeta], input_fields: dict, operation: str
) -> Type:
    return type(
        get_unique_input_type_name(
            model_name=model.__name__, input_fields=input_fields, operation=operation
        ),
        (graphene.InputObjectType,),
        input_fields,
    )
