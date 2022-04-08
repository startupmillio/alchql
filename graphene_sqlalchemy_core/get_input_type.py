import hashlib
from typing import List, Type

import graphene
import sqlalchemy as sa
from graphene.types.enum import EnumMeta
from sqlalchemy.orm import DeclarativeMeta

from .sqlalchemy_converter import convert_sqlalchemy_type


# There we contain unique type names
initialized_types = {}


def get_unique_input_type_name(model_name: str, input_fields: dict) -> str:
    input_type_name = f"Input{model_name}"

    model_hash = hashlib.md5(
        str(
            {
                key: f"{type(value)}({getattr(value, 'kwargs', '')})"
                for key, value in sorted(input_fields.items(), key=lambda item: item[0])
            }
        ).encode("utf-8")
    ).hexdigest()

    if initialized_types.setdefault(input_type_name, model_hash) == model_hash:
        return input_type_name

    input_type_name = f"Input{model_name}" + str(model_hash)[:5]
    initialized_types[input_type_name] = model_hash

    return input_type_name


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

        field = convert_sqlalchemy_type(
            getattr(column, "type", None),
            column,
        )
        if callable(field):
            field = field()
        if isinstance(field, EnumMeta):
            field = field()

        if name in required_fields:
            getattr(field, "kwargs", {})["required"] = True

        fields[name] = field

    return fields


def get_input_type(model: Type[DeclarativeMeta], input_fields: dict) -> Type:
    return type(
        get_unique_input_type_name(
            model_name=model.__name__, input_fields=input_fields
        ),
        (graphene.InputObjectType,),
        input_fields,
    )
