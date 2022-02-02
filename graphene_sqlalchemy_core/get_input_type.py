from typing import List, Type

import graphene
import sqlalchemy as sa
from graphene.types.enum import EnumMeta
from sqlalchemy.orm import DeclarativeMeta

from .sqlalchemy_converter import convert_sqlalchemy_type


def get_input_fields(
    model: DeclarativeMeta, only_fields: List = (), exclude_fields: List = ()
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

        fields[name] = field

    return fields


def get_input_type(
    model: DeclarativeMeta, only_fields: List = (), exclude_fields: List = ()
) -> Type:
    return type(
        f"Input{model.__name__}",
        (graphene.InputObjectType,),
        get_input_fields(
            model=model,
            only_fields=only_fields,
            exclude_fields=exclude_fields,
        ),
    )
