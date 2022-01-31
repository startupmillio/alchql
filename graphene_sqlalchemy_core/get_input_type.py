from typing import Type

import graphene
import sqlalchemy as sa
from graphene.types.enum import EnumMeta
from sqlalchemy.orm import DeclarativeMeta

from .sqlalchemy_converter import convert_sqlalchemy_type


def get_input_fields(model: DeclarativeMeta) -> dict:
    table = sa.inspect(model).mapped_table

    fields = {}
    for name, column in dict(table.columns).items():
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


def get_input_type(model: DeclarativeMeta) -> Type:
    return type(
        f"Input{model.__name__}",
        (graphene.InputObjectType,),
        get_input_fields(model),
    )
