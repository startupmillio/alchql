from typing import Type

import graphene
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeMeta

from .sqlalchemy_converter import convert_sqlalchemy_type


def get_input_fields(model: DeclarativeMeta) -> dict:
    table = sa.inspect(model).mapped_table

    fields = (
        (
            name,
            convert_sqlalchemy_type(
                getattr(column, "type", None),
                column,
            ),
        )
        for name, column in table.columns.items()
    )

    return {name: field() if callable(field) else field for name, field in fields}


def get_input_type(model: DeclarativeMeta) -> Type:
    return type(
        f"Input{model.__name__}",
        (graphene.InputObjectType,),
        get_input_fields(model),
    )
