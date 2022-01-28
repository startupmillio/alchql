from typing import Type

import graphene
import sqlalchemy as sa
from graphene_sqlalchemy_core.sqlalchemy_converter import convert_sqlalchemy_type
from sqlalchemy.orm import DeclarativeMeta


def get_input_type(model: DeclarativeMeta) -> Type:
    table = sa.inspect(model).mapped_table

    fields = (
        (
            name,
            convert_sqlalchemy_type(
                getattr(column, "type", None),
                column,
            ),
        )
        for name, column in dict(table.columns).items()
    )

    return type(
        f"Input{model.__name__}",
        (graphene.InputObjectType,),
        {name: field() if callable(field) else field for name, field in fields},
    )
