from enum import EnumMeta

from graphene import Boolean, Enum, Float, ID, Int, List, String
from graphene.types.json import JSONString
from functools import singledispatch
from sqlalchemy import types
from sqlalchemy.dialects import postgresql

from .enums import enum_for_sa_enum
from .registry import get_global_registry

try:
    from sqlalchemy_utils import (
        ChoiceType,
        JSONType,
        ScalarListType,
        TSVectorType,
    )
except ImportError:
    ChoiceType = JSONType = ScalarListType = TSVectorType = object


@singledispatch
def convert_sqlalchemy_type(type_, column, registry=None):
    raise Exception(
        "Don't know how to convert the SQLAlchemy field %s (%s)"
        % (column, column.__class__)
    )


@convert_sqlalchemy_type.register(types.Time)
@convert_sqlalchemy_type.register(types.String)
@convert_sqlalchemy_type.register(types.Text)
@convert_sqlalchemy_type.register(types.Unicode)
@convert_sqlalchemy_type.register(types.UnicodeText)
@convert_sqlalchemy_type.register(postgresql.UUID)
@convert_sqlalchemy_type.register(postgresql.INET)
@convert_sqlalchemy_type.register(postgresql.CIDR)
@convert_sqlalchemy_type.register(postgresql.TSVECTOR)
@convert_sqlalchemy_type.register(TSVectorType)
def convert_column_to_string(type_, column, registry=None):
    return String


@convert_sqlalchemy_type.register(types.Date)
def convert_column_to_date(type_, column, registry=None):
    from graphene.types.datetime import Date

    return Date


@convert_sqlalchemy_type.register(types.DateTime)
def convert_column_to_datetime(type_, column, registry=None):
    from graphene.types.datetime import DateTime

    return DateTime


@convert_sqlalchemy_type.register(types.SmallInteger)
@convert_sqlalchemy_type.register(types.Integer)
def convert_column_to_int_or_id(type_, column, registry=None):
    return ID if column.primary_key else Int


@convert_sqlalchemy_type.register(types.Boolean)
def convert_column_to_boolean(type_, column, registry=None):
    return Boolean


@convert_sqlalchemy_type.register(types.Float)
@convert_sqlalchemy_type.register(types.Numeric)
@convert_sqlalchemy_type.register(types.BigInteger)
def convert_column_to_float(type_, column, registry=None):
    return Float


@convert_sqlalchemy_type.register(types.Enum)
def convert_enum_to_enum(type_, column, registry=None):
    return lambda: enum_for_sa_enum(type_, registry or get_global_registry())


# TODO Make ChoiceType conversion consistent with other enums
@convert_sqlalchemy_type.register(ChoiceType)
def convert_choice_to_enum(type_, column, registry=None):
    name = f"{column.table.name}_{column.name}".upper()
    if isinstance(type_.choices, EnumMeta):
        # type.choices may be Enum/IntEnum, in ChoiceType both presented as EnumMeta
        # do not use from_enum here because we can have more than one enum column in table
        return Enum(name, list((v.name, v.value) for v in type_.choices))
    else:
        return Enum(name, type_.choices)


@convert_sqlalchemy_type.register(ScalarListType)
def convert_scalar_list_to_list(type_, column, registry=None):
    return List(String)


@convert_sqlalchemy_type.register(types.ARRAY)
@convert_sqlalchemy_type.register(postgresql.ARRAY)
def convert_array_to_list(type_, column, registry=None):
    inner_type = convert_sqlalchemy_type(column.type.item_type, column)
    return List(inner_type)


@convert_sqlalchemy_type.register(postgresql.HSTORE)
@convert_sqlalchemy_type.register(postgresql.JSON)
@convert_sqlalchemy_type.register(postgresql.JSONB)
@convert_sqlalchemy_type.register(JSONType)
@convert_sqlalchemy_type.register(types.JSON)
def convert_json_type_to_string(type_, column, registry=None):
    return JSONString
