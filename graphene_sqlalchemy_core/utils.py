import logging
import re
import warnings
from dataclasses import dataclass
from typing import Optional, Union

import graphene
import sqlalchemy as sa
from graphene import Field, Scalar
from graphene.types.objecttype import ObjectTypeMeta
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import class_mapper, object_mapper
from sqlalchemy.orm.exc import UnmappedClassError, UnmappedInstanceError

from .gql_fields import get_fields


@dataclass
class FilterItem:
    field_type: graphene.Field
    filter_func: callable
    value_func: Optional[callable] = lambda x: x


@dataclass
class GlobalFilters:
    ID__EQ = "id__eq"
    ID__IN = "id__in"


def get_session(context):
    if hasattr(context, "session"):
        return context.session
    elif hasattr(context, "get"):
        return context.get("session")
    else:
        raise Exception("Session not found")


def get_query(model, info):
    try:
        if info:
            fields = get_fields(model, info)
        else:
            fields = model.__table__.columns
    except Exception as e:
        logging.error(e)
        fields = model.__table__.columns

    return sa.select(fields)


def is_mapped_class(cls):
    try:
        class_mapper(cls)
    except (ArgumentError, UnmappedClassError):
        return False
    else:
        return True


def is_mapped_instance(cls):
    try:
        object_mapper(cls)
    except (ArgumentError, UnmappedInstanceError):
        return False
    else:
        return True


def to_type_name(name):
    """Convert the given name to a GraphQL type name."""
    return "".join(part[:1].upper() + part[1:] for part in name.split("_"))


_re_enum_value_name_1 = re.compile("(.)([A-Z][a-z]+)")
_re_enum_value_name_2 = re.compile("([a-z0-9])([A-Z])")


def to_enum_value_name(name):
    """Convert the given name to a GraphQL enum value name."""
    return _re_enum_value_name_2.sub(
        r"\1_\2", _re_enum_value_name_1.sub(r"\1_\2", name)
    ).upper()


class EnumValue(str):
    """String that has an additional value attached.

    This is used to attach SQLAlchemy model columns to Enum symbols.
    """

    def __new__(cls, s, value):
        return super().__new__(cls, s)

    def __init__(self, _s, value):
        super().__init__()
        self.value = value


def _deprecated_default_symbol_name(column_name, sort_asc):
    return column_name + ("_asc" if sort_asc else "_desc")


# unfortunately, we cannot use lru_cache because we still support Python 2
_deprecated_object_type_cache = {}


def _deprecated_object_type_for_model(cls, name):

    try:
        return _deprecated_object_type_cache[cls, name]
    except KeyError:
        from .types import SQLAlchemyObjectType

        obj_type_name = name or cls.__name__

        class ObjType(SQLAlchemyObjectType):
            class Meta:
                name = obj_type_name
                model = cls

        _deprecated_object_type_cache[cls, name] = ObjType
        return ObjType


def sort_enum_for_model(cls, name=None, symbol_name=None):
    """Get a Graphene Enum for sorting the given model class.

    This is deprecated, please use object_type.sort_enum() instead.
    """
    warnings.warn(
        "sort_enum_for_model() is deprecated; use object_type.sort_enum() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    from .enums import sort_enum_for_object_type

    return sort_enum_for_object_type(
        _deprecated_object_type_for_model(cls, name),
        name,
        get_symbol_name=symbol_name or _deprecated_default_symbol_name,
    )


def sort_argument_for_model(cls, has_default=True):
    """Get a Graphene Argument for sorting the given model class.

    This is deprecated, please use object_type.sort_argument() instead.
    """
    warnings.warn(
        "sort_argument_for_model() is deprecated;"
        " use object_type.sort_argument() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    from graphene import Argument, List
    from .enums import sort_enum_for_object_type

    enum = sort_enum_for_object_type(
        _deprecated_object_type_for_model(cls, None),
        get_symbol_name=_deprecated_default_symbol_name,
    )
    if not has_default:
        enum.default = None

    return Argument(List(enum), default_value=enum.default)


def filter_value_to_python(value):
    """
    Turn the string `value` into a python object.
    >>> filter_value_to_python([1, 2, 3])
    [1, 2, 3]
    >>> filter_value_to_python(123)
    123
    >>> filter_value_to_python('true')
    True
    >>> filter_value_to_python('False')
    False
    >>> filter_value_to_python('null')
    >>> filter_value_to_python('None')
    >>> filter_value_to_python('Ø')
    u'O'
    """
    if isinstance(value, list):
        return value
    if isinstance(value, int):
        return value

    # Simple values
    if value in ["true", "True", True]:
        value = True
    elif value in ["false", "False", False]:
        value = False
    elif value in ("null", "none", "None", None):
        value = None

    return value


def filter_requested_fields_for_object(
    data: dict, conversion_type: Union[ObjectTypeMeta, object]
):
    if not isinstance(conversion_type, ObjectTypeMeta):
        return data

    result = {}
    fields = conversion_type._meta.fields.keys()
    for key, value in data.items():
        if key in fields:
            result[key] = value
        else:
            attr = getattr(conversion_type, key, None)
            if attr and isinstance(attr, (Field, Scalar)):
                result[key] = value

    return result
