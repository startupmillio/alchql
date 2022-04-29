from typing import Callable, Dict, Optional, Sequence, TYPE_CHECKING, Type

from graphene import Argument, Enum, List
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.sql.elements import UnaryExpression
from sqlalchemy.types import Enum as SQLAlchemyEnumType

from .registry import Registry
from .utils import get_object_type_manual_fields, to_enum_value_name, to_type_name

if TYPE_CHECKING:
    from .types import SQLAlchemyObjectType


def _convert_sa_to_graphene_enum(sa_enum: SQLAlchemyEnumType, fallback_name=None):
    """Convert the given SQLAlchemy Enum type to a Graphene Enum type.

    The name of the Graphene Enum will be determined as follows:
    If the SQLAlchemy Enum is based on a Python Enum, use the name
    of the Python Enum.  Otherwise, if the SQLAlchemy Enum is named,
    use the SQL name after conversion to a type name. Otherwise, use
    the given fallback_name or raise an error if it is empty.

    The Enum value names are converted to upper case if necessary.
    """
    if not isinstance(sa_enum, SQLAlchemyEnumType):
        raise TypeError(f"Expected sqlalchemy.types.Enum, but got: {sa_enum!r}")
    enum_class = sa_enum.enum_class
    if enum_class:
        if all(to_enum_value_name(key) == key for key in enum_class.__members__):
            return Enum.from_enum(enum_class)
        name = enum_class.__name__
        members = [
            (to_enum_value_name(key), value.value)
            for key, value in enum_class.__members__.items()
        ]
    else:
        sql_enum_name = sa_enum.name
        if sql_enum_name:
            name = to_type_name(sql_enum_name)
        elif fallback_name:
            name = fallback_name
        else:
            raise TypeError(f"No type name specified for {sa_enum!r}")
        members = [(to_enum_value_name(key), key) for key in sa_enum.enums]
    return Enum(name, members)


def enum_for_sa_enum(sa_enum: SQLAlchemyEnumType, registry: Registry):
    """Return the Graphene Enum type for the specified SQLAlchemy Enum type."""
    if not isinstance(sa_enum, SQLAlchemyEnumType):
        raise TypeError(f"Expected sqlalchemy.types.Enum, but got: {sa_enum!r}")
    enum = registry.get_graphene_enum_for_sa_enum(sa_enum)
    if not enum:
        enum = _convert_sa_to_graphene_enum(sa_enum)
        registry.register_enum(sa_enum, enum)
    return enum


def enum_for_field(obj_type: Type["SQLAlchemyObjectType"], field_name: str):
    """Return the Graphene Enum type for the specified Graphene field."""
    from .types import SQLAlchemyObjectType

    if not isinstance(obj_type, type) or not issubclass(obj_type, SQLAlchemyObjectType):
        raise TypeError(f"Expected SQLAlchemyObjectType, but got: {obj_type!r}")
    if not field_name or not isinstance(field_name, str):
        raise TypeError(f"Expected a field name, but got: {field_name!r}")
    registry = obj_type._meta.registry
    orm_field = registry.get_orm_field_for_graphene_field(obj_type, field_name)
    if orm_field is None:
        raise TypeError(f"Cannot get {obj_type._meta.name}.{field_name}")
    if not isinstance(orm_field, ColumnProperty):
        raise TypeError(
            f"{obj_type._meta.name}.{field_name} does not map to model column"
        )
    column = orm_field.columns[0]
    sa_enum = column.type
    if not isinstance(sa_enum, SQLAlchemyEnumType):
        raise TypeError(
            f"{obj_type._meta.name}.{field_name} does not map to enum column"
        )
    enum = registry.get_graphene_enum_for_sa_enum(sa_enum)
    if not enum:
        fallback_name = obj_type._meta.name + to_type_name(field_name)
        enum = _convert_sa_to_graphene_enum(sa_enum, fallback_name)
        registry.register_enum(sa_enum, enum)
    return enum


def _default_sort_enum_symbol_name(column_name: str, sort_asc=True):
    return to_enum_value_name(column_name) + ("_ASC" if sort_asc else "_DESC")


def sort_enum_for_object_type(
    obj_type: Type["SQLAlchemyObjectType"],
    name: Optional[str] = None,
    only_fields: Optional[Sequence[str]] = None,
    only_indexed: Optional[bool] = None,
    get_symbol_name: Optional[Callable] = None,
    extra_members: Optional[Dict[str, UnaryExpression]] = None,
):
    """Return Graphene Enum for sorting the given SQLAlchemyObjectType.

    Parameters
    - obj_type: The object type for which the sort Enum shall be generated.
    - name: Name to use for the sort Enum.
        If not provided, it will be set to the object type name + 'SortEnum'
    - only_fields: If this is set, only fields from this sequence will be considered.
    - only_indexed : If this is set, only indexed columns will be considered.
    - get_symbol_name : Function which takes the column name and a boolean indicating
        if the sort direction is ascending, and returns the symbol name
        for the current column and sort direction. If no such function
        is passed, a default function will be used that creates the symbols
        'foo_asc' and 'foo_desc' for a column with the name 'foo'.

    Returns
    - Enum
        The Graphene Enum type
    """
    name = name or obj_type._meta.name + "SortEnum"
    registry = obj_type._meta.registry
    enum = registry.get_sort_enum_for_object_type(obj_type)
    custom_options = dict(
        only_fields=only_fields,
        only_indexed=only_indexed,
        get_symbol_name=get_symbol_name,
    )
    if enum:
        if name != enum.__name__ or custom_options != enum.custom_options:
            raise ValueError(f"Sort enum for {obj_type} has already been customized")
    else:
        members = {}
        default = []
        fields = obj_type._meta.fields
        get_name = get_symbol_name or _default_sort_enum_symbol_name
        for field_name in fields:
            if only_fields and field_name not in only_fields:
                continue
            orm_field = registry.get_orm_field_for_graphene_field(obj_type, field_name)
            if not isinstance(orm_field, ColumnProperty):
                continue
            column = orm_field.columns[0]
            if only_indexed and not (column.primary_key or column.index):
                continue
            asc_name = get_name(field_name, True)
            asc_value = column.asc()
            desc_name = get_name(field_name, False)
            desc_value = column.desc()
            if column.primary_key:
                default.append(asc_value)
            members.update({asc_name: asc_value, desc_name: desc_value})

        extra_fields = get_object_type_manual_fields(obj_type)
        for field_name, attr in extra_fields.items():
            if hasattr(attr, "model_field"):
                column = getattr(attr, "model_field", None)
                if column is not None:
                    asc_name = get_name(field_name, True)
                    asc_value = column.asc()
                    desc_name = get_name(field_name, False)
                    desc_value = column.desc()
                    if column.primary_key:
                        default.append(asc_value)
                    members.update({asc_name: asc_value, desc_name: desc_value})

        if extra_members:
            members.update(extra_members)

        enum = Enum(name, members.items())
        enum.default = default  # store default as attribute
        enum.custom_options = custom_options
        registry.register_sort_enum(obj_type, enum)
    return enum


def sort_argument_for_object_type(
    obj_type: Type["SQLAlchemyObjectType"],
    enum_name: Optional[str] = None,
    only_fields: Optional[Sequence] = None,
    only_indexed: Optional[bool] = None,
    get_symbol_name: Optional[Callable] = None,
    has_default: bool = True,
):
    """Returns Graphene Argument for sorting the given SQLAlchemyObjectType.

    Parameters
    - obj_type: The object type for which the sort Argument shall be generated.
    - enum_name: Name to use for the sort Enum.
        If not provided, it will be set to the object type name + 'SortEnum'
    - only_fields: If this is set, only fields from this sequence will be considered.
    - only_indexed: If this is set, only indexed columns will be considered.
    - get_symbol_name: Function which takes the column name and a boolean indicating
        if the sort direction is ascending, and returns the symbol name
        for the current column and sort direction. If no such function
        is passed, a default function will be used that creates the symbols
        'foo_asc' and 'foo_desc' for a column with the name 'foo'.
    - has_default: If this is set to False, no sorting will happen when this argument is not
        passed. Otherwise results will be sortied by the primary key(s) of the model.

    Returns
    - Enum
        A Graphene Argument that accepts a list of sorting directions for the model.
    """
    enum = obj_type.sort_enum(
        enum_name,
        only_fields=only_fields,
        only_indexed=only_indexed,
        get_symbol_name=get_symbol_name,
    )
    if not has_default:
        enum.default = None

    return Argument(List(enum), default_value=enum.default)
