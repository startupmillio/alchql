from collections import defaultdict
from typing import TYPE_CHECKING, Type, Union

import sqlalchemy as sa
from graphene import Enum
from sqlalchemy import Column, Table
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.types import Enum as SQLAlchemyEnumType

if TYPE_CHECKING:
    from .types import SQLAlchemyObjectType


class Registry:
    def __init__(self):
        self._registry = {}
        self._registry_models = {}
        self._registry_orm_fields = defaultdict(dict)
        self._registry_composites = {}
        self._registry_enums = {}
        self._registry_sort_enums = {}

    def register(self, obj_type: Type["SQLAlchemyObjectType"]):
        from .types import SQLAlchemyObjectType

        if not isinstance(obj_type, type) or not issubclass(
            obj_type, SQLAlchemyObjectType
        ):
            raise TypeError(f"Expected SQLAlchemyObjectType, but got: {obj_type!r}")
        assert obj_type._meta.registry == self, "Registry for a Model have to match."
        # assert self.get_type_for_model(cls._meta.model) in [None, cls], (
        #     f'SQLAlchemy model "{cls._meta.model}" already associated with '
        #     f'another type "{self._registry[cls._meta.model]}".'
        # )
        self._registry[sa.inspect(obj_type._meta.model).local_table] = obj_type

    def get_type_for_model(self, model: Union[DeclarativeMeta, Table]):
        if isinstance(model, DeclarativeMeta):
            model = sa.inspect(model).local_table
        return self._registry.get(model)

    def register_orm_field(
        self,
        obj_type: Type["SQLAlchemyObjectType"],
        field_name: str,
        orm_field: Column,
    ):
        from .types import SQLAlchemyObjectType

        if not isinstance(obj_type, type) or not issubclass(
            obj_type, SQLAlchemyObjectType
        ):
            raise TypeError(f"Expected SQLAlchemyObjectType, but got: {obj_type!r}")
        if not field_name or not isinstance(field_name, str):
            raise TypeError(f"Expected a field name, but got: {field_name!r}")
        self._registry_orm_fields[obj_type][field_name] = orm_field

    def get_orm_field_for_graphene_field(
        self, obj_type: Type["SQLAlchemyObjectType"], field_name: str
    ):
        return self._registry_orm_fields.get(obj_type, {}).get(field_name)

    def register_composite_converter(self, composite, converter):
        self._registry_composites[composite] = converter

    def get_converter_for_composite(self, composite):
        return self._registry_composites.get(composite)

    def register_enum(self, sa_enum: SQLAlchemyEnumType, graphene_enum: Enum):
        if not isinstance(sa_enum, SQLAlchemyEnumType):
            raise TypeError(f"Expected SQLAlchemyEnumType, but got: {sa_enum!r}")
        if not isinstance(graphene_enum, type(Enum)):
            raise TypeError(f"Expected Graphene Enum, but got: {graphene_enum!r}")

        self._registry_enums[sa_enum] = graphene_enum

    def get_graphene_enum_for_sa_enum(self, sa_enum: SQLAlchemyEnumType):
        return self._registry_enums.get(sa_enum)

    def register_sort_enum(
        self, obj_type: Type["SQLAlchemyObjectType"], sort_enum: Enum
    ):
        from .types import SQLAlchemyObjectType

        if not isinstance(obj_type, type) or not issubclass(
            obj_type, SQLAlchemyObjectType
        ):
            raise TypeError(f"Expected SQLAlchemyObjectType, but got: {obj_type!r}")
        if not isinstance(sort_enum, type(Enum)):
            raise TypeError(f"Expected Graphene Enum, but got: {sort_enum!r}")
        self._registry_sort_enums[obj_type] = sort_enum

    def get_sort_enum_for_object_type(self, obj_type: Type["SQLAlchemyObjectType"]):
        return self._registry_sort_enums.get(obj_type)


registry = None


def get_global_registry():
    global registry
    if not registry:
        registry = Registry()
    return registry


def reset_global_registry():
    global registry
    registry = None
