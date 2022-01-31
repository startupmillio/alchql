from typing import Callable, TYPE_CHECKING, Type

from graphene.utils.get_unbound_function import get_unbound_function

if TYPE_CHECKING:
    from .types import SQLAlchemyObjectType


def get_custom_resolver(obj_type: Type["SQLAlchemyObjectType"], orm_field_name):
    """
    Since `graphene` will call `resolve_<field_name>` on a field only if it
    does not have a `resolver`, we need to re-implement that logic here so
    users are able to override the default resolvers that we provide.
    """
    resolver = getattr(obj_type, f"resolve_{orm_field_name}", None)
    if resolver:
        return get_unbound_function(resolver)

    return None


def get_attr_resolver(model_attr: str) -> Callable:
    """
    In order to support field renaming via `ORMField.model_attr`,
    we need to define resolver functions for each field.
    """

    async def resolver(root, _info):
        return getattr(root, model_attr, None)

    return resolver
