from inspect import isawaitable
from typing import List, Optional, Protocol

from graphene import Context, ResolveInfo
from graphql import GraphQLError


class Extension(Protocol):
    def __init__(self):
        pass  # pragma: no cover

    def request_started(self, context: Context):
        pass  # pragma: no cover

    def request_finished(self, context: Context):
        pass  # pragma: no cover

    async def resolve(self, next_, parent, info: ResolveInfo, **kwargs):
        result = next_(parent, info, **kwargs)
        if isawaitable(result):
            result = await result
        return result

    def has_errors(self, errors: List[GraphQLError], context: Context):
        pass  # pragma: no cover

    def format(self, context: Context) -> Optional[dict]:
        pass  # pragma: no cover
