from contextlib import contextmanager
from inspect import isawaitable
from typing import Any, Dict, List, Optional, Protocol, Type

from graphene import Context
from graphql import GraphQLError, GraphQLResolveInfo


class Extension(Protocol):
    def __init__(self):
        pass  # pragma: no cover

    def request_started(self, context: Context):
        pass  # pragma: no cover

    def request_finished(self, context: Context):
        pass  # pragma: no cover

    async def resolve(self, next_, parent, info: GraphQLResolveInfo, **kwargs):
        result = next_(parent, info, **kwargs)
        if isawaitable(result):
            result = await result
        return result

    def has_errors(self, errors: List[GraphQLError], context: Context):
        pass  # pragma: no cover

    def format(self, context: Context) -> Optional[dict]:
        pass  # pragma: no cover


class ExtensionManager:
    __slots__ = ("context", "extensions")

    def __init__(
        self,
        extensions: List[Type[Extension]] = None,
        context: Context = None,
    ):
        self.context = context

        if extensions:
            self.extensions = tuple(ext() for ext in extensions)
        else:
            self.extensions = ()

    @contextmanager
    def request(self):
        for ext in self.extensions:
            ext.request_started(self.context)
        try:
            yield
        finally:
            for ext in self.extensions[::-1]:
                ext.request_finished(self.context)

    def has_errors(self, errors: List[GraphQLError]):
        for ext in self.extensions:
            ext.has_errors(errors, self.context)

    def format(self) -> Dict[str, Any]:
        data = {}
        for ext in self.extensions:
            ext_data = ext.format(self.context)
            if ext_data:
                data.update(ext_data)
        return data
