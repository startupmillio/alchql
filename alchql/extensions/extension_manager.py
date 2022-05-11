from contextlib import contextmanager
from typing import Any, Dict, List, Type

from graphene import Context
from graphql import GraphQLError

from .extension import Extension


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
