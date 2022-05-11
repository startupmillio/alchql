import json
import logging
from inspect import isawaitable
from typing import Callable

from graphql import GraphQLResolveInfo


class BaseDebugMiddleware:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    async def resolve(self, next_, root, info: GraphQLResolveInfo, **args):
        if root is None:
            try:
                self.log(info)
            except Exception as e:
                ...

        result = next_(root, info, **args)
        if isawaitable(result):
            return await result

        return result

    def log(self, info: GraphQLResolveInfo):
        raise NotImplementedError()


class LogMiddleware(BaseDebugMiddleware):
    def __init__(
        self,
        logger: logging.Logger = logging.getLogger("gsc"),
        level: int = logging.INFO,
    ):
        super().__init__(logger, level)

    def log(self, info):
        full_query = json.loads(info.context.request._body)
        if full_query.get("operationName") != "IntrospectionQuery":
            text = json.dumps(full_query, ensure_ascii=False, sort_keys=True)
            self.logger.log(self.level, text)


class BreadcrumbMiddleware(BaseDebugMiddleware):
    logger: Callable

    def __init__(self, level: str = "info"):
        from sentry_sdk import add_breadcrumb

        super().__init__(add_breadcrumb, level)

    def log(self, info):
        full_query = json.loads(info.context.request._body)
        if full_query.get("operationName") != "IntrospectionQuery":
            if full_query.get("query"):
                self.logger(
                    category="graphql", message=full_query["query"], level="info"
                )


DebugMiddleware = LogMiddleware
