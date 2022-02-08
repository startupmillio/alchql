import json
from inspect import isawaitable
import logging

from graphql import GraphQLResolveInfo


class DebugMiddleware:
    def __init__(self, logger: logging.Logger, level: int = logging.INFO):
        self.logger = logger
        self.level = level

    async def resolve(self, next_, root, info: GraphQLResolveInfo, **args):
        if root is None:
            try:
                full_query = json.loads(info.context.request._body)
                if full_query.get("operationName") != "IntrospectionQuery":
                    self.logger.log(
                        self.level,
                        json.dumps(full_query, ensure_ascii=False, sort_keys=True),
                    )
            except Exception as e:
                ...

        result = next_(root, info, **args)
        if isawaitable(result):
            return await result
        return result
