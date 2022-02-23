from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, Union

from graphene import Context
from graphql import ExecutionResult, graphql
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from starlette.background import BackgroundTasks
from starlette.requests import HTTPConnection, Request
from starlette.responses import JSONResponse, Response
from starlette_graphene3 import (
    GraphQLApp,
    _get_operation_from_request,
    make_graphiql_handler,
)

from .extensions import Extension, ExtensionManager

DEFAULT_GET = object()


class SessionQLApp(GraphQLApp):
    def __init__(
        self,
        engine: AsyncEngine,
        context_value: Callable = Context,
        on_get: Optional[
            Callable[[Request], Union[Response, Awaitable[Response]]]
        ] = DEFAULT_GET,
        extensions: List[Type[Extension]] = (),
        *args,
        **kwargs,
    ):
        self.engine = engine
        if on_get == DEFAULT_GET:
            on_get = make_graphiql_handler()

        self.extensions = extensions or ()
        super().__init__(context_value=context_value, on_get=on_get, *args, **kwargs)

    async def _handle_http_request(self, request: Request) -> JSONResponse:
        try:
            operations = await _get_operation_from_request(request)
        except ValueError as e:
            return JSONResponse({"errors": [e.args[0]]}, status_code=400)

        if isinstance(operations, list):
            return JSONResponse(
                {"errors": ["This server does not support batching"]}, status_code=400
            )
        else:
            operation = operations

        query = operation["query"]
        variable_values = operation.get("variables")
        operation_name = operation.get("operationName")

        async with self._get_context_value(request) as context_value:
            middleware = self.middleware or ()
            extension_manager = ExtensionManager(self.extensions, context=context_value)

            with extension_manager.request():
                result: ExecutionResult = await graphql(
                    self.schema.graphql_schema,
                    source=query,
                    context_value=context_value,
                    root_value=self.root_value,
                    middleware=(*middleware, *extension_manager.extensions),
                    variable_values=variable_values,
                    operation_name=operation_name,
                    execution_context_class=self.execution_context_class,
                )

            extension_results = extension_manager.format()
            if extension_results:
                result.extensions = extension_results

            background = getattr(context_value, "background", None)

        response: Dict[str, Any] = {"data": result.data}
        if result.errors:
            for error in result.errors:
                if error.original_error:
                    self.logger.error(
                        "An exception occurred in resolvers",
                        exc_info=error.original_error,
                    )
            response["errors"] = [
                self.error_formatter(error) for error in result.errors
            ]
        if result.extensions:
            response["extensions"] = result.extensions

        return JSONResponse(
            response,
            status_code=200,
            background=background,
        )

    @asynccontextmanager
    async def _get_context_value(self, request: HTTPConnection) -> Context:
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                if callable(self.context_value):
                    context = self.context_value(
                        request=request,
                        background=BackgroundTasks(),
                        session=session,
                    )
                    if isawaitable(context):
                        context = await context
                    yield context
                else:
                    yield self.context_value or Context(
                        request=request,
                        background=BackgroundTasks(),
                        session=session,
                    )
