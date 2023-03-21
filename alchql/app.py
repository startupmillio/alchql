import re
from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, Union

from graphene import Context
from graphql import ExecutionResult, graphql
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, AsyncSessionTransaction
from starlette.background import BackgroundTasks
from starlette.requests import HTTPConnection, Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette_graphene3 import (
    GraphQLApp,
    _get_operation_from_request,
)

from .extensions import Extension, ExtensionManager

DEFAULT_GET = object()

QUERY_REGEX = re.compile(r"^\s*?(query)", flags=re.M)


class SessionQLApp(GraphQLApp):
    def __init__(
        self,
        engine: AsyncEngine,
        context_value: Callable = Context,
        on_get: Optional[
            Callable[[Request], Union[Response, Awaitable[Response]]]
        ] = DEFAULT_GET,
        extensions: List[Type[Extension]] = (),
        raise_exceptions: List[Type[Exception]] = (),
        *args,
        **kwargs,
    ):
        self.engine = engine
        if on_get == DEFAULT_GET:
            on_get = lambda request: HTMLResponse(
                f"""
                <html>
                <head>
                    <script 
                        type="application/javascript" 
                        src="https://embeddable-sandbox.cdn.apollographql.com/_latest/embeddable-sandbox.umd.production.min.js"
                    ></script>
                </head>
                <body style="margin:0">
                    <div style="width: 100%; height: 100%;" id='embedded-sandbox'></div>
                    <script>
                      new window.EmbeddedSandbox({{
                        target: '#embedded-sandbox',
                        initialEndpoint: '{request.url}'
                      }});
                    </script>
                </body>
                </html>
                """
            )

        self.extensions = extensions or ()
        self.raise_exceptions = tuple(raise_exceptions) or ()
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

        is_ro_operation = QUERY_REGEX.search(query) is not None

        async with self._get_context_value(
            request
        ) as context_value, self._get_transaction(is_ro_operation) as transaction:
            context_value.session = transaction.session

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

            if result.errors:
                await transaction.rollback()

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
                    if isinstance(error.original_error, self.raise_exceptions):
                        raise error.original_error
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
        if callable(self.context_value):
            context = self.context_value(
                request=request,
                background=BackgroundTasks(),
            )
            if isawaitable(context):
                context = await context
            yield context
        else:
            yield self.context_value or Context(
                request=request,
                background=BackgroundTasks(),
            )

    @asynccontextmanager
    async def _get_transaction(self, is_ro_operation: bool) -> AsyncSessionTransaction:
        async with AsyncSession(self.engine) as session, session.begin() as transaction:
            execution_options = {}

            if is_ro_operation:
                execution_options["isolation_level"] = "AUTOCOMMIT"
                # does not work with AUTOCOMMIT
                # if getattr(session.bind, "name", "") == "postgresql":
                #     execution_options["postgresql_readonly"] = True
                #     execution_options["postgresql_deferrable"] = True

            if execution_options:
                await session.connection(execution_options=execution_options)

            yield transaction
