from typing import Optional, Type

from graphene import Connection, PageInfo
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query

from .utils import (
    get_offset_with_default,
    offset_to_cursor,
)
from ..utils import filter_requested_fields_for_object


async def connection_from_query(
    query: Query,
    session: AsyncSession,
    args: Optional[dict] = None,
    list_length: int = 0,
    connection_type: Type[Connection] = Connection,
    page_info_type: Type[PageInfo] = PageInfo,
) -> Connection:
    """
    Given a slice (subset) of an array, returns a connection object for use in
    GraphQL.
    This function is similar to `connectionFromArray`, but is intended for use
    cases where you know the cardinality of the connection, consider it too large
    to materialize the entire array, and instead wish pass in a slice of the
    total result large enough to cover the range specified in `args`.
    """
    args = args or {}
    edge_type = connection_type.Edge

    before = args.get("before")
    after = args.get("after")
    first = args.get("first")
    last = args.get("last")

    before_offset = get_offset_with_default(before, list_length)
    after_offset = get_offset_with_default(after, -1)

    start_offset = max(after_offset, -1) + 1
    end_offset = min(before_offset, list_length)

    if isinstance(first, int):
        end_offset = min(end_offset, start_offset + first)
    if isinstance(last, int):
        start_offset = max(start_offset, end_offset - last)

    lower_bound = after_offset + 1 if after else 0
    upper_bound = before_offset if before else list_length

    _slice = query
    # If supplied slice is too large, trim it down before mapping over it.
    limit = first or last or end_offset
    if limit:
        _slice = _slice.limit(limit)
    if start_offset:
        _slice = _slice.offset(start_offset)

    edges = []

    for i, v in enumerate(await session.execute(_slice)):
        node_type = edge_type.node.type
        node_value = filter_requested_fields_for_object(dict(v), node_type)
        edge = edge_type(
            node=node_type(**node_value),
            cursor=offset_to_cursor(start_offset + i),
        )
        edges.append(edge)

    first_edge_cursor = edges[0].cursor if edges else None
    last_edge_cursor = edges[-1].cursor if edges else None

    return connection_type(
        edges=edges,
        page_info=page_info_type(
            start_cursor=first_edge_cursor,
            end_cursor=last_edge_cursor,
            has_previous_page=isinstance(last, int) and start_offset > lower_bound,
            has_next_page=isinstance(first, int)
            and len(edges) == first
            and end_offset < upper_bound,
        ),
    )
