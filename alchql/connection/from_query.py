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
    count_query: Query,
    session: AsyncSession,
    args: Optional[dict] = None,
    page_info_fields: Optional[list] = None,
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

    page_info_fields = page_info_fields or []
    has_next_page_check = "has_next_page" in page_info_fields

    list_length = None
    edge_type = connection_type.Edge

    before = args.get("before")
    after = args.get("after")
    first = args.get("first")
    last = args.get("last")

    total_count = None
    if last and not before:
        total_count = (await session.execute(count_query)).scalar()
        right_offset = total_count
    else:
        right_offset = get_offset_with_default(before)

    left_offset = get_offset_with_default(after) + 1 if after else 0

    if isinstance(first, int):
        right_offset = min(left_offset + first, right_offset)
    if isinstance(last, int):
        left_offset = max(left_offset, right_offset - last)

    _slice = query
    # If supplied slice is too large, trim it down before mapping over it.
    original_limit = first or last
    if original_limit:
        limit = original_limit + 1 if has_next_page_check else original_limit
        _slice = _slice.limit(limit)
    if left_offset:
        _slice = _slice.offset(left_offset)

    edges = []

    for i, v in enumerate(await session.execute(_slice)):
        node_type = edge_type.node.type
        node_value = filter_requested_fields_for_object(dict(v), node_type)
        edge = edge_type(
            node=node_type(**node_value),
            cursor=offset_to_cursor(left_offset + i),
        )
        edges.append(edge)

    has_next_page_check = original_limit and len(edges) > original_limit
    edges = edges[:original_limit]

    connection_init_kwargs = {"edges": edges}
    if page_info_fields:
        page_info_kwargs = {}

        for field in page_info_fields:
            if field == "start_cursor":
                page_info_kwargs[field] = edges[0].cursor if edges else None
            elif field == "end_cursor":
                page_info_kwargs[field] = edges[-1].cursor if edges else None
            elif field == "has_previous_page":
                page_info_kwargs[field] = left_offset > 0
            elif field == "has_next_page":
                page_info_kwargs[field] = has_next_page_check

        connection_init_kwargs["page_info"] = page_info_type(**page_info_kwargs)

    connection = connection_type(**connection_init_kwargs)

    if hasattr(connection, "total_count"):

        # get max count
        if total_count is None:
            list_length = (await session.execute(count_query)).scalar()

        connection.total_count = list_length

    return connection
