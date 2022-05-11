from graphql_relay import get_offset_with_default, offset_to_cursor
from sqlalchemy.ext.asyncio import AsyncSession

from .utils import filter_requested_fields_for_object


async def connection_from_query(
    query,
    session: AsyncSession,
    args=None,
    connection_type=None,
    edge_type=None,
    page_info_type=None,
    slice_start=0,
    list_length=0,
    list_slice_length=None,
):
    """
    Given a slice (subset) of an array, returns a connection object for use in
    GraphQL.
    This function is similar to `connectionFromArray`, but is intended for use
    cases where you know the cardinality of the connection, consider it too large
    to materialize the entire array, and instead wish pass in a slice of the
    total result large enough to cover the range specified in `args`.
    """
    args = args or {}

    before = args.get("before")
    after = args.get("after")
    first = args.get("first")
    last = args.get("last")

    slice_end = slice_start + list_slice_length
    before_offset = get_offset_with_default(before, list_length)
    after_offset = get_offset_with_default(after, -1)

    start_offset = max(slice_start - 1, after_offset, -1) + 1
    end_offset = min(slice_end, before_offset, list_length)
    if isinstance(first, int):
        end_offset = min(end_offset, start_offset + first)
    if isinstance(last, int):
        start_offset = max(start_offset, end_offset - last)

    limit = first or last or end_offset

    # If supplied slice is too large, trim it down before mapping over it.
    _slice = query.limit(limit).offset(start_offset)
    edges = []

    for i, node in enumerate(await session.execute(_slice)):
        node = filter_requested_fields_for_object(
            dict(node), connection_type.Edge.node.type
        )
        node = connection_type.Edge.node.type(**node)
        edge = edge_type(node, cursor=offset_to_cursor(start_offset + i))
        edges.append(edge)

    first_edge_cursor = edges[0].cursor if edges else None
    last_edge_cursor = edges[-1].cursor if edges else None
    lower_bound = after_offset + 1 if after else 0
    upper_bound = before_offset if before else list_length

    return connection_type(
        edges=edges,
        page_info=page_info_type(
            startCursor=first_edge_cursor,
            endCursor=last_edge_cursor,
            hasPreviousPage=isinstance(last, int) and start_offset > lower_bound,
            hasNextPage=isinstance(first, int)
            and end_offset < upper_bound
            and edges != [],
        ),
    )
