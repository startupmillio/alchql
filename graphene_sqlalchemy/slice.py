from graphql_relay.connection.arrayconnection import get_offset_with_default, offset_to_cursor

from .sa_version import __sa_version__


def connection_from_query(query, model, session, args=None, connection_type=None,
                               edge_type=None, pageinfo_type=None,
                               slice_start=0, list_length=0, list_slice_length=None):
    """
    Given a slice (subset) of an array, returns a connection object for use in
    GraphQL.
    This function is similar to `connectionFromArray`, but is intended for use
    cases where you know the cardinality of the connection, consider it too large
    to materialize the entire array, and instead wish pass in a slice of the
    total result large enough to cover the range specified in `args`.
    """
    args = args or {}

    before = args.get('before')
    after = args.get('after')
    first = args.get('first')
    last = args.get('last')

    slice_end = slice_start + list_slice_length
    before_offset = get_offset_with_default(before, list_length)
    after_offset = get_offset_with_default(after, -1)

    start_offset = max(
        slice_start - 1,
        after_offset,
        -1
    ) + 1
    end_offset = min(
        slice_end,
        before_offset,
        list_length
    )
    if isinstance(first, int):
        end_offset = min(
            end_offset,
            start_offset + first
        )
    if isinstance(last, int):
        start_offset = max(
            start_offset,
            end_offset - last
        )

    # If supplied slice is too large, trim it down before mapping over it.
    _slice = query.limit(end_offset).offset(start_offset)
    if __sa_version__ > (1, 4):
        edges = [
            edge_type(
                node=node[0],
                cursor=offset_to_cursor(start_offset + i)
            )
            for i, node in enumerate(session.execute(_slice))
        ]
    else:
        edges = [
            edge_type(
                node=model(**node),
                cursor=offset_to_cursor(start_offset + i)
            )
            for i, node in enumerate(session.execute(_slice))
        ]

    first_edge_cursor = edges[0].cursor if edges else None
    last_edge_cursor = edges[-1].cursor if edges else None
    lower_bound = after_offset + 1 if after else 0
    upper_bound = before_offset if before else list_length

    return connection_type(
        edges=edges,
        page_info=pageinfo_type(
            start_cursor=first_edge_cursor,
            end_cursor=last_edge_cursor,
            has_previous_page=isinstance(last, int) and start_offset > lower_bound,
            has_next_page=isinstance(first, int) and end_offset < upper_bound
        )
    )