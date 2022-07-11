import logging
from typing import Optional, TYPE_CHECKING, Type

import sqlalchemy as sa
from graphene import Connection, PageInfo
from graphene.types import ResolveInfo
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeMeta

from .utils import (
    get_offset_with_default,
    offset_to_cursor,
)
from ..query_helper import QueryHelper
from ..utils import filter_requested_fields_for_object

if TYPE_CHECKING:
    from ..fields import UnsortedSQLAlchemyConnectionField
DEFAULT_LIMIT = 1000


def get_count_query(query, model):
    only_q = query.with_only_columns(
        *sa.inspect(model).primary_key,
    ).order_by(None)
    return sa.select([sa.func.count()]).select_from(only_q.alias())


def construct_page_info(
    cls: Type[PageInfo],
    info: ResolveInfo,
    edges: list,
    limit: int,
    offset: int,
) -> PageInfo:
    page_info_kwargs = {}

    page_info_fields = QueryHelper.get_page_info_fields(info)

    if not page_info_fields:
        return cls()

    for field in page_info_fields:
        if field == "has_previous_page":
            page_info_kwargs[field] = offset > 0
        elif field == "has_next_page":
            page_info_kwargs[field] = limit and len(edges) > limit
        elif edges:
            if field == "start_cursor":
                page_info_kwargs[field] = edges[0].cursor
            elif field == "end_cursor":
                page_info_kwargs[field] = edges[:limit][-1].cursor

    return cls(**page_info_kwargs)


async def connection_from_query(
    cls: Type["UnsortedSQLAlchemyConnectionField"],
    model: Type[DeclarativeMeta],
    info: ResolveInfo,
    args: Optional[dict] = None,
    connection_type: Type[Connection] = Connection,
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
    session: AsyncSession = info.context.session

    # has_last_arg = QueryHelper.has_arg(info, "last")
    # if not QueryHelper.get_filters(info) and has_last_arg:
    #     raise TypeError('Cannot set "last" without filters applied')

    edge_type = connection_type.Edge
    node_type = edge_type.node.type
    page_info_type = getattr(connection_type, "PageInfo", PageInfo)

    before = args.get("before")
    after = args.get("after")
    first = args.get("first")
    last = args.get("last")

    current_field = QueryHelper.get_current_field(info)
    has_total_count = current_field and "total_count" in {
        i.name for i in current_field.values
    }

    if (before, after, first, last) == (None, None, None, None):
        first = DEFAULT_LIMIT
        logging.warning(f"Query without border, {first=}")

    query = await cls.get_query(
        model=model,
        info=info,
        cls_name=node_type.__name__,
        **args,
    )

    total_count = None
    # TODO: Move total_count to PageInfo
    if (last and not before) or (after and not first and not before) or has_total_count:
        count_query = get_count_query(query, model)
        right_offset = total_count = (await session.execute(count_query)).scalar()
    else:
        right_offset = get_offset_with_default(before)

    left_offset = get_offset_with_default(after) + 1 if after else 0

    if isinstance(first, int):
        right_offset = min(left_offset + first, right_offset)
    if isinstance(last, int):
        left_offset = max(left_offset, right_offset - last)

    _slice = query
    # If supplied slice is too large, trim it down before mapping over it.
    limit = first or last or (right_offset - left_offset)

    if limit:
        _slice = _slice.limit(limit + 1)
    if left_offset:
        _slice = _slice.offset(left_offset)

    edges = []

    for i, v in enumerate(await session.execute(_slice)):
        node_value = filter_requested_fields_for_object(dict(v), node_type)
        edge = edge_type(
            node=node_type(**node_value),
            cursor=offset_to_cursor(left_offset + i),
        )
        edges.append(edge)

    connection = connection_type(
        edges=edges[:limit],
        page_info=construct_page_info(
            cls=page_info_type,
            info=info,
            edges=edges,
            limit=limit,
            offset=left_offset,
        ),
    )

    if has_total_count:
        connection.total_count = total_count

    return connection
