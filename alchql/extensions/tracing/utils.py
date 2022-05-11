import json

from graphql import GraphQLError

from .generated import reports_pb2 as Trace


def hr_timestamp_to_nanos(timestamp):
    return (timestamp.seconds * (10**9)) + timestamp.nanos


def response_path_as_string(p=None):
    if p is None:
        return ""

    res = p.key
    p = p.prev
    while p is not None:
        res = str(p.key) + "." + str(res)
        p = p.prev
    return res


def create_location_message(loc):
    location = Trace.Trace().Location()
    location.line = loc[0]
    location.column = loc[1]
    return location


def encode_graphql_error(error):
    if isinstance(error, GraphQLError):
        return {
            "message": error.message,
            "nodes": error.nodes,
            "source": error.source,
            "positions": error.positions,
            "locations": error.locations,
            "path": error.path,
            "original_error": error.original_error,
            "extensions": error.extensions,
        }


def error_to_protobuf_error(error):
    trace_error = Trace.Trace().Error()
    trace_error.message = error.message
    if len(error.locations):
        for loc in error.locations:
            location_message = create_location_message(loc)
            trace_error.location.append(location_message)
    trace_error.json = json.dumps(error, default=encode_graphql_error)
    return trace_error
