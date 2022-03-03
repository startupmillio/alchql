import time

from graphql import GraphQLResolveInfo

from .generated.reports_pb2 import Trace
from .utils import (
    error_to_protobuf_error,
    hr_timestamp_to_nanos,
    response_path_as_string,
)


class TraceTreeBuilder:
    def __init__(self):
        self.trace = Trace()
        self.root_node = self.trace.Node()
        self.start_hr_time = None
        self.stopped = False
        root_response_path = response_path_as_string()
        self.nodes = {root_response_path: self.root_node}

    def start_timing(self):
        if self.start_hr_time:
            raise Exception("start_timing called twice!")
        if self.stopped:
            raise Exception("start_timing called after stop_timing!")
        self.trace.start_time.GetCurrentTime()
        self.start_hr_time = self.trace.start_time

    def stop_timing(self):
        if not self.start_hr_time:
            raise Exception("stop_timing called before start_timing!")
        if self.stopped:
            raise Exception("stop_timing called twice!")
        self.trace.end_time.GetCurrentTime()
        self.trace.duration_ns = hr_timestamp_to_nanos(
            self.trace.end_time
        ) - hr_timestamp_to_nanos(self.trace.start_time)
        self.stopped = True

    def will_resolve_field(self, info: GraphQLResolveInfo):
        if not self.start_hr_time:
            raise Exception("will_resolve_field called before start_timing!")
        if self.stopped:
            raise Exception("will_resolve_field called after stop_timing!")
        path = info.path
        node = self.new_node(path)
        node.type = str(info.return_type)
        node.parent_type = str(info.parent_type)
        trace_start_time = hr_timestamp_to_nanos(self.trace.start_time)
        node.start_time = time.time_ns() - trace_start_time
        if type(path.key) == str and path.key != info.field_name:
            # This field was aliased; send the original field name too (for FieldStats).
            node.original_field_name = info.field_name

        def end_node_trace():
            node.end_time = time.time_ns() - trace_start_time

        return end_node_trace

    def did_encounter_errors(self, errors, context):
        for error in errors:
            # @TODO Add more handling here like Apollo's `TraceTreeBuilder`?
            # However, the service name isn't available in the error extensions
            # See: https://github.com/apollographql/apollo-server/blob/b7a91df76acef748488eedcfe998917173cff142/packages/apollo-server-core/src/plugin/traceTreeBuilder.ts#L79
            # @TODO Add support for rewriting errors
            # See: https://github.com/apollographql/apollo-server/blob/b7a91df76acef748488eedcfe998917173cff142/packages/apollo-server-core/src/plugin/traceTreeBuilder.ts#L95
            self.add_protobuf_error(error.path, error_to_protobuf_error(error))

    def new_node(self, path):
        parent_node = self.ensure_parent_node(path)
        node = parent_node.child.add()
        id_ = path.key
        if isinstance(id_, int):
            node.index = id_
        else:
            node.response_name = id_
        response_path = response_path_as_string(path)
        self.nodes[response_path] = node
        return node

    def ensure_parent_node(self, path):
        parent_path = response_path_as_string(path.prev)
        parent_node = self.nodes.get(parent_path, None)
        if parent_node:
            return parent_node

        # Because we set up the root path when creating self.nodes, we now know
        # that path.prev isn't undefined.
        return self.new_node(path.prev)

    def add_protobuf_error(self, path, error):
        if not self.start_hr_time:
            raise Exception("add_protobuf_error called before start_timing!")
        if self.stopped:
            raise Exception("add_protobuf_error called after stop_timing!")
        # By default, put errors on the root node.
        node = self.root_node
        # If a non-GraphQLError Error sneaks in here somehow with a non-array
        # path, don't crash.
        if isinstance(path, list):
            node_key = ".".join(path)
            specified_node = self.nodes.get(node_key, None)
            if specified_node:
                node = specified_node
            else:
                print(
                    "Could not find node with path "
                    + node_key
                    + "; defaulting to put errors on root node."
                )
        node.error.append(error)

    def add_nodes_to_trace(self):
        self.trace.root.CopyFrom(self.root_node)
