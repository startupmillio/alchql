from base64 import b64encode
from inspect import isawaitable

from graphene import Context
from graphql import GraphQLResolveInfo

from .TraceTreeBuilder import TraceTreeBuilder
from ..extension import Extension


class InlineTraceExtension(Extension):
    def __init__(self):
        self.should_trace = True
        self.tree_builder = TraceTreeBuilder()

    async def resolve(self, next_, parent, info: GraphQLResolveInfo, **kwargs):
        ftv1_header = not info.context.request.headers.get(
            "apollo-federation-include-trace", None
        )
        if ftv1_header:
            self.should_trace = False
        if parent is None and self.should_trace:
            self.tree_builder.start_timing()

        if self.should_trace and not self.tree_builder.stopped:

            end_node_trace = self.tree_builder.will_resolve_field(info)
        else:
            end_node_trace = lambda: None

        try:
            result = next_(parent, info, **kwargs)
            if isawaitable(result):
                result = await result
            return result
        finally:
            end_node_trace()

    def has_errors(self, errors, context):
        self.tree_builder.did_encounter_errors(errors, context)

    def format(self, context: Context):
        if self.should_trace:
            self.tree_builder.stop_timing()
            self.tree_builder.add_nodes_to_trace()

            ftv1 = str(b64encode(self.tree_builder.trace.SerializeToString()), "utf8")

            return {"ftv1": ftv1}
