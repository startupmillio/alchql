from inspect import isawaitable


def to_std_dicts(value):
    """Convert nested ordered dicts to normal dicts for better comparison."""
    if isinstance(value, dict):
        return {k: to_std_dicts(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [to_std_dicts(v) for v in value]
    else:
        return value


# class SessionMiddleware:
#     def __init__(self, session):
#         self.session = session
#
#     async def resolve(self, next_, root, info, **args):
#         context = info.context
#
#         if callable(self.session):
#             context.session = self.session()
#         else:
#             context.session = self.session
#
#         result = next_(root, info, **args)
#         if isawaitable(result):
#             result = await result
#
#         return result
