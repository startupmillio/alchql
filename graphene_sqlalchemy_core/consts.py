from typing import Callable, Dict, Tuple

OP_LTE = "lte"
OP_EQ = "eq"
OP_GTE = "gte"
OP_ILIKE = "ilike"
OP_IN = "in"
OP_EMPTY = "empty"
OP_CONTAINS = "contains"
OP_LT = "lt"
OP_GT = "gt"

OPERATORS_MAPPING: Dict[str, Tuple[str, Callable]] = {
    OP_LTE: ("__le__", lambda v: v),
    OP_EQ: ("__eq__", lambda v: v),
    OP_GTE: ("__ge__", lambda v: v),
    OP_ILIKE: ("ilike", lambda v: f"%{v}%"),
    OP_IN: ("in_", lambda v: v),
    OP_EMPTY: ("is_", lambda v: None),
    OP_CONTAINS: ("contains", lambda v: v),
    OP_LT: ("__lt__", lambda v: v),
    OP_GT: ("__gt__", lambda v: v),
}
