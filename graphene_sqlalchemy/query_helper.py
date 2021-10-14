import base64
from dataclasses import dataclass

import graphene
import sqlalchemy as sa
from typing import Optional, List

from graphene import Dynamic, Field, Scalar
from graphql import FieldNode, ListValueNode, VariableNode

from graphene_sqlalchemy.gql_fields import camel_to_snake
from graphene_sqlalchemy.utils import filter_value_to_python, FilterItem, GlobalFilters

RESERVED_NAMES = ["edges", "node"]
FRAGMENT = "fragment_spread"


@dataclass
class QueryField:
    name: str
    values: Optional[list]
    arguments: Optional[dict]


@dataclass
class FragmentField:
    name: str


class QueryHelper:
    @staticmethod
    def get_filters(info) -> list:
        object_types = getattr(info.context, "object_types", {})
        object_type = object_types.get(info.field_name)
        if not object_type:
            return []

        if not hasattr(object_type, "parsed_filters"):
            return []

        filters_to_apply = []

        parsed_filters = object_type.parsed_filters

        gql_field = QueryHelper.get_current_field(info)

        if gql_field and gql_field.arguments:
            for name, value in gql_field.arguments.items():
                if name not in parsed_filters:
                    continue

                filter_item: FilterItem
                filter_item = parsed_filters[name]
                if not filter_item.filter_func:
                    continue

                if hasattr(filter_item.field_type, "parse_value"):
                    value = filter_item.field_type.parse_value(value)

                if filter_item.field_type == graphene.ID:
                    decoded = base64.b64decode(value).decode()
                    value = int(decoded.split(":")[1])

                if filter_item.field_type == graphene.List and filter_item.field_type.of_type == graphene.ID:
                    new_value = []
                    for item in value:
                        decoded = base64.b64decode(item).decode()
                        new_value.append(int(decoded.split(":")[1]))
                    value = new_value

                field_expr = parsed_filters[name].filter_func(value)
                filters_to_apply.append(field_expr)
        return filters_to_apply

    @staticmethod
    def parse_query(info) -> List[QueryField]:

        if not hasattr(
            info.context, "parsed_query"
        ) or not info.context.parsed_query.get(info.field_name):

            nodes = info.field_nodes

            variables = info.variable_values
            result = QueryHelper.__parse_nodes(nodes, variables)
            fragments = QueryHelper.__parse_fragments(info.fragments, variables)

            result = QueryHelper.__set_fragment_fields(result, fragments)
            setattr(info.context, "parsed_query", {info.field_name: result})

        return info.context.parsed_query[info.field_name].copy()

    @staticmethod
    def get_selected_fields(info, model):
        object_types = getattr(info.context, "object_types", {})
        object_type = object_types.get(info.field_name)

        if not object_type:
            return

        gql_field = QueryHelper.get_current_field(info)

        object_type_fields = {}

        for _name in dir(object_type):
            if _name.startswith("_"):
                continue

            attr = getattr(object_type, _name, None)
            if attr and isinstance(attr, (Field, Scalar)):
                if hasattr(attr, "kwargs") and attr.kwargs.get("name"):
                    object_type_fields[attr.kwargs.get("name")] = attr
                else:
                    object_type_fields[_name] = attr

        type_ = info.context.object_types[info.field_name]
        meta_fields = type_._meta.fields
        select_fields = {sa.inspect(model).primary_key[0]}
        for field in gql_field.values:
            current_field = object_type_fields.get(field.name, None) or meta_fields.get(
                field.name
            )
            if isinstance(current_field, Dynamic) and isinstance(
                current_field.type(), Field
            ):
                try:
                    columns = getattr(
                        object_type._meta.model, field.name
                    ).prop.local_columns
                    relation_key = next(iter(columns))
                    select_fields.add(relation_key)
                except Exception as _:
                    pass
            model_field = getattr(current_field, "model_field", None)
            if model_field is not None:
                if getattr(current_field, "use_label", True):
                    select_fields.add(model_field.label(field.name))
                else:
                    select_fields.add(model_field)
        return select_fields

    @staticmethod
    def get_current_field(info) -> Optional[QueryField]:
        gql_fields = QueryHelper.parse_query(info)
        field_name = camel_to_snake(info.field_name)
        current_gql_field = None

        while gql_fields:
            next_field = gql_fields.pop()
            if next_field.name == field_name:
                return next_field

            if next_field.values:
                gql_fields.extend(next_field.values)

        return current_gql_field

    @staticmethod
    def has_last_arg(info):
        gql_field = QueryHelper.get_current_field(info)
        if gql_field and gql_field.arguments:
            for name, value in gql_field.arguments.items():
                if name == "last":
                    return True

        return False

    @staticmethod
    def __parse_nodes(nodes, variables) -> list:
        values = []
        node: FieldNode
        for node in nodes:
            name = camel_to_snake(node.name.value)
            node_values = None
            if node.kind == FRAGMENT:
                values.append(FragmentField(name=name))
                continue

            if node.selection_set:
                node_values = QueryHelper.__parse_nodes(
                    node.selection_set.selections, variables
                )

            arguments = {}
            if node.arguments:
                for arg in node.arguments:
                    if isinstance(arg.value, ListValueNode):
                        value = []
                        for arg_value in arg.value.values:
                            value.append(arg_value.value)

                    elif isinstance(arg.value, VariableNode):
                        value = variables.get(arg.value.name.value)
                    else:
                        value = arg.value.value

                    arguments[camel_to_snake(arg.name.value)] = filter_value_to_python(
                        value
                    )

            if name in RESERVED_NAMES:
                values.extend(node_values)
            else:
                values.append(
                    QueryField(name=name, values=node_values, arguments=arguments)
                )

        return values

    @staticmethod
    def __parse_fragments(fragments: dict, variables: dict) -> dict:
        result = {}
        for name, fragment in fragments.items():
            result[camel_to_snake(name)] = QueryHelper.__parse_nodes(
                fragment.selection_set.selections, variables
            )

        return result

    @staticmethod
    def __set_fragment_fields(parsed_query, fragments) -> list:
        new_values = []
        for field in parsed_query:
            if isinstance(field, FragmentField):
                extra_fields = fragments.get(field.name)
                for extra_field in extra_fields:
                    if extra_field.values:
                        extra_field.values = QueryHelper.__set_fragment_fields(
                            parsed_query=extra_field.values, fragments=fragments
                        )
                new_values.extend(extra_fields)
            else:
                if field.values:
                    field.values = QueryHelper.__set_fragment_fields(
                        parsed_query=field.values, fragments=fragments
                    )

                new_values.append(field)
        return new_values
