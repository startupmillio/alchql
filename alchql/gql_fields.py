"""
MIT License

Copyright (c) 2018 Mitchel Cabuloy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import re
import sqlalchemy
from typing import Union

from graphql import FieldNode, FragmentDefinitionNode, GraphQLResolveInfo

from alchql.registry import get_global_registry

_camel_to_snake_re = re.compile(
    r"((?!^[^A-Z]*)|\b[a-zA-Z][a-z\d]*)([A-Z]\d*[a-z]*|\d+)"
)


def camel_to_snake(name: str) -> str:
    name = _camel_to_snake_re.sub(r"\1_\2", name)
    return name.lower()


def collect_fields(node, fragments, cls_name: str = None):
    """Recursively collects fields from the AST

    Args:
        node (dict): A node in the AST
        fragments (dict): Fragment definitions

    Returns:
        A dict mapping each field found, along with their sub fields.

        {'name': {},
         'sentimentsPerLanguage': {'id': {},
                                   'name': {},
                                   'totalSentiments': {}},
         'slug': {}}
    """

    field = {}

    if node.get("selection_set"):
        for leaf in node["selection_set"]["selections"]:
            if leaf["kind"] == "field":
                field.update({leaf["name"]["value"]: collect_fields(leaf, fragments)})
            elif leaf["kind"] == "fragment_spread":
                field.update(
                    collect_fields(fragments[leaf["name"]["value"]], fragments)
                )
            elif (
                leaf["kind"] == "inline_fragment"
                and leaf["type_condition"].name.value == cls_name
            ):
                field.update(collect_fields(leaf, fragments))

    return field


def ast_to_dict(ast: Union[FieldNode, FragmentDefinitionNode]):
    result = {}
    for k in ast.keys:
        if k == "loc":
            continue
        value = getattr(ast, k)
        if k in {"name", "selection_set"}:
            value = ast_to_dict(value) if value else value
        if k == "selections":
            value = [ast_to_dict(i) for i in value]

        result[k] = value

    if hasattr(ast, "kind"):
        result["kind"] = ast.kind

    return result


def get_tree(info: GraphQLResolveInfo, cls_name: str = None):
    """A convenience function to call collect_fields with info

    Args:
        info (ResolveInfo)

    Returns:
        dict: Returned from collect_fields
    """

    fragments = {}
    node = ast_to_dict(info.field_nodes[0])

    for name, value in info.fragments.items():
        fragments[name] = ast_to_dict(value)

    return collect_fields(node, fragments, cls_name)


def get_fields(model, info: GraphQLResolveInfo, cls_name=None):
    tree = get_tree(info, cls_name)

    if "edges" in tree:
        tree = tree["edges"]
        if "node" in tree:
            tree = tree["node"]

    fields = []
    for key in tree.keys():
        if key == "__typename":
            continue

        model_names = {key, camel_to_snake(key)}
        for model_name in model_names:
            if hasattr(model, model_name):
                ex = getattr(model, model_name).expression
                break
        else:
            registry = get_global_registry()
            type_ = registry.get_type_for_model(model)
            for k, v in type_._meta.fields.items():
                if getattr(v, "name", None) in model_names:
                    ex = getattr(type_, k).model_field.expression
                    if k != ex.key:
                        ex = ex.label(k)
                    break
            else:
                raise Exception(f"Field not found: {key}")

        if hasattr(ex, "left") and hasattr(ex, "right"):
            if model.__table__ == ex.left.table:
                fields.append(ex.left)
            elif model.__table__ == ex.right.table:
                fields.append(ex.right)
        else:
            fields.append(ex)

    for pk in sqlalchemy.inspect(model).primary_key:
        fields.append(pk)
    fields = list(set(fields))

    return fields
