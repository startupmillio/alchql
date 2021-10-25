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
from typing import Union

from graphql import FieldNode, FragmentDefinitionNode, GraphQLResolveInfo


def camel_to_snake(name: str) -> str:
    name = re.sub("(.)([0-9]+[A-Z]*[a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def collect_fields(node, fragments):
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


def get_tree(info: GraphQLResolveInfo):
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

    return collect_fields(node, fragments)


def get_fields(model, info):
    tree = get_tree(info)

    if "edges" in tree:
        tree = tree["edges"]
        if "node" in tree:
            tree = tree["node"]

    fields = []
    for k in tree.keys():
        if hasattr(model, k):
            ex = getattr(model, k).expression
        else:
            ex = getattr(model, camel_to_snake(k)).expression

        if hasattr(ex, "left") and hasattr(ex, "right"):
            if model.__table__ == ex.left.table:
                fields.append(ex.left)
            elif model.__table__ == ex.right.table:
                fields.append(ex.right)
        else:
            fields.append(ex)

    return fields
