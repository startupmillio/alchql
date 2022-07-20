import base64
import json
from typing import NamedTuple, Type, TypeVar, Union

_T = TypeVar("_T")


class ResolvedGlobalId(NamedTuple):
    type: str
    id: Union[str, int]

    def encode(self) -> str:
        text = f"{self.type}:{json.dumps(self.id)}"
        return base64.b64encode(text.encode()).decode()

    @classmethod
    def decode(cls: Type[_T], encoded_id: str) -> _T:
        if not encoded_id:
            raise ValueError("Empty ID")

        try:
            text = base64.b64decode(encoded_id).decode()
        except Exception as e:
            raise ValueError("Invalid base64")

        type_name, id_text = text.split(":")

        return cls(
            type=type_name,
            id=json.loads(id_text),
        )

    def __str__(self):
        return self.encode()
