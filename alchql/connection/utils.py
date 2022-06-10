import binascii
from base64 import b64decode, b64encode
from typing import Optional

PREFIX = "arrayconnection:"


def base64(s: str) -> str:
    """Encode the string s using Base64."""
    b: bytes = s.encode("utf-8") if isinstance(s, str) else s
    return b64encode(b).decode("ascii")


def unbase64(s: str) -> str:
    """Decode the string s using Base64."""
    try:
        b: bytes = s.encode("ascii") if isinstance(s, str) else s
    except UnicodeEncodeError:
        return ""
    try:
        return b64decode(b).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return ""


def offset_to_cursor(offset: int) -> str:
    """Create the cursor string from an offset."""
    return base64(f"{PREFIX}{offset}")


def cursor_to_offset(cursor: str) -> Optional[int]:
    """Extract the offset from the cursor string."""
    try:
        return int(unbase64(cursor)[len(PREFIX) :])
    except ValueError:
        return None


def get_offset_with_default(
    cursor: Optional[str] = None, default_offset: int = 0
) -> int:
    """Get offset from a given cursor and a default.

    Given an optional cursor and a default offset, return the offset to use;
    if the cursor contains a valid offset, that will be used,
    otherwise it will be the default.
    """
    if not isinstance(cursor, str):
        return default_offset

    offset = cursor_to_offset(cursor)
    return default_offset if offset is None else offset
