"""Decode payment JSON before any binary floating-point conversion of money."""

import json
import math
from decimal import Decimal, DecimalException


class InvalidJsonPayload(ValueError):
    """The request cannot be represented by the API/PostgreSQL JSON contract."""


def _text(value: str) -> str:
    if "\x00" in value:
        raise InvalidJsonPayload("JSON strings cannot contain NUL")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise InvalidJsonPayload("JSON strings must contain valid Unicode") from None
    return value


def _constant(value: str) -> None:
    raise InvalidJsonPayload("JSON numbers must be finite")


def _normalize(value: object, *, money: bool = False) -> object:
    if isinstance(value, str):
        return _text(value)
    if isinstance(value, Decimal):
        if money:
            return value
        # Metadata keeps the existing JSON int/float contract; only money is Decimal.
        number = float(value)
        if not math.isfinite(number):
            raise InvalidJsonPayload("JSON numbers must be finite")
        return number
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {_text(key): _normalize(item) for key, item in value.items()}
    return value


def decode_payment_json(raw: bytes) -> dict[str, object]:
    """Retain exact amount literals and reject PostgreSQL-incompatible strings."""
    try:
        value = json.loads(raw.decode("utf-8"), parse_float=Decimal, parse_constant=_constant)
        if not isinstance(value, dict):
            raise InvalidJsonPayload("The request body must be a JSON object")
        return {_text(key): _normalize(item, money=key == "amount") for key, item in value.items()}
    except InvalidJsonPayload:
        raise
    except (ValueError, DecimalException, RecursionError, OverflowError):
        raise InvalidJsonPayload("Invalid JSON request body") from None
