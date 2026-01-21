from __future__ import annotations

from typing import Dict

_REGISTRY: Dict[str, Dict[str, str]] = {}


def register_query(key: str, sql: str, description: str) -> str:
    if key in _REGISTRY and _REGISTRY[key]["sql"] != sql:
        raise ValueError(f"Query key '{key}' already registered with different SQL.")
    _REGISTRY[key] = {"sql": sql, "description": description}
    return sql


def get_registry() -> Dict[str, Dict[str, str]]:
    return dict(_REGISTRY)
