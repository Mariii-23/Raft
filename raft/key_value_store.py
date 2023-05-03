from typing import Any

class KeyValueStore:
    _store: dict[Any, Any]

    def __init__(self) -> None:
        self._store = {}

    def write(self, key: Any, value: Any) -> None:
        self._store[key] = value

    def read(self, key: Any) -> Any | None:
        return self._store.get(key)
