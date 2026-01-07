from __future__ import annotations

import json
import os
from typing import Dict, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(BASE_DIR, "assignment_settings.json")

DEFAULT_ASSIGNMENT_SETTINGS: Dict[str, int] = {
    "max_covers_default": 2,
    "max_covers_high": 2,
    "max_covers_high_friday": 3,
    "max_covers_middle": 2,
    "max_covers_middle_friday": 2,
    "highschool_full_threshold": 5,
    "middleschool_full_threshold": 4,
}


def _as_int(value: Optional[str | int], fallback: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return fallback


class AssignmentSettingsManager:
    def __init__(self, storage_path: Optional[str] = None):
        self.storage_path = storage_path or SETTINGS_FILE
        self._settings = DEFAULT_ASSIGNMENT_SETTINGS.copy()
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.storage_path):
            return
        try:
            with open(self.storage_path, encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                merged = DEFAULT_ASSIGNMENT_SETTINGS.copy()
                for key, value in data.items():
                    if key in merged:
                        merged[key] = _as_int(value, merged[key])
                self._settings = merged
        except (OSError, json.JSONDecodeError):
            pass

    def _save(self) -> None:
        directory = os.path.dirname(self.storage_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        with open(self.storage_path, "w", encoding="utf-8") as handle:
            json.dump(self._settings, handle, indent=2)

    def update(self, overrides: Dict[str, int]) -> None:
        updated = False
        for key, value in overrides.items():
            if key not in self._settings:
                continue
            if value <= 0:
                continue
            if self._settings[key] != value:
                self._settings[key] = value
                updated = True
        if updated:
            self._save()

    def to_dict(self) -> Dict[str, int]:
        return self._settings.copy()

    def get(self, key: str) -> int:
        return self._settings.get(key, DEFAULT_ASSIGNMENT_SETTINGS.get(key, 1))

    @property
    def max_covers_default(self) -> int:
        return self.get("max_covers_default")

    @property
    def max_covers_high(self) -> int:
        return self.get("max_covers_high")

    @property
    def max_covers_high_friday(self) -> int:
        return self.get("max_covers_high_friday")

    @property
    def max_covers_middle(self) -> int:
        return self.get("max_covers_middle")

    @property
    def max_covers_middle_friday(self) -> int:
        return self.get("max_covers_middle_friday")

    @property
    def highschool_full_threshold(self) -> int:
        return self.get("highschool_full_threshold")

    @property
    def middleschool_full_threshold(self) -> int:
        return self.get("middleschool_full_threshold")
