from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from covers_service import CoversManager
from schedule_service import ScheduleManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSIGNMENTS_FILE = os.path.join(BASE_DIR, "cover_assignments.json")
DAY_CODE_BY_WEEKDAY = {0: "Mo", 1: "Tu", 2: "We", 3: "Th", 4: "Fr"}

logger = logging.getLogger(__name__)


class CoverAssignmentManager:
    def __init__(
        self,
        schedule_manager: ScheduleManager,
        covers_manager: CoversManager,
        storage_path: Optional[str] = None,
    ):
        self.schedule_manager = schedule_manager
        self.covers_manager = covers_manager
        self.storage_path = storage_path or ASSIGNMENTS_FILE
        self.assignments: dict[str, list[dict[str, Any]]] = self._load_assignments()

    def _load_assignments(self) -> dict[str, list[dict[str, Any]]]:
        if not os.path.exists(self.storage_path):
            return {}
        try:
            with open(self.storage_path, encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
        except (OSError, json.JSONDecodeError):
            pass
        return {}

    def _save_assignments(self) -> None:
        directory = os.path.dirname(self.storage_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        with open(self.storage_path, "w", encoding="utf-8") as handle:
            json.dump(self.assignments, handle, indent=2)

    def assign_for_record(self, record: Dict[str, Any]) -> None:
        absent_email = record.get("teacher_email")
        if not absent_email:
            return
        status = str(record.get("status") or "").strip().lower()
        if status != "approved":
            return
        try:
            start_date = date.fromisoformat(record["leave_start"])
            end_date = date.fromisoformat(record["leave_end"])
        except Exception:
            logger.warning("Invalid leave dates for %s", record.get("request_id"))
            return
        current = start_date
        while current <= end_date:
            weekday = current.weekday()
            if weekday >= 5:
                current += timedelta(days=1)
                continue
            date_key = current.isoformat()
            if self._already_assigned(record["teacher_email"], date_key):
                current += timedelta(days=1)
                continue
            cover = self._select_cover(record, date_key, weekday)
            if cover:
                self._store_assignment(date_key, record, cover)
            current += timedelta(days=1)

    def _already_assigned(self, absent_email: str, date_key: str) -> bool:
        return any(
            assignment["absent_email"].lower() == absent_email.lower()
            for assignment in self.assignments.get(date_key, [])
        )

    def _select_cover(self, record: Dict[str, Any], date_key: str, weekday: int) -> Optional[dict]:
        day_code = DAY_CODE_BY_WEEKDAY.get(weekday)
        if not day_code:
            return None
        absent_emails = {
            entry.get("teacher_email", "").lower()
            for entry in self.covers_manager.get_absences_for_date(date_key)
        }
        assigned_cover_emails = {
            assignment["cover_email"].lower()
            for assignment in self.assignments.get(date_key, [])
        }
        target_subject = str(record.get("subject") or "").lower()
        target_cycles = self._cycles_from_label(record.get("level_label"))
        candidates: List[dict[str, Any]] = []
        for teacher in self.schedule_manager.teacher_cards:
            email = (teacher.get("email") or "").strip().lower()
            if not email or email == record["teacher_email"].lower():
                continue
            if email in absent_emails or email in assigned_cover_emails:
                continue
            if target_subject and teacher.get("subject", "").strip().lower() != target_subject:
                continue
            teacher_cycles = self._cycles_from_label(teacher.get("level_label"))
            if not target_cycles & teacher_cycles:
                continue
            day_summary = self.schedule_manager.day_summary_for_teacher(teacher["slug"], day_code)
            if day_summary["free_periods"] <= 0:
                continue
            candidates.append({"meta": teacher, "day": day_summary})
        if not candidates:
            return None
        candidates.sort(
            key=lambda candidate: (
                candidate["day"]["scheduled_count"],
                -candidate["day"]["free_periods"],
            )
        )
        return candidates[0]

    def _store_assignment(
        self, date_key: str, record: Dict[str, Any], cover: dict[str, Any]
    ) -> None:
        assignment = {
            "date": date_key,
            "absent_teacher": record["teacher"],
            "absent_email": record["teacher_email"],
            "cover_teacher": cover["meta"]["name"],
            "cover_email": cover["meta"]["email"],
            "subject": record.get("subject"),
            "status": record.get("status"),
            "leave_type": record.get("leave_type"),
            "leave_start": record.get("leave_start"),
            "leave_end": record.get("leave_end"),
            "submitted_at": record.get("submitted_at"),
            "cover_free_periods": cover["day"]["free_periods"],
            "cover_scheduled": cover["day"]["scheduled_count"],
            "cover_max_periods": cover["day"]["max_periods"],
            "cover_assigned_at": datetime.utcnow().isoformat(),
            "day_label": cover["day"]["label"],
        }
        self.assignments.setdefault(date_key, []).append(assignment)
        self._save_assignments()

    def _cycles_from_label(self, label: Optional[str]) -> set[str]:
        if not label:
            return {"general"}
        normalized = label.lower()
        result = set()
        if "high" in normalized:
            result.add("high")
        if "middle" in normalized:
            result.add("middle")
        if not result:
            result.add("general")
        return result

    def get_assignments(self) -> dict[str, list[dict[str, Any]]]:
        return self.assignments.copy()
