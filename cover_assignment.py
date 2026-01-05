from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from covers_service import CoversManager
from schedule_service import ScheduleManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSIGNMENTS_FILE = os.path.join(BASE_DIR, "cover_assignments.json")
DAY_CODE_BY_WEEKDAY = {0: "Mo", 1: "Tu", 2: "We", 3: "Th", 4: "Fr"}

CYCLE_HIGH = "HighSchool"
CYCLE_MIDDLE = "MiddleSchool"
CYCLE_GENERAL = "General"

ALLOWED_EDIT_FIELDS = {
    "status",
    "cover_teacher",
    "cover_email",
    "cover_subject",
    "class_subject",
    "class_grade",
    "class_details",
    "period_label",
    "period_raw",
    "class_time",
}

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
        absent_slug = record.get("teacher_slug")
        if not absent_slug:
            return
        try:
            start_date = date.fromisoformat(record["leave_start"])
            end_date = date.fromisoformat(record["leave_end"])
        except Exception:
            logger.warning("Invalid leave dates for %s", record.get("request_id"))
            return
        target_cycles = self._cycles_from_label(record.get("level_label"))
        record_subject = str(record.get("subject") or "").strip()
        absent_email_normalized = absent_email.strip().lower()
        current = start_date
        while current <= end_date:
            weekday = current.weekday()
            if weekday >= 5:
                current += timedelta(days=1)
                continue
            day_code = DAY_CODE_BY_WEEKDAY.get(weekday)
            if not day_code:
                current += timedelta(days=1)
                continue
            date_key = current.isoformat()
            details = self._details_for_teacher_on_day(absent_slug, day_code)
            if not details:
                logger.warning(
                    "No schedule data for %s on %s, skipping period-based cover assignment",
                    record.get("teacher"),
                    date_key,
                )
                current += timedelta(days=1)
                continue
            is_friday = day_code == "Fr"
            hs_max_slots = 5 if is_friday else 7
            absent_emails = {
                str(entry.get("teacher_email") or "").strip().lower()
                for entry in self.covers_manager.get_absences_for_date(date_key)
            }
            session_covers_log: dict[str, int] = {}
            for detail in details:
                cover = self._select_cover_for_detail(
                    date_key,
                    day_code,
                    detail,
                    target_cycles,
                    record_subject,
                    absent_email_normalized,
                    absent_emails,
                    session_covers_log,
                    hs_max_slots,
                )
                if not cover:
                    logger.warning(
                        "Unable to find cover for %s on %s (%s)",
                        record.get("teacher"),
                        date_key,
                        detail.get("period_label") or detail.get("period_raw"),
                    )
                    continue
                slug = cover["meta"].get("slug")
                if slug:
                    session_covers_log[slug] = session_covers_log.get(slug, 0) + 1
                self._store_assignment(date_key, record, detail, cover)
            current += timedelta(days=1)

    def _select_cover_for_detail(
        self,
        date_key: str,
        day_code: str,
        detail: dict[str, Any],
        target_cycles: set[str],
        record_subject: str,
        absent_email: str,
        absent_emails: Set[str],
        session_covers_log: dict[str, int],
        hs_max_slots: int,
    ) -> Optional[dict[str, Any]]:
        period_label_raw = detail.get("period_label") or detail.get("period_raw") or ""
        period_lookup = self.schedule_manager.normalize_period(period_label_raw) or period_label_raw
        available_slugs: Optional[Set[str]] = None
        if period_lookup:
            available_slugs = {
                teacher["slug"]
                for teacher in self.schedule_manager.teachers_available(day_code, period_lookup)
            }
        target_subject = detail.get("subject") or record_subject
        normalized_target_subject = self._normalize_subject(target_subject)
        candidates: List[dict[str, Any]] = []
        for teacher in self.schedule_manager.teacher_cards:
            slug = teacher.get("slug")
            email = str(teacher.get("email") or "").strip().lower()
            if not slug or not email or email == absent_email:
                continue
            if email in absent_emails:
                continue
            if available_slugs is not None and slug not in available_slugs:
                continue
            day_summary = self.schedule_manager.day_summary_for_teacher(slug, day_code)
            if day_summary["free_periods"] <= 0:
                continue
            teacher_cycles = self._cycles_from_label(teacher.get("level_label"))
            database_covers = self._covers_for_teacher_on_date(date_key, slug)
            runtime_covers = session_covers_log.get(slug, 0)
            total_covers = database_covers + runtime_covers
            if total_covers >= 2:
                continue
            if CYCLE_HIGH in teacher_cycles:
                occupied_slots = day_summary["scheduled_count"] + total_covers
                if (occupied_slots + 1) >= hs_max_slots:
                    continue
            teacher_subject_normalized = self._normalize_subject(teacher.get("subject"))
            match_subject = (
                bool(normalized_target_subject)
                and teacher_subject_normalized == normalized_target_subject
            )
            cycle_overlap = bool(target_cycles & teacher_cycles)
            tier = self._priority_tier(match_subject, cycle_overlap)
            candidates.append(
                {
                    "meta": teacher,
                    "day": day_summary,
                    "priority": (
                        tier,
                        self._as_int(teacher.get("course_total")),
                        teacher.get("name") or "",
                    ),
                }
            )
        if not candidates:
            return None
        candidates.sort(key=lambda candidate: candidate["priority"])
        return candidates[0]

    def _priority_tier(self, match_subject: bool, cycle_overlap: bool) -> int:
        if match_subject and cycle_overlap:
            return 1
        if match_subject:
            return 2
        if cycle_overlap:
            return 3
        return 4

    def _details_for_teacher_on_day(self, slug: Optional[str], day_code: str) -> list[dict[str, Any]]:
        if not slug:
            return []
        schedule = self.schedule_manager.get_schedule_for_teacher(slug)
        if not schedule:
            return []
        for day in schedule["schedule"]:
            if day.get("code") != day_code:
                continue
            details: list[dict[str, Any]] = []
            for section in day.get("sections") or []:
                period_label = section.get("period")
                period_time = section.get("time")
                for entry in section.get("details") or []:
                    details.append(
                        {
                            "period_label": period_label,
                            "period_raw": entry.get("period_raw"),
                            "subject": entry.get("subject"),
                            "grade": entry.get("grade"),
                            "details": entry.get("details"),
                            "time": period_time,
                        }
                    )
            return details
        return []

    def _normalize_subject(self, subject: Optional[str]) -> str:
        if not subject:
            return ""
        return str(subject).strip().lower()

    def _store_assignment(
        self,
        date_key: str,
        record: Dict[str, Any],
        detail: dict[str, Any],
        cover: dict[str, Any],
    ) -> None:
        class_subject = detail.get("subject") or record.get("subject") or "General"
        assignment = {
            "date": date_key,
            "absent_teacher": record["teacher"],
            "absent_email": record["teacher_email"],
            "cover_teacher": cover["meta"]["name"],
            "cover_email": cover["meta"]["email"],
            "cover_slug": cover["meta"].get("slug"),
            "subject": record.get("subject"),
            "class_subject": class_subject,
            "class_grade": detail.get("grade"),
            "class_details": detail.get("details"),
            "period_label": detail.get("period_label"),
            "period_raw": detail.get("period_raw"),
            "class_time": detail.get("time"),
            "cover_subject": cover["meta"].get("subject"),
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

    def update_assignment(
        self, date_key: str, index: int, updates: dict[str, Any]
    ) -> bool:
        rows = self.assignments.get(date_key)
        if not rows or not (0 <= index < len(rows)):
            return False
        entry = rows[index]
        for key in ALLOWED_EDIT_FIELDS:
            if key in updates:
                entry[key] = updates[key]
        new_cover_slug = updates.get("cover_slug")
        if new_cover_slug:
            entry["cover_slug"] = new_cover_slug
            teacher = self.schedule_manager.get_teacher(new_cover_slug)
            if teacher:
                entry["cover_teacher"] = teacher.get("name", entry.get("cover_teacher"))
                entry["cover_email"] = teacher.get("email", entry.get("cover_email"))
                entry["cover_subject"] = teacher.get("subject", entry.get("cover_subject"))
                day_code = self._day_code_from_date(date_key)
                if day_code:
                    day_summary = self.schedule_manager.day_summary_for_teacher(new_cover_slug, day_code)
                    entry["cover_free_periods"] = day_summary["free_periods"]
                    entry["cover_scheduled"] = day_summary["scheduled_count"]
                    entry["cover_max_periods"] = day_summary["max_periods"]
        self._save_assignments()
        return True

    def _covers_for_teacher_on_date(self, date_key: str, slug: str) -> int:
        return sum(
            1
            for assignment in self.assignments.get(date_key, [])
            if assignment.get("cover_slug") == slug
        )

    def _day_code_from_date(self, date_key: str) -> Optional[str]:
        try:
            parsed = datetime.fromisoformat(date_key)
        except ValueError:
            return None
        return DAY_CODE_BY_WEEKDAY.get(parsed.weekday())

    @staticmethod
    def _as_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return 0

    def _cycles_from_label(self, label: Optional[str]) -> set[str]:
        if not label:
            return {CYCLE_GENERAL}
        normalized = label.lower()
        result: set[str] = set()
        if "high" in normalized:
            result.add(CYCLE_HIGH)
        if "middle" in normalized:
            result.add(CYCLE_MIDDLE)
        if not result:
            result.add(CYCLE_GENERAL)
        return result

    def get_assignments(self) -> dict[str, list[dict[str, Any]]]:
        return self.assignments.copy()
