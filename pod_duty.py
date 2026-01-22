from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Iterable

from models import PodDutyAssignment
from schedule_service import WEEKDAY_TO_DAY_CODE

POD_GRADES = (6, 6, 7, 7, 10, 10, 11, 11, 12, 12)


class PodDutyManager:
    def __init__(
        self,
        schedule_manager: Any,
        session_factory: Callable | None = None,
        covers_manager: Any | None = None,
        excluded_slugs_source: Callable | Iterable[str] | None = None,
    ) -> None:
        self.schedule_manager = schedule_manager
        self._session_factory = session_factory
        self.covers_manager = covers_manager
        self._excluded_slugs_source = excluded_slugs_source
        self._cached_assignments: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._pods = self._build_pods()
        self.refresh_dynamic_rows()

    @staticmethod
    def _build_pods() -> list[dict[str, Any]]:
        pods: list[dict[str, Any]] = []
        for index, grade in enumerate(POD_GRADES, start=1):
            pods.append({"label": f"Pod {index}", "grade": grade, "key": str(index)})
        return pods

    @property
    def pods(self) -> list[dict[str, Any]]:
        return list(self._pods)

    @property
    def excluded_slugs(self) -> set[str]:
        source = self._excluded_slugs_source
        if callable(source):
            return set(source() or [])
        if source is None:
            return set()
        return set(source)

    def refresh_dynamic_rows(self) -> None:
        if not self._session_factory:
            return
        assignments: list[dict[str, Any]] = []
        with self._session_factory() as session:
            rows = session.query(PodDutyAssignment).all()
        for row in rows:
            date_iso = row.assignment_date.isoformat() if row.assignment_date else None
            assignments.append(
                {
                    "assignment_date": date_iso,
                    "day_code": row.day_code or "",
                    "period_label": row.period_label or "",
                    "period_raw": row.period_label or "",
                    "pod_label": row.pod_label or "",
                    "teacher": row.teacher_name,
                    "teacher_name": row.teacher_name,
                    "teacher_email": row.teacher_email,
                    "details": f"Pod duty {row.pod_label or ''}".strip(),
                }
            )
        self.schedule_manager.rebuild_pod_duty_assignments(assignments)

    def _parse_assignment_date(self, value: date | str | None) -> date | None:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
        return None

    def _normalize_period(self, value: str | None) -> str | None:
        if not value:
            return None
        return self.schedule_manager.normalize_period(value or "")

    def _assignment_key(self, assignment_date: date, period_label: str) -> tuple[str, str]:
        return (assignment_date.isoformat(), period_label)

    def _day_code_for_date(self, assignment_date: date) -> str | None:
        return WEEKDAY_TO_DAY_CODE.get(assignment_date.weekday())

    def list_assignments(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> dict[str, dict[str, Any]]:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period or not self._session_factory:
            return {}
        with self._session_factory() as session:
            rows = (
                session.query(PodDutyAssignment)
                .filter(
                    PodDutyAssignment.assignment_date == parsed,
                    PodDutyAssignment.period_label == period,
                )
                .order_by(PodDutyAssignment.pod_label)
                .all()
            )
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            label = row.pod_label or ""
            result[label] = {
                "pod_label": label,
                "teacher_name": row.teacher_name,
                "teacher_email": row.teacher_email,
                "teacher_slug": row.teacher_slug,
            }
        return result

    def assignments_for_period(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> list[dict[str, Any]]:
        return list(self.list_assignments(assignment_date, period_label).values())

    def available_teachers(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> list[dict[str, Any]]:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return []
        day_code = self._day_code_for_date(parsed)
        if not day_code:
            return []
        payload = self.schedule_manager.available_for_slot_api(
            day_code, period, assignment_date=parsed
        )
        return payload.get("available", [])

    def allowed_slugs_by_pod(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> dict[str, set[str]]:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return {}
        excluded = self.excluded_slugs
        available = self.available_teachers(parsed, period)
        absentees = self._absent_slugs(parsed)
        pool = sorted(
            {
                teacher.get("slug")
                for teacher in available
                if teacher.get("slug")
                and teacher.get("slug") not in excluded
                and teacher.get("slug") not in absentees
            }
        )
        allowed: dict[str, set[str]] = {}
        for pod in self._pods:
            allowed[pod["label"]] = set(pool)
        return allowed

    def _absent_slugs(self, assignment_date: date | None) -> set[str]:
        if not assignment_date or not self.covers_manager:
            return set()
        records = self.covers_manager.get_absences_for_date(assignment_date.isoformat())
        slugs = {
            record.get("teacher_slug")
            for record in records
            if record.get("teacher_slug")
        }
        return {slug for slug in slugs if slug}

    def cache_assignments(
        self,
        assignment_date: date | str | None,
        period_label: str,
        assignments: list[dict[str, Any]],
    ) -> None:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return
        key = self._assignment_key(parsed, period)
        self._cached_assignments[key] = [assignment.copy() for assignment in assignments]

    def get_cached_assignments(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> list[dict[str, Any]]:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return []
        key = self._assignment_key(parsed, period)
        return [assignment.copy() for assignment in self._cached_assignments.get(key, [])]

    def clear_cached_assignments(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> None:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return
        key = self._assignment_key(parsed, period)
        self._cached_assignments.pop(key, None)

    def _build_record(
        self,
        slug: str,
        assignment_date: date,
        period_label: str,
        pod_label: str,
        meta: dict[str, Any],
    ) -> PodDutyAssignment:
        day_code = self._day_code_for_date(assignment_date)
        return PodDutyAssignment(
            assignment_date=assignment_date,
            day_code=day_code,
            period_label=period_label,
            pod_label=pod_label,
            teacher_name=meta.get("name"),
            teacher_email=meta.get("email"),
            teacher_slug=slug,
            created_at=datetime.utcnow(),
        )

    def _replace_assignments(
        self,
        assignment_date: date,
        period_label: str,
        assignments: list[PodDutyAssignment],
        pods_to_replace: set[str] | None = None,
    ) -> int:
        if not self._session_factory:
            return len(assignments)
        with self._session_factory() as session:
            query = (
                session.query(PodDutyAssignment)
                .filter(
                    PodDutyAssignment.assignment_date == assignment_date,
                    PodDutyAssignment.period_label == period_label,
                )
            )
            if pods_to_replace:
                query = query.filter(PodDutyAssignment.pod_label.in_(pods_to_replace))
            query.delete(synchronize_session=False)
            if assignments:
                session.bulk_save_objects(assignments)
            session.commit()
        self.refresh_dynamic_rows()
        return len(assignments)

    def _auto_assign_candidates(
        self,
        assignment_date: date | str | None,
        period_label: str,
        target_pods: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str], set[str]]:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return [], ["Invalid period or date."], set()
        allowed = self.allowed_slugs_by_pod(parsed, period)
        selection: list[dict[str, Any]] = []
        errors: list[str] = []
        pods_to_assign = [
            pod for pod in self._pods if not target_pods or pod["label"] in target_pods
        ]
        used: set[str] = set()
        excluded = self.excluded_slugs
        absent = self._absent_slugs(parsed)
        for pod in pods_to_assign:
            slugs = sorted(allowed.get(pod["label"], []))
            slug = next(
                (
                    candidate
                    for candidate in slugs
                    if candidate not in used and candidate not in excluded and candidate not in absent
                ),
                None,
            )
            if not slug:
                errors.append(f"No available teacher for {pod['label']}.")
                continue
            teacher = self.schedule_manager.get_teacher(slug) or {}
            selection.append(
                {
                    "pod_label": pod["label"],
                    "teacher_slug": slug,
                    "teacher_name": teacher.get("name") or slug,
                    "teacher_email": teacher.get("email"),
                }
            )
            used.add(slug)
        return selection, errors, {item["pod_label"] for item in selection}

    def plan_auto_assign(
        self,
        assignment_date: date | str | None,
        period_label: str,
        target_pods: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        assignments, errors, _ = self._auto_assign_candidates(
            assignment_date, period_label, target_pods=target_pods
        )
        return assignments, errors

    def auto_assign(
        self,
        assignment_date: date | str | None,
        period_label: str,
        target_pods: list[str] | None = None,
        persist: bool = True,
    ) -> tuple[int, list[str]]:
        assignments, errors, pods_to_replace = self._auto_assign_candidates(
            assignment_date, period_label, target_pods=target_pods
        )
        if not persist:
            return len(assignments), errors
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return 0, errors
        records: list[PodDutyAssignment] = []
        for assignment in assignments:
            slug = assignment.get("teacher_slug")
            if not slug:
                continue
            meta = self.schedule_manager.get_teacher(slug) or {}
            records.append(
                self._build_record(
                    slug,
                    parsed,
                    period,
                    assignment.get("pod_label", ""),
                    meta,
                )
            )
        count = self._replace_assignments(parsed, period, records, pods_to_replace)
        return count, errors

    def save_assignments(
        self,
        assignment_date: date | str | None,
        period_label: str,
        selections: dict[str, str],
    ) -> tuple[bool, list[str]]:
        parsed = self._parse_assignment_date(assignment_date)
        period = self._normalize_period(period_label)
        if not parsed or not period:
            return False, ["Invalid date or period."]
        used: set[str] = set()
        assignments: list[dict[str, Any]] = []
        errors: list[str] = []
        for pod in self._pods:
            label = pod["label"]
            slug = (selections.get(label) or "").strip()
            if not slug:
                continue
            if slug in used:
                errors.append(f"{slug} is selected for multiple pods.")
                continue
            teacher = self.schedule_manager.get_teacher(slug)
            if not teacher:
                errors.append(f"Teacher not found for pod {label}.")
                continue
            assignments.append(
                {
                    "pod_label": label,
                    "teacher_slug": slug,
                    "teacher_name": teacher.get("name"),
                    "teacher_email": teacher.get("email"),
                }
            )
            used.add(slug)
        if errors:
            return False, errors
        records = [
            self._build_record(
                assignment["teacher_slug"],
                parsed,
                period,
                assignment["pod_label"],
                assignment,
            )
            for assignment in assignments
        ]
        self._replace_assignments(parsed, period, records)
        self.clear_cached_assignments(parsed, period)
        return True, []

    def assignments_to_notify(
        self,
        assignments: list[dict[str, Any]],
        force: bool = False,
    ) -> list[dict[str, Any]]:
        return assignments

    def record_notifications(self, entries: list[tuple[dict[str, Any], str]]) -> None:
        return
