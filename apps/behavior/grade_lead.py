"""Grade lead pod-duty management routes (self-contained)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import json
import requests
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    render_template_string,
    request,
    session,
    url_for,
)

from auth import login_required
from behaviour import Teacher
try:
    from behaviour import TeacherRole
except Exception:
    TeacherRole = None
from duty_admin import DailyDutyAssignment, DailyDutyAcknowledgement, DUTY_TYPES
from extensions import db
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for older interpreters
    ZoneInfo = None  # type: ignore


grade_lead_bp = Blueprint("grade_lead_bp", __name__, url_prefix="/grade-lead")

GRADE_CODES: Tuple[str, ...] = ("6", "7", "10", "11", "12")
GRADE_PERIODS: Dict[str, int] = {"6": 6, "7": 6, "10": 7, "11": 7, "12": 7}
BREAK_POD_KEY = "GRADE_BREAK"
BREAK_LABEL = "Grade Break Duty"
BREAK_LOCATION_CHOICES: Tuple[Tuple[str, str], ...] = (
    ("bathroom", "Bathroom"),
    ("canteen_gate", "Canteen Gate"),
    ("outside_area", "Outside Area"),
    ("canteen_door", "Canteen Door"),
    ("store", "Store"),
    ("end_canteen", "End Canteen"),
    ("shop", "Shop"),
)
BREAK_LOCATIONS = {key: label for key, label in BREAK_LOCATION_CHOICES}
BREAK_LOCATION_OPTIONS = [{"value": key, "label": label} for key, label in BREAK_LOCATION_CHOICES]
BREAK_LOCATION_GRADES = {"6", "7", "10"}
BREAK_LOCATION_REVERSE = {label: key for key, label in BREAK_LOCATION_CHOICES}
STATUS_LABELS = {
    "pending": "Pending",
    "present": "Checked in",
    "unavailable": "Unavailable",
}
STATUS_BADGES = {
    "pending": "bg-slate-200 text-slate-700",
    "present": "bg-emerald-100 text-emerald-700",
    "unavailable": "bg-amber-100 text-amber-700",
}

EXCLUDED_ROLES_FROM_ROSTERS: Set[str] = {"administrator"}
BREAK_ONLY_EXCLUDED_ROLES: Set[str] = {"slt"}

def _ensure_daily_tables() -> None:
    DailyDutyAssignment.__table__.create(bind=db.engine, checkfirst=True)
    DailyDutyAcknowledgement.__table__.create(bind=db.engine, checkfirst=True)

UAE_TZ = ZoneInfo("Asia/Dubai") if ZoneInfo else None
RESET_TIME = time(hour=15, minute=0)  # 3 PM


class GradeLeadDutyAssignment(db.Model):
    __tablename__ = "grade_lead_duty_assignments"
    __bind_key__ = "teachers_bind"

    id = db.Column(db.Integer, primary_key=True)
    assignment_date = db.Column(db.Date, nullable=False, index=True)
    grade = db.Column(db.String(10), nullable=False, index=True)
    pod = db.Column(db.String(20), nullable=False)
    slot_type = db.Column(db.String(10), nullable=False)  # 'period' or 'break'
    period = db.Column(db.Integer, nullable=True)
    teacher_id = db.Column(db.Integer, db.ForeignKey("teachers.id"), nullable=False, index=True)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    break_location = db.Column(db.String(50), nullable=True)

    teacher = db.relationship("Teacher", foreign_keys=[teacher_id])
    acknowledgement = db.relationship(
        "GradeLeadDutyAcknowledgement",
        back_populates="assignment",
        uselist=False,
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        db.UniqueConstraint(
            "assignment_date",
            "teacher_id",
            "slot_type",
            "period",
            name="uq_grade_lead_teacher_slot",
        ),
    )


class GradeLeadDutyAcknowledgement(db.Model):
    __tablename__ = "grade_lead_duty_acknowledgements"
    __bind_key__ = "teachers_bind"

    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey("grade_lead_duty_assignments.id"), unique=True, nullable=False)
    teacher_id = db.Column(db.Integer, nullable=False, index=True)
    status = db.Column(db.String(20), nullable=False, default="pending")  # pending, present, unavailable
    note = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    assignment = db.relationship("GradeLeadDutyAssignment", back_populates="acknowledgement")


def _ensure_tables() -> None:
    GradeLeadDutyAssignment.__table__.create(bind=db.engine, checkfirst=True)
    GradeLeadDutyAcknowledgement.__table__.create(bind=db.engine, checkfirst=True)

    inspector = inspect(db.engine)
    dialect = db.engine.dialect.name

    try:
        columns = {column["name"] for column in inspector.get_columns("grade_lead_duty_assignments")}
    except SQLAlchemyError:
        columns = set()

    if "break_location" not in columns:
        statement = "ALTER TABLE grade_lead_duty_assignments ADD COLUMN break_location VARCHAR(50)"
        if dialect == "sqlite":
            statement = "ALTER TABLE grade_lead_duty_assignments ADD COLUMN break_location TEXT"
        with db.engine.begin() as connection:
            try:
                connection.execute(text(statement))
            except SQLAlchemyError:
                pass

    try:
        unique_constraints = inspector.get_unique_constraints("grade_lead_duty_assignments")
    except SQLAlchemyError:
        unique_constraints = []
    unique_names = {uc.get("name") for uc in unique_constraints if uc.get("name")}

    try:
        indexes = inspector.get_indexes("grade_lead_duty_assignments")
    except SQLAlchemyError:
        indexes = []
    index_names = {idx.get("name") for idx in indexes if idx.get("name")}

    needs_day_unique_drop = (
        "uq_grade_lead_teacher_day" in unique_names or "uq_grade_lead_teacher_day" in index_names
    )

    if needs_day_unique_drop or dialect == "sqlite":
        with db.engine.begin() as connection:
            try:
                if dialect == "sqlite":
                    pragma_indexes = connection.execute(
                        text("PRAGMA index_list('grade_lead_duty_assignments')")
                    ).fetchall()
                    has_autoindex = any(
                        row[1] == "sqlite_autoindex_grade_lead_duty_assignments_1" for row in pragma_indexes
                    )
                else:
                    has_autoindex = False
            except SQLAlchemyError:
                has_autoindex = False

            try:
                if dialect == "sqlite" and has_autoindex:
                    connection.execute(text("PRAGMA foreign_keys=OFF"))
                    connection.execute(
                        text("ALTER TABLE grade_lead_duty_assignments RENAME TO grade_lead_duty_assignments_old")
                    )
                    GradeLeadDutyAssignment.__table__.create(bind=connection, checkfirst=False)

                    try:
                        pragma_cols = connection.execute(
                            text("PRAGMA table_info('grade_lead_duty_assignments_old')")
                        ).fetchall()
                        old_columns = [row[1] for row in pragma_cols]
                    except SQLAlchemyError:
                        old_columns = []

                    target_columns = [
                        "id",
                        "assignment_date",
                        "grade",
                        "pod",
                        "slot_type",
                        "period",
                        "teacher_id",
                        "created_by_teacher_id",
                        "created_at",
                        "break_location",
                    ]
                    copy_columns = [col for col in target_columns if col in old_columns]
                    column_csv = ", ".join(copy_columns)
                    connection.execute(
                        text(
                            f"INSERT INTO grade_lead_duty_assignments ({column_csv}) "
                            f"SELECT {column_csv} FROM grade_lead_duty_assignments_old"
                        )
                    )
                    connection.execute(text("DROP TABLE grade_lead_duty_assignments_old"))
                    connection.execute(text("PRAGMA foreign_keys=ON"))
                elif needs_day_unique_drop:
                    if dialect == "sqlite":
                        connection.execute(text("DROP INDEX IF EXISTS uq_grade_lead_teacher_day"))
                    else:
                        connection.execute(
                            text("ALTER TABLE grade_lead_duty_assignments DROP CONSTRAINT uq_grade_lead_teacher_day")
                        )
            except SQLAlchemyError:
                pass
        unique_names.discard("uq_grade_lead_teacher_day")
        index_names.discard("uq_grade_lead_teacher_day")

    if "uq_grade_lead_teacher_slot" not in unique_names:
        with db.engine.begin() as connection:
            try:
                if dialect == "sqlite":
                    connection.execute(
                        text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS uq_grade_lead_teacher_slot "
                            "ON grade_lead_duty_assignments (assignment_date, teacher_id, slot_type, period)"
                        )
                    )
                else:
                    connection.execute(
                        text(
                            "ALTER TABLE grade_lead_duty_assignments "
                            "ADD CONSTRAINT uq_grade_lead_teacher_slot "
                            "UNIQUE (assignment_date, teacher_id, slot_type, period)"
                        )
                    )
            except SQLAlchemyError:
                pass


def _current_grade_lead_grade() -> Optional[str]:
    role = session.get("role") or ""
    if role.startswith("grade_lead_"):
        return role.rsplit("_", 1)[-1]
    return None


def _ensure_grade_lead_access() -> bool:
    if session.get("is_admin"):
        return True
    if _current_grade_lead_grade():
        return True
    flash("Grade Lead access required.", "error")
    return False


def _uae_now() -> datetime:
    if UAE_TZ:
        return datetime.now(UAE_TZ)
    # Fallback to UTC+4 if zoneinfo unavailable
    return datetime.utcnow() + timedelta(hours=4)


def _assignment_date_for_now(now: Optional[datetime] = None) -> date:
    now = now or _uae_now()
    if now.time() >= RESET_TIME:
        return (now + timedelta(days=1)).date()
    return now.date()


def _week_start(target: Optional[date]) -> date:
    base = target or _assignment_date_for_now()
    return base - timedelta(days=base.weekday())


def _week_dates(week_start: date) -> List[date]:
    return [week_start + timedelta(days=offset) for offset in range(5)]


_AVAILABILITY_API_URL = "http://coveralreef.pythonanywhere.com/api/check-availability"
_DAY_CODE_MAP: Dict[str, str] = {
    "Monday": "Mo",
    "Tuesday": "Tu",
    "Wednesday": "We",
    "Thursday": "Th",
    "Friday": "Fr",
}


def _day_code_for_date(target: date) -> str:
    return _DAY_CODE_MAP.get(target.strftime("%A"), target.strftime("%a")[:2])


def _fetch_availability_records(day_code: str, period: str) -> List[Dict[str, object]]:
    if not day_code or not period:
        return []
    try:
        response = requests.get(
            _AVAILABILITY_API_URL,
            params={"day": day_code, "period": period},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload.get("available") or []
        return []
    except Exception as exc:
        current_app.logger.warning("Availability API failed (%s %s): %s", day_code, period, exc)
        return []


def _dedupe_by_email(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
    seen: Dict[str, Dict[str, object]] = {}
    for record in records:
        email = (record.get("email") or "").strip().lower()
        if email and email not in seen:
            seen[email] = record
    return list(seen.values())


def _build_availability_options(
    records: List[Dict[str, object]],
    email_map: Dict[str, Dict[str, object]],
    assignment_summary: Dict[int, str],
) -> List[Dict[str, object]]:
    options: List[Dict[str, object]] = []
    seen: Set[int] = set()
    for record in records:
        email = (record.get("email") or "").strip().lower()
        base = email_map.get(email)
        if not base:
            continue
        teacher_id = base["id"]
        if teacher_id in seen:
            continue
        seen.add(teacher_id)
        option = base.copy()
        summary = assignment_summary.get(teacher_id)
        info = summary if summary and summary != "Available" else record.get("level_label") or base.get("info") or "Available"
        option["info"] = info
        options.append(option)
    return options


def _level_label_matches_grade(grade: str, level_label: Optional[str]) -> bool:
    if not level_label:
        return False
    label = level_label.lower()
    if grade in {"6", "7"}:
        return "middle school" in label
    return "high school" in label


def _latest_assignment_date_for_grade(grade: str) -> Optional[date]:
    try:
        row = (
            db.session.query(GradeLeadDutyAssignment.assignment_date)
            .filter(GradeLeadDutyAssignment.grade == grade)
            .order_by(GradeLeadDutyAssignment.assignment_date.desc())
            .limit(1)
            .first()
        )
    except SQLAlchemyError:
        return None
    return row[0] if row else None


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _pods_for_grade(grade: str) -> List[str]:
    return [f"G{grade} Pod 1", f"G{grade} Pod 2"]


@dataclass
class Slot:
    slot_type: str  # 'period' or 'break'
    label: str
    period: Optional[int]
    assignments: List[GradeLeadDutyAssignment]


def _slot_key(slot_type: str, pod: str, period: Optional[int]) -> str:
    period_part = "" if period is None else str(period)
    return f"{slot_type}|{pod}|{period_part}"


def _build_slot_structure(
    grade: str,
    assignments: Iterable[GradeLeadDutyAssignment],
) -> Tuple[List[Dict[str, object]], Slot]:
    assignments_map: Dict[Tuple[str, str, Optional[int]], List[GradeLeadDutyAssignment]] = {}
    for item in assignments:
        key = (item.pod, item.slot_type, item.period)
        assignments_map.setdefault(key, []).append(item)

    pods_payload: List[Dict[str, object]] = []
    periods = list(range(1, GRADE_PERIODS.get(grade, 0) + 1))
    for pod in _pods_for_grade(grade):
        slots: List[Slot] = []
        for period in periods:
            key = (pod, "period", period)
            slots.append(
                Slot(
                    slot_type="period",
                    label=f"Period {period}",
                    period=period,
                    assignments=assignments_map.get(key, []),
                )
            )
        pods_payload.append({"name": pod, "slots": slots})

    break_assignments = assignments_map.get((BREAK_POD_KEY, "break", None), [])
    break_slot = Slot(
        slot_type="break",
        label=BREAK_LABEL,
        period=None,
        assignments=break_assignments,
    )
    return pods_payload, break_slot


def _assignment_label(assignment: GradeLeadDutyAssignment) -> str:
    if assignment.slot_type == "break":
        base = f"Grade {assignment.grade} Break Duty"
        if assignment.grade in BREAK_LOCATION_GRADES and getattr(assignment, "break_location", None):
            return f"{base} ({assignment.break_location})"
        return base
    period_label = f"P{assignment.period}" if assignment.period is not None else ""
    return f"{assignment.pod} {period_label}".strip()


def _teacher_display_label(teacher: Teacher) -> str:
    email = (teacher.email or "").strip()
    if email:
        local_part = email.split("@", 1)[0]
        if local_part:
            return local_part.lower()
    if teacher.name:
        return teacher.name
    if teacher.email:
        return teacher.email
    return "Teacher"


def _normalize_role(role_value: Optional[str]) -> str:
    return (role_value or "").strip().lower()


def _load_teacher_roles() -> Dict[int, str]:
    if TeacherRole is None:
        return {}
    try:
        rows = db.session.query(TeacherRole).all()
    except SQLAlchemyError:
        return {}
    return {row.teacher_id: _normalize_role(row.role) for row in rows if row.teacher_id}


def _role_allowed_for_slot(role: Optional[str], slot_type: Optional[str] = None) -> bool:
    normalized = _normalize_role(role)
    if normalized in EXCLUDED_ROLES_FROM_ROSTERS:
        return False
    if slot_type == "break" and normalized in BREAK_ONLY_EXCLUDED_ROLES:
        return False
    return True


def _filter_options_for_slot(
    options: Iterable[Dict[str, object]], slot_type: Optional[str], role_map: Dict[int, str]
) -> List[Dict[str, object]]:
    filtered: List[Dict[str, object]] = []
    for option in options:
        teacher_id = option.get("id")
        if not isinstance(teacher_id, int):
            continue
        if _role_allowed_for_slot(role_map.get(teacher_id), slot_type):
            filtered.append(option)
    return filtered


def _resolve_acknowledgement(assignment: GradeLeadDutyAssignment) -> Optional[GradeLeadDutyAcknowledgement]:
    ack = getattr(assignment, "ack", None)
    if ack is not None:
        return ack
    return assignment.acknowledgement


def _build_email_payload(
    assignments: Iterable[GradeLeadDutyAssignment],
    grade: str,
    assignment_date: date,
) -> Optional[Dict[str, object]]:
    teacher_map: Dict[int, Dict[str, object]] = {}
    for assignment in assignments:
        teacher = assignment.teacher
        if not teacher or not teacher.email:
            continue
        entry = teacher_map.setdefault(
            teacher.id,
            {"teacher": teacher, "labels": []},
        )
        entry["labels"].append(_assignment_label(assignment))

    if not teacher_map:
        return None

    recipients = sorted(
        {entry["teacher"].email for entry in teacher_map.values()},
        key=lambda value: value.lower(),
    )
    subject = f"Grade {grade} duty assignments – {assignment_date.strftime('%d %b %Y')}"
    body_lines = [
        "Hello team,",
        "",
        f"Here are the duty assignments for Grade {grade} on {assignment_date.strftime('%A, %d %B %Y')}:",
        "",
    ]
    for entry in sorted(teacher_map.values(), key=lambda item: item["teacher"].name.lower()):
        teacher = entry["teacher"]
        labels = ", ".join(sorted(entry["labels"]))
        body_lines.append(f"- {teacher.name}: {labels}")
    body_lines.extend(
        [
            "",
            "Please reach out if you need any adjustments.",
            "",
            "Thank you!",
        ]
    )
    body = "\n".join(body_lines)
    query_parts = [
        f"subject={quote(subject)}",
        f"body={quote(body)}",
    ]
    mailto_link = f"mailto:{','.join(recipients)}?{'&'.join(query_parts)}"

    return {
        "mailto": mailto_link,
        "subject": subject,
        "body": body,
        "recipients": recipients,
    }


@grade_lead_bp.route("/")
@login_required
def dashboard():
    if not _ensure_grade_lead_access():
        return redirect(url_for("behaviour_bp.behaviour_dashboard"))

    _ensure_tables()

    is_admin = session.get("is_admin", False)
    role_grade = _current_grade_lead_grade()

    requested_grade = request.args.get("grade")
    if is_admin and requested_grade in GRADE_CODES:
        grade = requested_grade
    else:
        grade = role_grade or GRADE_CODES[0]

    if grade not in GRADE_CODES:
        flash("Invalid grade selection.", "error")
        return redirect(url_for("behaviour_bp.behaviour_dashboard"))

    requested_date = _parse_date(request.args.get("date"))
    fallback_date = _latest_assignment_date_for_grade(grade)
    focus_date = requested_date or fallback_date or _assignment_date_for_now()
    week_start = _week_start(focus_date)
    week_dates = _week_dates(week_start)
    week_end = week_dates[-1]
    if focus_date < week_start or focus_date > week_end:
        focus_date = week_start

    all_assignments = (
        db.session.query(GradeLeadDutyAssignment)
        .filter(GradeLeadDutyAssignment.assignment_date.in_(week_dates))
        .all()
    )
    role_map = _load_teacher_roles()
    assignments = [
        assignment
        for assignment in all_assignments
        if _role_allowed_for_slot(role_map.get(assignment.teacher_id), assignment.slot_type)
    ]

    assignment_ids = [a.id for a in assignments]
    if assignment_ids:
        ack_rows = (
            db.session.query(GradeLeadDutyAcknowledgement)
            .filter(GradeLeadDutyAcknowledgement.assignment_id.in_(assignment_ids))
            .all()
        )
        ack_map = {ack.assignment_id: ack for ack in ack_rows}
    else:
        ack_map = {}
    for assignment in assignments:
        assignment.ack = ack_map.get(assignment.id)

    day_assignments = [a for a in assignments if a.assignment_date == focus_date]
    grade_assignments = [a for a in day_assignments if a.grade == grade]

    pods_payload, break_slot = _build_slot_structure(grade, grade_assignments)

    teacher_assignments: Dict[int, List[str]] = {}
    for assignment in day_assignments:
        teacher_assignments.setdefault(assignment.teacher_id, []).append(_assignment_label(assignment))

    teachers = db.session.query(Teacher).order_by(Teacher.name).all()
    teacher_options = []
    for teacher in teachers:
        assignments_list = teacher_assignments.get(teacher.id, [])
        email = teacher.email or ""
        display_label = _teacher_display_label(teacher)
        teacher_role = _normalize_role(role_map.get(teacher.id)) or "teacher"
        if teacher_role in EXCLUDED_ROLES_FROM_ROSTERS:
            continue
        teacher_options.append(
            {
                "id": teacher.id,
                "name": teacher.name,
                "email": teacher.email,
                "display_name": display_label,
                "info": ", ".join(assignments_list) if assignments_list else "Available",
                "search": f"{teacher.name.lower()} {email.lower()} {display_label.lower()}",
            }
        )

    teacher_email_map: Dict[str, Dict[str, object]] = {}
    for option in teacher_options:
        email_key = (option.get("email") or "").strip().lower()
        if email_key:
            teacher_email_map[email_key] = option

    assignment_summary_map = {
        teacher_id: ", ".join(entries) if entries else "Available"
        for teacher_id, entries in teacher_assignments.items()
    }

    email_payload = _build_email_payload(grade_assignments, grade, focus_date)

    break_assignments_data: List[Dict[str, object]] = []
    for assignment in break_slot.assignments:
        if not _role_allowed_for_slot(role_map.get(assignment.teacher_id), "break"):
            continue
        ack = assignment.ack or assignment.acknowledgement
        raw_status = (ack.status if ack and ack.status else "pending") if ack else "pending"
        status = raw_status.strip().lower() if isinstance(raw_status, str) else "pending"
        if status not in STATUS_LABELS:
            status = "pending"
        break_assignments_data.append(
            {
                "teacher_id": assignment.teacher_id,
                "teacher_name": assignment.teacher.name,
                "teacher_email": assignment.teacher.email,
                "status_label": STATUS_LABELS.get(status, "Pending"),
                "status_badge": STATUS_BADGES.get(status, "bg-slate-200 text-slate-700"),
                "note": (ack.note or "") if ack else "",
                "location_label": assignment.break_location or "",
                "location_key": BREAK_LOCATION_REVERSE.get((assignment.break_location or "").strip(), ""),
            }
        )

    break_card = {
        "slot_key": _slot_key("break", BREAK_POD_KEY, None),
        "slot_type": "break",
        "pod": BREAK_POD_KEY,
        "period": None,
        "title": break_slot.label,
        "subtitle": "One break slot per grade.",
        "assignments": break_assignments_data,
        "requires_location": grade in BREAK_LOCATION_GRADES,
    }

    pod_cards: List[Dict[str, object]] = []
    flat_slots: List[Dict[str, object]] = [
        {
            "slot_key": break_card["slot_key"],
            "slot_type": break_card["slot_type"],
            "pod": break_card["pod"],
            "period": break_card["period"],
            "label": break_card["title"],
            "assignments": break_card["assignments"],
            "requires_location": break_card["requires_location"],
        }
    ]

    for pod in pods_payload:
        slot_entries: List[Dict[str, object]] = []
        for slot in pod["slots"]:
            slot_assignments: List[Dict[str, object]] = []
            for assignment in slot.assignments:
                ack = assignment.ack or assignment.acknowledgement
                raw_status = (ack.status if ack and ack.status else "pending") if ack else "pending"
                status = raw_status.strip().lower() if isinstance(raw_status, str) else "pending"
                if status not in STATUS_LABELS:
                    status = "pending"
                slot_assignments.append(
                    {
                        "teacher_id": assignment.teacher_id,
                        "teacher_name": assignment.teacher.name,
                        "teacher_email": assignment.teacher.email,
                        "status_label": STATUS_LABELS.get(status, "Pending"),
                        "status_badge": STATUS_BADGES.get(status, "bg-slate-200 text-slate-700"),
                        "note": (ack.note or "") if ack else "",
                    }
                )
            key = _slot_key(slot.slot_type, pod["name"], slot.period)
            slot_entry = {
                "slot_key": key,
                "slot_type": slot.slot_type,
                "pod": pod["name"],
                "period": slot.period,
                "label": slot.label,
                "assignments": slot_assignments,
                "requires_location": False,
            }
            slot_entries.append(slot_entry)
            flat_slots.append(slot_entry)
        pod_cards.append({"pod": pod["name"], "slots": slot_entries})

    day_code = _day_code_for_date(focus_date)
    availability_map: Dict[str, List[int]] = {}
    for slot in flat_slots:
        if slot["slot_type"] != "period" or slot.get("period") is None:
            continue
        records = _fetch_availability_records(day_code, f"P{slot['period']}")
        deduped = _dedupe_by_email(records)
        preferred = [
            record
            for record in deduped
            if _level_label_matches_grade(grade, record.get("level_label"))
        ]
        options = _filter_options_for_slot(
            _build_availability_options(preferred, teacher_email_map, assignment_summary_map),
            slot["slot_type"],
            role_map,
        )
        if not options and deduped:
            options = _filter_options_for_slot(
                _build_availability_options(deduped, teacher_email_map, assignment_summary_map),
                slot["slot_type"],
                role_map,
            )
        if not options:
            options = _filter_options_for_slot(teacher_options, slot["slot_type"], role_map)
        availability_map[slot["slot_key"]] = [option["id"] for option in options]

    teacher_options_json = json.dumps(teacher_options)
    location_options_json = json.dumps(BREAK_LOCATION_OPTIONS)
    slots_json = json.dumps(flat_slots)

    return render_template(
        "grade_lead_dashboard.html",
        grade=grade,
        grade_options=GRADE_CODES,
        is_admin=is_admin,
        week_start=week_start,
        week_end=week_end,
        week_dates=week_dates,
        focus_date=focus_date,
        break_card=break_card,
        pod_cards=pod_cards,
        teacher_options=teacher_options,
        location_options=BREAK_LOCATION_OPTIONS,
        teacher_options_json=teacher_options_json,
        location_options_json=location_options_json,
        slots_json=slots_json,
        availability_map_json=json.dumps(availability_map),
        email_payload=email_payload,
    )






def _validate_grade_access(grade: str) -> bool:
    if grade not in GRADE_CODES:
        flash("Invalid grade supplied.", "error")
        return False
    if session.get("is_admin"):
        return True
    return grade == _current_grade_lead_grade()


def _prepare_assignment_parameters() -> Optional[Dict[str, object]]:
    is_admin = session.get("is_admin", False)
    grade_input = (request.form.get("grade") or "").strip()
    role_grade = _current_grade_lead_grade()
    grade = grade_input if is_admin else (role_grade or grade_input)
    if not grade:
        flash("Grade information is required to create an assignment.", "error")
        return None
    if not _validate_grade_access(grade):
        return None

    assignment_date = _parse_date(request.form.get("assignment_date")) or _assignment_date_for_now()

    slot_type = (request.form.get("slot_type") or "").strip()
    if slot_type not in {"period", "break"}:
        flash("Invalid slot type.", "error")
        return None

    pod = (request.form.get("pod") or "").strip()
    if slot_type == "break":
        pod = BREAK_POD_KEY
    elif pod not in _pods_for_grade(grade):
        flash("Invalid pod selected.", "error")
        return None

    period = None
    if slot_type == "period":
        try:
            period = int(request.form.get("period", ""))
        except ValueError:
            flash("Invalid period supplied.", "error")
            return None
        if period < 1 or period > GRADE_PERIODS.get(grade, 0):
            flash("Period out of range for this grade.", "error")
            return None

    break_location = None
    if slot_type == "break":
        location_value = (request.form.get("break_location") or "").strip().lower()
        if grade in BREAK_LOCATION_GRADES:
            if location_value not in BREAK_LOCATIONS:
                flash("Please choose a break location.", "error")
                return None
            break_location = BREAK_LOCATIONS[location_value]
        else:
            break_location = BREAK_LOCATIONS.get(location_value) if location_value else None

    try:
        teacher_id = int(request.form.get("teacher_id", ""))
    except ValueError:
        flash("Please choose a teacher.", "error")
        return None

    teacher = db.session.get(Teacher, teacher_id)
    if not teacher:
        flash("Teacher not found.", "error")
        return None

    return {
        "grade": grade,
        "assignment_date": assignment_date,
        "pod": pod,
        "slot_type": slot_type,
        "period": period,
        "break_location": break_location,
        "teacher": teacher,
        "teacher_id": teacher_id,
    }


@grade_lead_bp.route("/my-duty", methods=["GET"])
@login_required
def my_duty():
    _ensure_tables()
    _ensure_daily_tables()

    teacher_id = session.get("teacher_id")
    if not teacher_id:
        flash("Teacher access required to view duty assignments.", "error")
        return redirect(url_for("behaviour_bp.behaviour_dashboard"))

    requested_date = _parse_date(request.args.get("date"))
    week_param = _parse_date(request.args.get("week"))
    today = _assignment_date_for_now()
    next_pod_row = (
        db.session.query(GradeLeadDutyAssignment.assignment_date)
        .filter(
            GradeLeadDutyAssignment.teacher_id == teacher_id,
            GradeLeadDutyAssignment.assignment_date >= today,
        )
        .order_by(GradeLeadDutyAssignment.assignment_date.asc())
        .first()
    )
    next_daily_row = (
        db.session.query(DailyDutyAssignment.assignment_date)
        .filter(
            DailyDutyAssignment.teacher_id == teacher_id,
            DailyDutyAssignment.assignment_date >= today,
        )
        .order_by(DailyDutyAssignment.assignment_date.asc())
        .first()
    )
    next_dates = [row[0] for row in (next_pod_row, next_daily_row) if row and row[0]]
    auto_focus_date = min(next_dates) if next_dates else today
    focus_date = requested_date or auto_focus_date
    week_start = _week_start(week_param or focus_date)
    week_dates = _week_dates(week_start)
    week_end = week_dates[-1]
    if focus_date < week_start or focus_date > week_end:
        focus_date = week_start

    assignments = (
        db.session.query(GradeLeadDutyAssignment)
        .filter(
            GradeLeadDutyAssignment.assignment_date.in_(week_dates),
            GradeLeadDutyAssignment.teacher_id == teacher_id,
        )
        .order_by(
            GradeLeadDutyAssignment.assignment_date.asc(),
            GradeLeadDutyAssignment.slot_type.desc(),
            GradeLeadDutyAssignment.period.asc().nullsfirst(),
            GradeLeadDutyAssignment.pod.asc(),
        )
        .all()
    )

    available_grade_dates_rows = (
        db.session.query(GradeLeadDutyAssignment.assignment_date)
        .filter(GradeLeadDutyAssignment.teacher_id == teacher_id)
        .distinct()
        .order_by(GradeLeadDutyAssignment.assignment_date.asc())
        .all()
    )
    available_daily_dates_rows = (
        db.session.query(DailyDutyAssignment.assignment_date)
        .filter(DailyDutyAssignment.teacher_id == teacher_id)
        .distinct()
        .order_by(DailyDutyAssignment.assignment_date.asc())
        .all()
    )
    available_dates_set = {row[0] for row in available_grade_dates_rows if row and row[0]}
    available_dates_set.update(
        row[0] for row in available_daily_dates_rows if row and row[0]
    )
    # Ensure the auto-selected focus date is available for the quick picker
    available_dates_set.add(focus_date)
    available_dates = sorted(available_dates_set)

    daily_duty_rows = (
        db.session.query(DailyDutyAssignment)
        .filter(
            DailyDutyAssignment.assignment_date.in_(week_dates),
            DailyDutyAssignment.teacher_id == teacher_id,
        )
        .order_by(DailyDutyAssignment.assignment_date.asc(), DailyDutyAssignment.duty_type.asc())
        .all()
    )
    daily_duty_rows = [
        duty for duty in daily_duty_rows if _role_allowed_for_slot(role_map.get(duty.teacher_id))
    ]

    daily_duty_ids = [duty.id for duty in daily_duty_rows]
    daily_ack_map: Dict[int, DailyDutyAcknowledgement] = {}
    if daily_duty_ids:
        daily_ack_rows = (
            db.session.query(DailyDutyAcknowledgement)
            .filter(DailyDutyAcknowledgement.assignment_id.in_(daily_duty_ids))
            .all()
        )
        daily_ack_map = {ack.assignment_id: ack for ack in daily_ack_rows}

    daily_duty_map: Dict[date, List[Dict[str, object]]] = {day: [] for day in week_dates}
    for duty in daily_duty_rows:
        ack = daily_ack_map.get(duty.id)
        raw_status = (ack.status if ack and ack.status else "pending") if ack else "pending"
        status = raw_status.strip().lower() if isinstance(raw_status, str) else "pending"
        if status not in STATUS_LABELS:
            status = "pending"
        daily_duty_map.setdefault(duty.assignment_date, []).append(
            {
                "id": duty.id,
                "type": duty.duty_type,
                "label": duty.duty_type.title() + " Duty",
                "location": duty.location or "Unspecified",
                "status": status,
                "status_label": STATUS_LABELS.get(status, "Pending"),
                "status_badge": STATUS_BADGES.get(status, "bg-slate-200 text-slate-700"),
                "note": (ack.note or "") if ack else "",
                "updated_at": ack.updated_at if ack else None,
            }
        )

    assignments_by_date: Dict[date, List[Dict[str, object]]] = {day: [] for day in week_dates}
    for assignment in assignments:
        ack = _resolve_acknowledgement(assignment)
        raw_status = (ack.status if ack and ack.status else "pending") if ack else "pending"
        status = raw_status.strip().lower() if isinstance(raw_status, str) else "pending"
        if status not in STATUS_LABELS:
            status = "pending"
        assignments_by_date.setdefault(assignment.assignment_date, []).append(
            {
                "id": assignment.id,
                "grade": assignment.grade,
                "pod": assignment.pod if assignment.slot_type == "period" else BREAK_LABEL,
                "label": _assignment_label(assignment),
                "slot_type": assignment.slot_type,
                "period": assignment.period,
                "is_break": assignment.slot_type == "break",
                "status": status,
                "status_label": STATUS_LABELS.get(status, "Pending"),
                "status_badge": STATUS_BADGES.get(status, "bg-slate-200 text-slate-700"),
                "note": (ack.note or "") if ack else "",
                "updated_at": ack.updated_at if ack else None,
                "break_location": assignment.break_location if assignment.slot_type == "break" else "",
            }
        )

    weekly_duties: List[Dict[str, object]] = []
    week_payload: List[Dict[str, object]] = []
    for day in week_dates:
        entries = assignments_by_date.get(day, [])
        duties = daily_duty_map.get(day, [])
        if duties:
            for duty in duties:
                weekly_duties.append(
                    {
                        "date": day,
                        "day_label": day.strftime("%A"),
                        "label": duty["label"],
                        "location": duty["location"],
                        "status": duty["status"],
                        "status_label": duty["status_label"],
                        "status_badge": duty["status_badge"],
                        "note": duty["note"],
                    }
                )
        week_payload.append(
            {
                "date": day,
                "iso": day.isoformat(),
                "items": entries,
                "is_focus_day": day == focus_date,
                "daily_duties": duties,
                "day_label": day.strftime("%A"),
            }
        )
    weekly_duties.sort(key=lambda duty: (duty["date"], duty["label"]))

    prev_week = week_start - timedelta(days=7)
    next_week = week_start + timedelta(days=7)
    has_assignments = any(block["items"] for block in week_payload)
    has_daily_duties = bool(weekly_duties)

    template = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>My Pod Duty</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
</head>
<body class="bg-slate-100 min-h-screen">
  <div class="max-w-5xl mx-auto px-4 py-10 space-y-6">
    <header class="flex flex-wrap items-center justify-between gap-4">
      <div>
        <h1 class="text-3xl font-bold text-slate-900">My Pod Duty</h1>
        <p class="text-sm text-slate-600 mt-1">
          Hello <span class="font-semibold text-slate-900">{{ session.get('teacher_name', 'Teacher') }}</span>.
          Here is your duty plan for the selected week.
        </p>
      </div>
      <div class="flex items-center gap-2">
        <a href="{{ url_for('behaviour_bp.behaviour_dashboard') }}" class="px-4 py-2 bg-slate-600 text-white text-sm font-medium rounded-lg hover:bg-slate-700 transition">
          Behaviour Dashboard
        </a>
        {% if session.get('is_grade_lead') or session.get('is_admin') %}
        <a href="{{ url_for('grade_lead_bp.dashboard') }}" class="px-4 py-2 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 transition">
          Grade Lead Hub
        </a>
        {% endif %}
      </div>
    </header>

    {% with messages = get_flashed_messages(with_categories=True) %}
      {% if messages %}
        <div class="space-y-3">
          {% for category, message in messages %}
            {% set styles = {'success': 'bg-emerald-50 border-emerald-200 text-emerald-700',
                             'error': 'bg-red-50 border-red-200 text-red-700',
                             'warning': 'bg-amber-50 border-amber-200 text-amber-700'} %}
            <div class="px-4 py-3 border rounded-lg {{ styles.get(category, 'bg-slate-50 border-slate-200 text-slate-700') }}">
              {{ message }}
            </div>
          {% endfor %}
        </div>
      {% endif %}
    {% endwith %}

    <section class="bg-white border border-slate-200 rounded-xl shadow-sm p-6 space-y-4">
      <div class="text-sm text-slate-700 space-y-1">
        <div>
          <span class="font-semibold text-slate-900">Focus day: {{ focus_date.strftime('%A') }}</span>
        </div>
        <p class="text-xs text-slate-500">This week spans {{ week_start.strftime('%A') }} through {{ week_end.strftime('%A') }}.</p>
        <p class="text-xs text-slate-500">The highlighted day updates whenever your grade lead switches focus.</p>
      </div>
      {% if available_dates %}
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-1">Jump to a day</label>
        <select class="px-3 py-2 border border-slate-300 rounded-lg" onchange="if (this.value) { window.location = '{{ url_for('grade_lead_bp.my_duty') }}?date=' + this.value; }">
          <option value="">Select a day</option>
          {% for dt in available_dates %}
          <option value="{{ dt.isoformat() }}" title="{{ dt.strftime('%d %b %Y') }}" {% if dt == focus_date %}selected{% endif %}>{{ dt.strftime('%A') }}</option>
          {% endfor %}
        </select>
      </div>
      {% endif %}
      <div class="flex flex-wrap items-center gap-2 text-sm">
        <a href="{{ url_for('grade_lead_bp.my_duty', week=prev_week.isoformat()) }}" class="px-3 py-1 bg-slate-100 text-slate-700 rounded-lg hover:bg-slate-200 transition">Previous week</a>
        <a href="{{ url_for('grade_lead_bp.my_duty', week=next_week.isoformat()) }}" class="px-3 py-1 bg-slate-100 text-slate-700 rounded-lg hover:bg-slate-200 transition">Next week</a>
        <a href="{{ url_for('grade_lead_bp.my_duty') }}" class="px-3 py-1 bg-slate-100 text-slate-700 rounded-lg hover:bg-slate-200 transition">Jump to today</a>
      </div>
    </section>

    {% if not has_assignments and not has_daily_duties %}
    <section class="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
      <p class="text-slate-600 text-sm">
        You have no pod duty assignments scheduled for this week. If you believe this is a mistake, please contact your grade lead.
      </p>
    </section>
    {% endif %}

    {% if weekly_duties %}
    <section class="bg-white border border-slate-200 rounded-xl shadow-sm p-6 space-y-4">
      <div>
        <h2 class="text-lg font-semibold text-slate-900">Week at a Glance</h2>
        <p class="text-xs uppercase tracking-wide text-slate-500">Morning and dismissal duties assigned to you this week.</p>
      </div>
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-slate-200 text-sm">
          <thead class="bg-slate-50">
            <tr>
              <th class="px-3 py-2 text-left font-semibold text-slate-600 uppercase tracking-wide">Day</th>
              <th class="px-3 py-2 text-left font-semibold text-slate-600 uppercase tracking-wide">Duty</th>
              <th class="px-3 py-2 text-left font-semibold text-slate-600 uppercase tracking-wide">Location</th>
              <th class="px-3 py-2 text-left font-semibold text-slate-600 uppercase tracking-wide">Status</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-200">
            {% for duty in weekly_duties %}
              <tr>
                <td class="px-3 py-2 text-slate-800">{{ duty.day_label }}</td>
              <td class="px-3 py-2 text-slate-700">{{ duty.label }}</td>
              <td class="px-3 py-2 text-slate-600">{{ duty.location }}</td>
              <td class="px-3 py-2 text-slate-600">
                <span class="inline-flex items-center px-2 py-1 rounded-full text-xs font-semibold uppercase tracking-wide {{ duty.status_badge }}">{{ duty.status_label }}</span>
                {% if duty.note %}<p class="text-xs text-slate-500 mt-1">Note: {{ duty.note }}</p>{% endif %}
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </section>
    {% endif %}

    <div class="space-y-8">
      {% for day in week_payload %}
      {% set border_classes = 'border-emerald-300 ring-1 ring-emerald-200' if day.is_focus_day else 'border-slate-200' %}
      <section class="bg-white border {{ border_classes }} rounded-xl shadow-sm">
        <div class="border-b border-slate-200 px-6 py-4 flex flex-wrap items-center justify-between gap-3">
          <div>
          <h2 class="text-xl font-semibold text-slate-900">{{ day.day_label }}</h2>
            <p class="text-xs uppercase tracking-wide text-slate-500">Your assignments for the day.</p>
          </div>
          <div class="flex flex-wrap items-center gap-2">
            <a href="{{ url_for('grade_lead_bp.my_duty', week=week_start.isoformat(), date=day.iso) }}" class="px-3 py-2 text-xs font-semibold rounded-lg {% if day.is_focus_day %}bg-emerald-100 text-emerald-700{% else %}bg-slate-100 text-slate-600 hover:bg-slate-200{% endif %}">
              {% if day.is_focus_day %}Focused{% else %}Focus this day{% endif %}
            </a>
          </div>
        </div>
        <div class="p-6 space-y-4">
          {% if day["daily_duties"] %}
          <div class="bg-slate-50 border border-slate-200 rounded-xl p-4 space-y-4">
            <h3 class="text-sm font-semibold text-slate-900">Daily Duties</h3>
            <ul class="space-y-4 text-sm text-slate-600">
              {% for duty in day["daily_duties"] %}
              <li class="bg-white border border-slate-200 rounded-lg p-4 space-y-3">
                <div class="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <p class="font-medium text-slate-800">{{ duty.label }}</p>
                    <p class="text-xs text-slate-500">Location: {{ duty.location }}</p>
                  </div>
                  <span class="inline-flex items-center px-2 py-1 rounded-full text-xs font-semibold uppercase tracking-wide {{ duty.status_badge }}">{{ duty.status_label }}</span>
                </div>
                {% if duty.note %}
                <p class="text-xs text-slate-500">Note: {{ duty.note }}</p>
                {% endif %}
                {% if duty.updated_at %}
                <p class="text-xs text-slate-400">Updated {{ duty.updated_at.strftime('%d %b %Y %H:%M') }}</p>
                {% endif %}
                <form method="post" action="{{ url_for('grade_lead_bp.update_daily_duty') }}" class="space-y-2">
                  <input type="hidden" name="assignment_id" value="{{ duty.id }}">
                  <input type="hidden" name="date" value="{{ day.iso }}">
                  <input type="hidden" name="week" value="{{ week_start.isoformat() }}">
                  <div>
                    <label class="block text-xs font-medium text-slate-700 mb-1">Update status</label>
                    <select name="status" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-white">
                      {% for key, label in status_labels.items() %}
                      <option value="{{ key }}" {% if key == duty.status %}selected{% endif %}>{{ label }}</option>
                      {% endfor %}
                    </select>
                  </div>
                  <div>
                    <label class="block text-xs font-medium text-slate-700 mb-1">Add a note {% if duty.status == 'unavailable' %}(required){% else %}(optional){% endif %}</label>
                    <textarea name="note" rows="2" class="w-full px-3 py-2 border border-slate-300 rounded-lg" placeholder="Example: Covering another class">{{ duty.note }}</textarea>
                  </div>
                  <button class="px-4 py-2 bg-emerald-600 text-white text-xs font-semibold rounded-lg hover:bg-emerald-700 transition">Save update</button>
                  <p class="text-xs text-slate-500">Choose "Checked in" when you arrive, or include a note if you are excused.</p>
                </form>
              </li>
              {% endfor %}
            </ul>
          </div>
          {% endif %}
          {% if day["items"] %}
            {% for item in day["items"] %}
            <div class="bg-white border border-slate-200 rounded-xl shadow-sm p-5 space-y-4">
              <div class="flex flex-wrap justify-between items-start gap-3">
                <div>
                  <p class="text-xs uppercase tracking-wide text-slate-500">{{ 'Break Duty' if item.is_break else 'Pod Duty' }}</p>
                  <h3 class="text-xl font-semibold text-slate-900">{{ item.label }}</h3>
                  <p class="text-sm text-slate-600">Grade {{ item.grade }}{% if item.is_break %} - {{ break_label }}{% else %} - {{ item.pod }}{% endif %}</p>
                  {% if item.is_break and item.break_location %}
                  <p class="text-xs text-slate-500">Location: {{ item.break_location }}</p>
                  {% endif %}
                </div>
                <div class="space-y-1 text-right">
                  <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold uppercase tracking-wide {{ item.status_badge }}">{{ item.status_label }}</span>
                  {% if item.note %}
                  <p class="text-xs text-slate-500">Note: {{ item.note }}</p>
                  {% endif %}
                  {% if item.updated_at %}
                  <p class="text-xs text-slate-400">Updated {{ item.updated_at.strftime('%d %b %Y %H:%M') }}</p>
                  {% endif %}
                </div>
              </div>

              <form method="post" action="{{ url_for('grade_lead_bp.update_my_duty') }}" class="space-y-3">
                <input type="hidden" name="assignment_id" value="{{ item.id }}">
                <input type="hidden" name="date" value="{{ day.iso }}">
                <input type="hidden" name="week" value="{{ week_start.isoformat() }}">
                <div>
                  <label class="block text-sm font-medium text-slate-700 mb-1">Update status</label>
                  <select name="status" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-white">
                    {% for key, label in status_labels.items() %}
                    <option value="{{ key }}" {% if key == item.status %}selected{% endif %}>{{ label }}</option>
                    {% endfor %}
                  </select>
                </div>
                <div>
                  <label class="block text-sm font-medium text-slate-700 mb-1">Add a note {% if item.status == 'unavailable' %}(required){% else %}(optional){% endif %}</label>
                  <textarea name="note" rows="3" class="w-full px-3 py-2 border border-slate-300 rounded-lg" placeholder="Example: Covering another class, feeling unwell, etc.">{{ item.note }}</textarea>
                </div>
                <div class="flex flex-wrap items-center gap-2">
                  <button class="px-4 py-2 bg-emerald-600 text-white rounded-lg text-sm font-medium hover:bg-emerald-700 transition">
                    Save update
                  </button>
                  <p class="text-xs text-slate-500">Choose \"Checked in\" when you arrive, or \"Unavailable\" with a brief reason if you cannot attend.</p>
                </div>
              </form>
            </div>
            {% endfor %}
          {% elif not day["daily_duties"] %}
            <div class="bg-slate-50 border border-slate-200 rounded-xl px-4 py-3 text-sm text-slate-600 italic">
              No duty assignments for this day.
            </div>
          {% endif %}
        </div>
      </section>
      {% endfor %}
    </div>
  </div>
</body>
</html>
"""

    return render_template_string(
        template,
        week_start=week_start,
        week_end=week_end,
        week_payload=week_payload,
        status_labels=STATUS_LABELS,
        status_badges=STATUS_BADGES,
        available_dates=available_dates,
        focus_date=focus_date,
        prev_week=prev_week,
        next_week=next_week,
        has_assignments=has_assignments,
        has_daily_duties=has_daily_duties,
        weekly_duties=weekly_duties,
        break_label=BREAK_LABEL,
        session=session,
    )


@grade_lead_bp.route("/my-duty/update", methods=["POST"])
@login_required
def update_my_duty():
    _ensure_tables()
    _ensure_daily_tables()

    week_value = request.form.get("week")

    try:
        assignment_id = int(request.form.get("assignment_id", ""))
    except ValueError:
        flash("Invalid assignment.", "error")
        params = {}
        if week_value:
            params["week"] = week_value
        return redirect(url_for("grade_lead_bp.my_duty", **params))

    assignment = db.session.get(GradeLeadDutyAssignment, assignment_id)
    if not assignment:
        flash("Assignment not found.", "error")
        params = {}
        if week_value:
            params["week"] = week_value
        return redirect(url_for("grade_lead_bp.my_duty", **params))

    teacher_id = session.get("teacher_id")
    is_admin = session.get("is_admin", False)
    if assignment.teacher_id != teacher_id and not is_admin:
        flash("You are not allowed to update this assignment.", "error")
        params = {"week": week_value} if week_value else {}
        return redirect(url_for("grade_lead_bp.my_duty", **params))

    status = (request.form.get("status") or "pending").strip().lower()
    if status not in STATUS_LABELS:
        flash("Invalid status selected.", "error")
        return redirect(url_for("grade_lead_bp.my_duty", date=assignment.assignment_date.isoformat(), week=week_value))

    note = (request.form.get("note") or "").strip()
    if status == "unavailable" and not note:
        flash("Please add a brief reason when you cannot attend.", "warning")
        return redirect(url_for("grade_lead_bp.my_duty", date=assignment.assignment_date.isoformat(), week=week_value))

    ack = assignment.acknowledgement
    if not ack:
        ack = GradeLeadDutyAcknowledgement(
            assignment_id=assignment.id,
            teacher_id=assignment.teacher_id,
        )
        db.session.add(ack)

    ack.status = status
    ack.note = note if note else (ack.note if status != "pending" else None)
    if status != "unavailable" and not note:
        ack.note = None
    ack.updated_at = datetime.utcnow()

    db.session.commit()

    if status == "unavailable":
        flash("Your grade lead has been notified that you are unavailable.", "warning")
    elif status == "present":
        flash("Thank you for checking in. Have a great duty!", "success")
    else:
        flash("Status updated.", "success")

    target_date = request.form.get("date") or assignment.assignment_date.isoformat()
    params = {"date": target_date}
    if week_value:
        params["week"] = week_value
    return redirect(url_for("grade_lead_bp.my_duty", **params))


@grade_lead_bp.route("/daily-duty/update", methods=["POST"])
@login_required
def update_daily_duty():
    _ensure_tables()
    _ensure_daily_tables()

    week_value = request.form.get("week")

    try:
        assignment_id = int(request.form.get("assignment_id", ""))
    except ValueError:
        flash("Invalid assignment.", "error")
        params = {}
        if week_value:
            params["week"] = week_value
        return redirect(url_for("grade_lead_bp.my_duty", **params))

    assignment = db.session.get(DailyDutyAssignment, assignment_id)
    if not assignment:
        flash("Assignment not found.", "error")
        params = {"week": week_value} if week_value else {}
        return redirect(url_for("grade_lead_bp.my_duty", **params))

    teacher_id = session.get("teacher_id")
    is_admin = session.get("is_admin", False)
    if assignment.teacher_id != teacher_id and not is_admin:
        flash("You are not allowed to update this duty.", "error")
        params = {"week": week_value} if week_value else {}
        return redirect(url_for("grade_lead_bp.my_duty", **params))

    status = (request.form.get("status") or "pending").strip().lower()
    if status not in STATUS_LABELS:
        flash("Invalid status selected.", "error")
        return redirect(url_for("grade_lead_bp.my_duty", date=assignment.assignment_date.isoformat(), week=week_value))

    note = (request.form.get("note") or "").strip()
    if status == "unavailable" and not note:
        flash("Please add a brief reason when you cannot attend.", "warning")
        return redirect(url_for("grade_lead_bp.my_duty", date=assignment.assignment_date.isoformat(), week=week_value))

    ack = assignment.acknowledgement
    if not ack:
        ack = DailyDutyAcknowledgement(
            assignment_id=assignment.id,
            teacher_id=assignment.teacher_id,
        )
        db.session.add(ack)

    ack.status = status
    ack.note = note if note else (ack.note if status != "pending" else None)
    if status != "unavailable" and not note:
        ack.note = None
    ack.updated_at = datetime.utcnow()

    db.session.commit()

    if status == "unavailable":
        flash("Duty marked as excused. Thank you for letting us know.", "warning")
    elif status == "present":
        flash("Thank you for checking in for your duty.", "success")
    else:
        flash("Status updated.", "success")

    target_date = request.form.get("date") or assignment.assignment_date.isoformat()
    params = {"date": target_date}
    if week_value:
        params["week"] = week_value
    return redirect(url_for("grade_lead_bp.my_duty", **params))

@grade_lead_bp.route("/assign/bulk", methods=["POST"])
@login_required
def assign_bulk():
    if not _ensure_grade_lead_access():
        return jsonify({"error": "Access denied."}), 403

    _ensure_tables()

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Invalid payload."}), 400

    grade = (payload.get("grade") or "").strip()
    if not grade or not _validate_grade_access(grade):
        return jsonify({"error": "Invalid grade supplied."}), 400

    assignment_date = _parse_date(payload.get("assignment_date"))
    if not assignment_date:
        return jsonify({"error": "Invalid assignment date supplied."}), 400

    slots_payload = payload.get("slots") or []
    if not isinstance(slots_payload, list):
        return jsonify({"error": "Invalid slots payload."}), 400

    desired_map: Dict[Tuple[str, str, Optional[int]], Dict[int, Dict[str, object]]] = {}
    teacher_ids: set[int] = set()
    errors: List[str] = []
    valid_pods = set(_pods_for_grade(grade))

    for slot_data in slots_payload:
        if not isinstance(slot_data, dict):
            continue
        slot_type = (slot_data.get("slot_type") or "").strip().lower()
        pod = (slot_data.get("pod") or "").strip()
        period_value = slot_data.get("period")
        assignments_list = slot_data.get("assignments") or []

        if slot_type not in {"period", "break"}:
            errors.append("Invalid slot type supplied.")
            continue

        if slot_type == "period":
            try:
                period = int(period_value)
            except (TypeError, ValueError):
                errors.append("Invalid period supplied for a slot.")
                continue
            max_period = GRADE_PERIODS.get(grade, 0)
            if period < 1 or period > max_period:
                errors.append(f"Period {period} out of range for grade {grade}.")
                continue
            if pod not in valid_pods:
                errors.append("Invalid pod supplied.")
                continue
        else:
            period = None
            if pod != BREAK_POD_KEY:
                errors.append("Invalid break pod supplied.")
                continue

        key = (slot_type, pod, period)
        slot_assignments: Dict[int, Dict[str, object]] = {}
        if not isinstance(assignments_list, list):
            errors.append("Assignments payload must be a list.")
            continue

        for assignment_entry in assignments_list:
            if not isinstance(assignment_entry, dict):
                continue
            try:
                teacher_id = int(assignment_entry.get("teacher_id"))
            except (TypeError, ValueError):
                errors.append("Invalid teacher identifier supplied.")
                continue

            assignment_payload: Dict[str, object] = {}
            if slot_type == "break" and grade in BREAK_LOCATION_GRADES:
                location_key = (assignment_entry.get("location") or "").strip().lower()
                if location_key not in BREAK_LOCATIONS:
                    errors.append("Break duty assignments require a valid location.")
                    continue
                assignment_payload["location_key"] = location_key
                assignment_payload["location_label"] = BREAK_LOCATIONS[location_key]
            elif slot_type == "break":
                location_key = (assignment_entry.get("location") or "").strip().lower()
                if location_key in BREAK_LOCATIONS:
                    assignment_payload["location_key"] = location_key
                    assignment_payload["location_label"] = BREAK_LOCATIONS[location_key]

            slot_assignments[teacher_id] = assignment_payload
            teacher_ids.add(teacher_id)

        desired_map[key] = slot_assignments

    teacher_map: Dict[int, Teacher] = {}
    if teacher_ids:
        teacher_rows = (
            db.session.query(Teacher)
            .filter(Teacher.id.in_(teacher_ids))
            .all()
        )
        teacher_map = {teacher.id: teacher for teacher in teacher_rows}
        missing_ids = [tid for tid in teacher_ids if tid not in teacher_map]
        if missing_ids:
            errors.append("One or more teachers could not be found.")

    existing_records = (
        db.session.query(GradeLeadDutyAssignment)
        .filter(
            GradeLeadDutyAssignment.assignment_date == assignment_date,
            GradeLeadDutyAssignment.grade == grade,
        )
        .all()
    )

    existing_map: Dict[Tuple[str, str, Optional[int]], Dict[int, GradeLeadDutyAssignment]] = {}
    for existing in existing_records:
        key = (existing.slot_type, existing.pod, existing.period)
        existing_map.setdefault(key, {})[existing.teacher_id] = existing

    for key, assignments in existing_map.items():
        desired_assignments = desired_map.get(key, {})
        for teacher_id, assignment in list(assignments.items()):
            if teacher_id not in desired_assignments:
                db.session.delete(assignment)

    for key, desired_assignments in desired_map.items():
        slot_type, pod, period = key
        for teacher_id, assignment_payload in desired_assignments.items():
            teacher = teacher_map.get(teacher_id)
            if not teacher:
                continue

            existing_assignment = existing_map.get(key, {}).get(teacher_id)
            if existing_assignment:
                if slot_type == "break":
                    location_label = assignment_payload.get("location_label")
                    if location_label and location_label != (existing_assignment.break_location or ""):
                        existing_assignment.break_location = location_label
                continue

            if slot_type == "period" and period is not None:
                conflict = (
                    db.session.query(GradeLeadDutyAssignment)
                    .filter(
                        GradeLeadDutyAssignment.assignment_date == assignment_date,
                        GradeLeadDutyAssignment.teacher_id == teacher_id,
                        GradeLeadDutyAssignment.slot_type == "period",
                        GradeLeadDutyAssignment.period == period,
                    )
                    .first()
                )
                if conflict:
                    errors.append(
                        f"{teacher.name} is already assigned to period {period} on {assignment_date}."
                    )
                    continue

            new_assignment = GradeLeadDutyAssignment(
                assignment_date=assignment_date,
                grade=grade,
                pod=pod,
                slot_type=slot_type,
                period=period,
                teacher_id=teacher_id,
                created_by_teacher_id=session.get("teacher_id"),
            )
            if slot_type == "break":
                new_assignment.break_location = assignment_payload.get("location_label")
            db.session.add(new_assignment)

    db.session.commit()

    if errors:
        flash("Assignments updated with some warnings.", "warning")
    else:
        flash("Assignments updated.", "success")

    return jsonify({"status": "ok", "errors": errors})


@grade_lead_bp.route("/assign", methods=["POST"])
@login_required
def assign_teacher():
    if not _ensure_grade_lead_access():
        return redirect(url_for("behaviour_bp.behaviour_dashboard"))

    _ensure_tables()

    week_value = request.form.get("week")
    focus_date = request.form.get("date")

    params = _prepare_assignment_parameters()
    if not params:
        week_param = week_value
        if not week_param:
            fallback_date = _parse_date(request.form.get("assignment_date"))
            if fallback_date:
                week_param = _week_start(fallback_date).isoformat()
        redirect_kwargs = {}
        grade_value = request.form.get("grade")
        if grade_value:
            redirect_kwargs["grade"] = grade_value
        assignment_date_value = request.form.get("assignment_date")
        if focus_date:
            redirect_kwargs["date"] = focus_date
        elif assignment_date_value:
            redirect_kwargs["date"] = assignment_date_value
        if week_param:
            redirect_kwargs["week"] = week_param
        return redirect(url_for("grade_lead_bp.dashboard", **redirect_kwargs))

    filters = [
        GradeLeadDutyAssignment.assignment_date == params["assignment_date"],
        GradeLeadDutyAssignment.teacher_id == params["teacher_id"],
        GradeLeadDutyAssignment.slot_type == params["slot_type"],
    ]
    if params["slot_type"] == "period":
        filters.append(GradeLeadDutyAssignment.period == params["period"])

    existing = (
        db.session.query(GradeLeadDutyAssignment)
        .filter(*filters)
        .first()
    )
    if existing:
        if params["slot_type"] == "period":
            flash(
                f"{params['teacher'].name} is already assigned during period {params['period']} on {params['assignment_date']}.",
                "error",
            )
        else:
            flash(
                f"{params['teacher'].name} already has a break duty on {params['assignment_date']}.",
                "error",
            )
        week_param = week_value or _week_start(params["assignment_date"]).isoformat()
        redirect_kwargs = {
            "grade": params["grade"],
            "date": focus_date or params["assignment_date"].isoformat(),
        }
        if week_param:
            redirect_kwargs["week"] = week_param
        return redirect(url_for("grade_lead_bp.dashboard", **redirect_kwargs))

    assignment = GradeLeadDutyAssignment(
        assignment_date=params["assignment_date"],
        grade=params["grade"],
        pod=params["pod"],
        slot_type=params["slot_type"],
        period=params["period"],
        teacher_id=params["teacher_id"],
        created_by_teacher_id=session.get("teacher_id"),
        break_location=params["break_location"],
    )
    db.session.add(assignment)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        flash(
            "That teacher already has a conflicting assignment for this slot.",
            "error",
        )
        week_param = week_value or _week_start(params["assignment_date"]).isoformat()
        redirect_kwargs = {
            "grade": params["grade"],
            "date": focus_date or params["assignment_date"].isoformat(),
        }
        if week_param:
            redirect_kwargs["week"] = week_param
        return redirect(url_for("grade_lead_bp.dashboard", **redirect_kwargs))

    flash("Teacher assigned.", "success")
    week_param = week_value or _week_start(params["assignment_date"]).isoformat()
    redirect_kwargs = {
        "grade": params["grade"],
        "date": focus_date or params["assignment_date"].isoformat(),
    }
    if week_param:
        redirect_kwargs["week"] = week_param
    return redirect(
        url_for("grade_lead_bp.dashboard", **redirect_kwargs)
    )


@grade_lead_bp.route("/remove", methods=["POST"])
@login_required
def remove_assignment():
    if not _ensure_grade_lead_access():
        return redirect(url_for("behaviour_bp.behaviour_dashboard"))

    _ensure_tables()

    week_value = request.form.get("week")
    focus_date = request.form.get("date")

    try:
        assignment_id = int(request.form.get("assignment_id", ""))
    except ValueError:
        flash("Invalid assignment.", "error")
        params = {}
        grade_value = request.form.get("grade")
        if grade_value:
            params["grade"] = grade_value
        if focus_date:
            params["date"] = focus_date
        if week_value:
            params["week"] = week_value
        return redirect(url_for("grade_lead_bp.dashboard", **params))

    assignment = db.session.get(GradeLeadDutyAssignment, assignment_id)
    if not assignment:
        flash("Assignment not found.", "error")
        params = {}
        grade_value = request.form.get("grade") or assignment.grade
        if grade_value:
            params["grade"] = grade_value
        if focus_date:
            params["date"] = focus_date
        if week_value:
            params["week"] = week_value
        return redirect(url_for("grade_lead_bp.dashboard", **params))

    if not _validate_grade_access(assignment.grade):
        redirect_kwargs = {"grade": assignment.grade}
        if focus_date:
            redirect_kwargs["date"] = focus_date
        elif week_value:
            maybe_date = request.form.get("assignment_date")
            if maybe_date:
                redirect_kwargs["date"] = maybe_date
        if week_value:
            redirect_kwargs["week"] = week_value
        return redirect(url_for("grade_lead_bp.dashboard", **redirect_kwargs))

    db.session.delete(assignment)
    db.session.commit()

    flash("Assignment removed.", "success")
    week_param = week_value or _week_start(assignment.assignment_date).isoformat()
    redirect_kwargs = {
        "grade": assignment.grade,
        "date": focus_date or assignment.assignment_date.isoformat(),
    }
    if week_param:
        redirect_kwargs["week"] = week_param
    return redirect(
        url_for("grade_lead_bp.dashboard", **redirect_kwargs)
    )
