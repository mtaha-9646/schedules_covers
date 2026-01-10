
"""Model Context Protocol server for the Behavior application.

The server exposes curated access to behaviour.db so AI assistants can
search for students, inspect their behaviour history, and review recent
incidents. It can run over stdio or Streamable HTTP (FastMCP).
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import parse_qs, urlparse

import anyio
from flask import Flask
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload

from extensions import db
from behaviour import (
    BehaviorContract,
    Incident,
    ParentAcknowledgment,
    ParentMeeting,
    PhoneViolationContract,
    SafeguardingConcern,
    StaffStatement,
    Students,
    StudentStatement,
    Suspension,
    TeacherExcuse,
)

from mcp import types
from mcp.server import Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.stdio import stdio_server

try:  # Fast transport is optional when running pure-stdio
    from mcp.server.fastmcp import FastMCP
except Exception:  # pragma: no cover - FastMCP may be unavailable
    FastMCP = None  # type: ignore[assignment]

# Map incident grades to friendly labels for summaries
INCIDENT_SEVERITY = {
    "C1": "Minor",
    "C2": "Minor",
    "C3": "Major",
    "C4": "Major",
}

DEFAULT_HISTORY_SECTIONS = {
    "incidents",
    "suspensions",
    "parent_meetings",
    "parent_acknowledgments",
    "student_statements",
    "staff_statements",
    "safeguarding_concerns",
    "phone_violations",
    "behavior_contracts",
}


def _coerce_iso_datetime(value: str | None) -> datetime | None:
    """Parse ISO-like strings into datetimes, raising if malformed."""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Could not parse date/datetime value '{value}'") from exc


def _isoformat(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="minutes")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, candidate))

ABSENCE_STATUSES = {"pending", "approved", "rejected", "invalid"}


def serialize_teacher_excuse(excuse: TeacherExcuse) -> dict[str, Any]:
    teacher = getattr(excuse, "teacher", None)
    return {
        "id": excuse.id,
        "teacher_id": excuse.teacher_id,
        "teacher_name": getattr(teacher, "name", None),
        "teacher_email": getattr(teacher, "email", None),
        "leave_type": excuse.leave_type,
        "status": excuse.status,
        "reason": excuse.reason,
        "leave_date": _isoformat(excuse.leave_date),
        "end_date": _isoformat(excuse.end_date),
        "start_time": excuse.start_time.isoformat() if excuse.start_time else None,
        "end_time": excuse.end_time.isoformat() if excuse.end_time else None,
        "attachment_required": bool(excuse.attachment_required),
        "attachment_status": excuse.attachment_status,
        "attachment_path": excuse.attachment_path,
        "attachment_due_at": _isoformat(excuse.attachment_due_at),
        "created_at": _isoformat(excuse.created_at),
        "updated_at": _isoformat(excuse.updated_at),
        "admin_comment": excuse.admin_comment,
        "reviewed_by": excuse.reviewed_by,
        "reviewed_at": _isoformat(excuse.reviewed_at),
    }


def _coerce_iso_date(value: str | None) -> date | None:
    dt = _coerce_iso_datetime(value)
    if not dt:
        return None
    if isinstance(dt, datetime):
        return dt.date()
    return dt


def serialize_incident(incident: Incident) -> dict[str, Any]:
    return {
        "id": incident.id,
        "esis": incident.esis,
        "student_name": incident.name,
        "homeroom": incident.homeroom,
        "date_of_incident": _isoformat(incident.date_of_incident),
        "place_of_incident": incident.place_of_incident,
        "incident_grade": incident.incident_grade,
        "severity": INCIDENT_SEVERITY.get(incident.incident_grade, "Unknown"),
        "action_taken": incident.action_taken,
        "incident_description": incident.incident_description,
        "attachment": incident.attachment,
        "created_at": _isoformat(incident.created_at),
        "teacher_id": incident.teacher_id,
        "teacher_name": getattr(getattr(incident, "teacher", None), "name", None),
    }


def serialize_suspension(record: Suspension) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "grade_class": record.grade_class,
        "date_of_suspension": _isoformat(record.date_of_suspension),
        "duration": record.duration,
        "reason": record.reason,
        "incident_details": record.incident_details,
        "parent_contacted": bool(record.parent_contacted),
        "parent_meeting": bool(record.parent_meeting),
        "behavior_plan": record.behavior_plan,
        "assigned_staff": record.assigned_staff,
        "reintegration_plan": record.reintegration_plan,
        "notes": record.notes,
        "created_at": _isoformat(record.created_at),
    }


def serialize_parent_meeting(record: ParentMeeting) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "grade_session": record.grade_session,
        "parent_name": record.parent_name,
        "attended_by": record.attended_by,
        "date": _isoformat(record.date),
        "time": record.time,
        "requested_by": record.requested_by,
        "parent_concerns": record.parent_concerns,
        "school_concerns": record.school_concerns,
        "solutions_parent": record.solutions_parent,
        "solutions_school": record.solutions_school,
        "agreed_next_steps": record.agreed_next_steps,
        "created_at": _isoformat(record.created_at),
    }


def serialize_parent_ack(record: ParentAcknowledgment) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "grade_session": record.grade_session,
        "date": _isoformat(record.date),
        "created_at": _isoformat(record.created_at),
    }


def serialize_student_statement(record: StudentStatement) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "class_session": record.class_session,
        "date": _isoformat(record.date),
        "time": record.time,
        "location": record.location,
        "statement": record.statement,
        "other_details": record.other_details,
        "completed_by": record.completed_by,
        "completed_by_role": record.completed_by_role,
        "reviewed_by": record.reviewed_by,
        "created_at": _isoformat(record.created_at),
    }


def serialize_staff_statement(record: StaffStatement) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "staff_name": record.staff_name,
        "position": record.position,
        "date_of_incident": _isoformat(record.date_of_incident),
        "time_of_incident": record.time_of_incident,
        "location_of_incident": record.location_of_incident,
        "date_of_statement": _isoformat(record.date_of_statement),
        "details": record.details,
        "individuals_involved": record.individuals_involved,
        "actions_taken": record.actions_taken,
        "witnesses": record.witnesses,
        "additional_comments": record.additional_comments,
        "slt_name": record.slt_name,
        "slt_position": record.slt_position,
        "slt_date_review": _isoformat(record.slt_date_review),
        "slt_actions": record.slt_actions,
        "created_at": _isoformat(record.created_at),
    }


def serialize_safeguarding(record: SafeguardingConcern) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "grade_session": record.grade_session,
        "reporting_name": record.reporting_name,
        "reporting_role": record.reporting_role,
        "report_date": _isoformat(record.report_date),
        "report_time": record.report_time,
        "incident_date": _isoformat(record.incident_date),
        "incident_time": record.incident_time,
        "incident_location": record.incident_location,
        "description": record.description,
        "concern_types": record.concern_types,
        "student_disclosure": record.student_disclosure,
        "disclosure_details": record.disclosure_details,
        "immediate_actions": record.immediate_actions,
        "referred_to": record.referred_to,
        "referral_time": record.referral_time,
        "referral_date": _isoformat(record.referral_date),
        "follow_up_actions": record.follow_up_actions,
        "additional_notes": record.additional_notes,
        "created_at": _isoformat(record.created_at),
    }


def serialize_phone_violation(record: PhoneViolationContract) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "grade_session": record.grade_session,
        "date": _isoformat(record.date),
        "created_at": _isoformat(record.created_at),
    }


def serialize_behavior_contract(record: BehaviorContract) -> dict[str, Any]:
    return {
        "id": record.id,
        "esis": record.esis,
        "student_name": record.student_name,
        "grade": record.grade,
        "date": _isoformat(record.date),
        "time": record.time,
        "cons_warning": record.cons_warning,
        "cons_parent_meeting": record.cons_parent_meeting,
        "cons_detention": record.cons_detention,
        "cons_referral": record.cons_referral,
        "cons_further_action": record.cons_further_action,
        "cons_further_action_text": record.cons_further_action_text,
        "created_at": _isoformat(record.created_at),
    }



class BehaviorRepository:
    """Read-only access to the behaviour.db database."""

    def __init__(self, db_path: Path):
        if not db_path.exists():
            raise FileNotFoundError(f"Behavior database not found at {db_path}")

        self.db_path = db_path
        self.app = Flask("behavior_mcp")
        uri = f"sqlite:///{db_path}"
        self.app.config["SQLALCHEMY_DATABASE_URI"] = uri
        self.app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
        self.app.config["SQLALCHEMY_BINDS"] = {"teachers_bind": uri}
        db.init_app(self.app)

    @contextmanager
    def session_scope(self):
        with self.app.app_context():
            try:
                yield db.session
            finally:
                db.session.remove()

    def search_students(self, query: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        query_value = (query or "").strip()
        limit = _clamp_int(limit, default=20, minimum=1, maximum=100)

        with self.session_scope() as session:
            stmt = session.query(Students)
            if query_value:
                pattern = f"%{query_value.lower()}%"
                stmt = stmt.filter(
                    or_(
                        func.lower(Students.name).like(pattern),
                        func.lower(Students.esis).like(pattern),
                        func.lower(func.coalesce(Students.homeroom, "")).like(pattern),
                    )
                )
            stmt = stmt.order_by(func.lower(Students.name)).limit(limit)
            results = []
            for student in stmt.all():
                results.append(
                    {
                        "esis": student.esis,
                        "name": student.name,
                        "homeroom": student.homeroom,
                    }
                )
            return results

    def list_incidents(
        self,
        *,
        esis: str | None = None,
        grade: str | None = None,
        homeroom: str | None = None,
        after: datetime | None = None,
        before: datetime | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        limit = _clamp_int(limit, default=20, minimum=1, maximum=200)

        with self.session_scope() as session:
            stmt = session.query(Incident).options(joinedload(Incident.teacher))
            if esis:
                stmt = stmt.filter(Incident.esis == esis)
            if grade:
                stmt = stmt.filter(Incident.incident_grade == grade)
            if homeroom:
                stmt = stmt.filter(Incident.homeroom == homeroom)
            if after:
                stmt = stmt.filter(Incident.date_of_incident >= after)
            if before:
                stmt = stmt.filter(Incident.date_of_incident <= before)

            stmt = stmt.order_by(Incident.date_of_incident.desc()).limit(limit)
            return [serialize_incident(record) for record in stmt.all()]

    def get_overview(self) -> dict[str, Any]:
        today = date.today()
        last_30_days = today - timedelta(days=30)
        one_week = today - timedelta(days=7)

        with self.session_scope() as session:
            total_students = session.query(func.count(Students.id)).scalar() or 0
            total_incidents = session.query(func.count(Incident.id)).scalar() or 0
            incidents_last_week = (
                session.query(func.count(Incident.id))
                .filter(Incident.date_of_incident >= one_week)
                .scalar()
                or 0
            )
            incidents_by_grade = {
                grade or "Unknown": count
                for grade, count in session.query(Incident.incident_grade, func.count(Incident.id)).group_by(Incident.incident_grade)
            }

            top_students = [
                {
                    "esis": esis,
                    "student_name": name,
                    "incident_count": count,
                }
                for esis, name, count in (
                    session.query(Incident.esis, Incident.name, func.count(Incident.id))
                    .group_by(Incident.esis, Incident.name)
                    .order_by(func.count(Incident.id).desc())
                    .limit(5)
                    .all()
                )
            ]

            latest_incident_obj = (
                session.query(Incident)
                .options(joinedload(Incident.teacher))
                .order_by(Incident.date_of_incident.desc())
                .first()
            )
            latest_incident = serialize_incident(latest_incident_obj) if latest_incident_obj else None

            suspensions_last_30 = (
                session.query(func.count(Suspension.id))
                .filter(Suspension.date_of_suspension >= last_30_days)
                .scalar()
                or 0
            )

            safeguarding_open = session.query(func.count(SafeguardingConcern.id)).scalar() or 0

            return {
                "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
                "totals": {
                    "students": total_students,
                    "incidents": total_incidents,
                    "incidents_last_7_days": incidents_last_week,
                    "suspensions_last_30_days": suspensions_last_30,
                    "safeguarding_records": safeguarding_open,
                },
                "incidents_by_grade": incidents_by_grade,
                "top_students_by_incidents": top_students,
                "latest_incident": latest_incident,
            }

    def get_student_history(
        self,
        *,
        esis: str,
        include: Sequence[str] | None = None,
        max_records: int = 25,
    ) -> dict[str, Any]:
        sections = set(include or DEFAULT_HISTORY_SECTIONS)
        max_records = _clamp_int(max_records, default=25, minimum=1, maximum=200)

        with self.session_scope() as session:
            student = session.query(Students).filter(Students.esis == esis).first()
            if not student:
                raise ValueError(f"No student found with ESIS '{esis}'")

            history: dict[str, Any] = {
                "student": {
                    "esis": student.esis,
                    "name": student.name,
                    "homeroom": student.homeroom,
                },
                "summary": {},
            }

            if "incidents" in sections:
                incidents = (
                    session.query(Incident)
                    .options(joinedload(Incident.teacher))
                    .filter(Incident.esis == esis)
                    .order_by(Incident.date_of_incident.desc())
                    .limit(max_records)
                    .all()
                )
                incidents_data = [serialize_incident(item) for item in incidents]
                history["incidents"] = incidents_data
            else:
                incidents_data = []

            if "suspensions" in sections:
                suspensions = (
                    session.query(Suspension)
                    .filter(Suspension.esis == esis)
                    .order_by(Suspension.date_of_suspension.desc())
                    .limit(max_records)
                    .all()
                )
                history["suspensions"] = [serialize_suspension(item) for item in suspensions]

            if "parent_meetings" in sections:
                meetings = (
                    session.query(ParentMeeting)
                    .filter(ParentMeeting.esis == esis)
                    .order_by(ParentMeeting.date.desc())
                    .limit(max_records)
                    .all()
                )
                history["parent_meetings"] = [serialize_parent_meeting(item) for item in meetings]

            if "parent_acknowledgments" in sections:
                acks = (
                    session.query(ParentAcknowledgment)
                    .filter(ParentAcknowledgment.esis == esis)
                    .order_by(ParentAcknowledgment.date.desc())
                    .limit(max_records)
                    .all()
                )
                history["parent_acknowledgments"] = [serialize_parent_ack(item) for item in acks]

            if "student_statements" in sections:
                statements = (
                    session.query(StudentStatement)
                    .filter(StudentStatement.esis == esis)
                    .order_by(StudentStatement.date.desc())
                    .limit(max_records)
                    .all()
                )
                history["student_statements"] = [serialize_student_statement(item) for item in statements]

            if "staff_statements" in sections:
                staff_statements = (
                    session.query(StaffStatement)
                    .filter(StaffStatement.esis == esis)
                    .order_by(StaffStatement.date_of_incident.desc())
                    .limit(max_records)
                    .all()
                )
                history["staff_statements"] = [serialize_staff_statement(item) for item in staff_statements]

            if "safeguarding_concerns" in sections:
                safeguarding = (
                    session.query(SafeguardingConcern)
                    .filter(SafeguardingConcern.esis == esis)
                    .order_by(SafeguardingConcern.report_date.desc())
                    .limit(max_records)
                    .all()
                )
                history["safeguarding_concerns"] = [serialize_safeguarding(item) for item in safeguarding]

            if "phone_violations" in sections:
                phone_contracts = (
                    session.query(PhoneViolationContract)
                    .filter(PhoneViolationContract.esis == esis)
                    .order_by(PhoneViolationContract.date.desc())
                    .limit(max_records)
                    .all()
                )
                history["phone_violations"] = [serialize_phone_violation(item) for item in phone_contracts]

            if "behavior_contracts" in sections:
                contracts = (
                    session.query(BehaviorContract)
                    .filter(BehaviorContract.esis == esis)
                    .order_by(BehaviorContract.date.desc())
                    .limit(max_records)
                    .all()
                )
                history["behavior_contracts"] = [serialize_behavior_contract(item) for item in contracts]

            incident_grades = Counter(item["incident_grade"] for item in incidents_data)
            history["summary"] = {
                "total_incidents": len(incidents_data),
                "incidents_by_grade": incident_grades,
                "last_incident": incidents_data[0] if incidents_data else None,
                "has_suspensions": bool(history.get("suspensions")),
                "has_safeguarding": bool(history.get("safeguarding_concerns")),
            }

            return history

    def list_absence_requests(
        self,
        *,
        status: str | None = None,
        leave_type: str | None = None,
        teacher_email: str | None = None,
        after: date | None = None,
        before: date | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_status = (status or "").strip().lower()
        normalized_leave_type = (leave_type or "").strip().lower()
        normalized_email = (teacher_email or "").strip().lower()
        limit = _clamp_int(limit, default=limit, minimum=1, maximum=200)

        with self.session_scope() as session:
            stmt = session.query(TeacherExcuse).options(joinedload(TeacherExcuse.teacher))
            if normalized_status and normalized_status in ABSENCE_STATUSES:
                stmt = stmt.filter(TeacherExcuse.status == normalized_status)
            if normalized_leave_type:
                stmt = stmt.filter(func.lower(TeacherExcuse.leave_type) == normalized_leave_type)
            if normalized_email:
                stmt = stmt.join(Teacher, TeacherExcuse.teacher).filter(func.lower(Teacher.email) == normalized_email)
            if after:
                stmt = stmt.filter(TeacherExcuse.leave_date >= after)
            if before:
                stmt = stmt.filter(TeacherExcuse.leave_date <= before)

            stmt = stmt.order_by(TeacherExcuse.created_at.desc()).limit(limit)
            excuses = [serialize_teacher_excuse(record) for record in stmt.all()]

        summary = {
            "requested_limit": limit,
            "status_filter": normalized_status or None,
            "leave_type_filter": normalized_leave_type or None,
            "teacher_email_filter": normalized_email or None,
            "records_returned": len(excuses),
        }
        return {"summary": summary, "results": excuses}

    def list_teacher_whereabouts(
        self,
        *,
        as_of: date | None = None,
        leave_type: str | None = None,
    ) -> dict[str, Any]:
        target_date = as_of or date.today()
        normalized_leave_type = (leave_type or "").strip().lower()

        with self.session_scope() as session:
            stmt = (
                session.query(TeacherExcuse)
                .options(joinedload(TeacherExcuse.teacher))
                .filter(TeacherExcuse.status == "approved")
                .filter(TeacherExcuse.leave_date <= target_date)
                .filter(
                    or_(
                        TeacherExcuse.end_date.is_(None),
                        TeacherExcuse.end_date >= target_date,
                    )
                )
            )
            if normalized_leave_type:
                stmt = stmt.filter(func.lower(TeacherExcuse.leave_type) == normalized_leave_type)
            excused = stmt.all()
            excused_ids = {record.teacher_id for record in excused if record.teacher_id}
            excused_list = [serialize_teacher_excuse(record) for record in excused]

            teachers = session.query(Teacher).order_by(Teacher.name).all()
            present = [
                {
                    "teacher_id": teacher.id,
                    "name": teacher.name,
                    "email": teacher.email,
                    "subject": teacher.subject,
                    "grade": teacher.grade,
                }
                for teacher in teachers
                if teacher.id not in excused_ids
            ]

        header = {
            "date": target_date.isoformat(),
            "filter_leave_type": normalized_leave_type or None,
            "excused_count": len(excused_list),
            "present_count": len(present),
        }
        return {"header": header, "excused": excused_list, "present": present}

    def update_absence_request_status(
        self,
        request_id: int,
        *,
        status: str,
        admin_comment: str | None = None,
        reviewer: str | None = None,
    ) -> dict[str, Any]:
        normalized_status = (status or "").strip().lower()
        if normalized_status not in ABSENCE_STATUSES:
            raise ValueError(f"Unsupported status: {status}")

        with self.session_scope() as session:
            record = session.get(TeacherExcuse, request_id)
            if not record:
                raise ValueError(f"Absence request {request_id} not found.")
            record.status = normalized_status
            record.admin_comment = admin_comment
            record.reviewed_by = reviewer or "MCP"
            record.reviewed_at = datetime.utcnow()
            session.commit()
            session.refresh(record)
            return serialize_teacher_excuse(record)

    def recent_incidents_resource(self, limit: int = 20, grade: str | None = None) -> dict[str, Any]:
        incidents = self.list_incidents(limit=limit, grade=grade)
        return {
            "requested_limit": limit,
            "grade_filter": grade,
            "results": incidents,
        }


def _build_tool_definitions() -> list[types.Tool]:
    return [
        types.Tool(
            name="search_students",
            description="Find students by name, ESIS ID, or homeroom. Returns matches sorted alphabetically.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Partial name, ESIS, or homeroom to search for. If omitted, returns the first page of students.",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 20,
                        "description": "Maximum number of students to return.",
                    },
                },
                "additionalProperties": False,
            },
        ),
        types.Tool(
            name="list_incidents",
            description="Query recent incidents with optional filters (ESIS, grade, homeroom, date range).",
            inputSchema={
                "type": "object",
                "properties": {
                    "esis": {
                        "type": "string",
                        "description": "Limit to a specific student ESIS code.",
                    },
                    "grade": {
                        "type": "string",
                        "enum": ["C1", "C2", "C3", "C4"],
                        "description": "Filter by incident grade (C1-C4).",
                    },
                    "homeroom": {
                        "type": "string",
                        "description": "Filter by homeroom code.",
                    },
                    "after": {
                        "type": "string",
                        "description": "Lower bound ISO date/datetime (inclusive).",
                    },
                    "before": {
                        "type": "string",
                        "description": "Upper bound ISO date/datetime (inclusive).",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 20,
                        "description": "Maximum incidents to return.",
                    },
                },
                "additionalProperties": False,
            },
        ),
        types.Tool(
            name="get_student_history",
            description="Retrieve a student's combined behavior history with optional sections and record limits.",
            inputSchema={
                "type": "object",
                "properties": {
                    "esis": {
                        "type": "string",
                        "description": "Student ESIS identifier (required).",
                    },
                    "include": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": sorted(DEFAULT_HISTORY_SECTIONS),
                        },
                        "description": "Select which sections to include. Defaults to all.",
                    },
                    "max_records": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 25,
                        "description": "Maximum rows to pull for each section (incidents, suspensions, etc).",
                    },
                },
                "required": ["esis"],
                "additionalProperties": False,
            },
        ),
        types.Tool(
            name="list_absence_requests",
            description="Return absence excuses submitted by teachers with optional filters and metadata.",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": sorted(ABSENCE_STATUSES),
                        "description": "Filter requests by status (pending, approved, rejected, invalid).",
                    },
                    "leave_type": {
                        "type": "string",
                        "description": "Filter by leave type slug.",
                    },
                    "teacher_email": {
                        "type": "string",
                        "description": "Only return requests for this teacher email.",
                    },
                    "after": {
                        "type": "string",
                        "description": "Lower-bound ISO date for leave_date (inclusive).",
                    },
                    "before": {
                        "type": "string",
                        "description": "Upper-bound ISO date for leave_date (inclusive).",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 50,
                        "description": "Maximum number of leave requests to return.",
                    },
                },
                "additionalProperties": False,
            },
        ),
        types.Tool(
            name="list_teacher_whereabouts",
            description="Provide active teacher whereabouts (approved absences) for a specific date.",
            inputSchema={
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Target ISO date (defaults to today if missing).",
                    },
                    "leave_type": {
                        "type": "string",
                        "description": "Optional leave type filter.",
                    },
                },
                "additionalProperties": False,
            },
        ),
        types.Tool(
            name="update_absence_request_status",
            description="Approve or deny an absence request by changing its status and adding an optional note.",
            inputSchema={
                "type": "object",
                "properties": {
                    "request_id": {
                        "type": "integer",
                        "description": "Internal ID of the teacher excuse request (required).",
                    },
                    "status": {
                        "type": "string",
                        "enum": sorted(ABSENCE_STATUSES),
                        "description": "New status value (pending/approved/rejected/invalid).",
                    },
                    "admin_comment": {
                        "type": "string",
                        "description": "Optional note describing the decision.",
                    },
                    "reviewer": {
                        "type": "string",
                        "description": "Name or identifier of the reviewer (defaults to MCP).",
                    },
                },
                "required": ["request_id", "status"],
                "additionalProperties": False,
            },
        ),
    ]


def _execute_tool(repo: BehaviorRepository, tool_name: str, arguments: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    if tool_name == "search_students":
        limit = arguments.get("limit", 20)
        students = repo.search_students(arguments.get("query"), limit)
        summary_lines = [f"Students found: {len(students)} (showing up to {limit})"]
        for student in students[:10]:
            summary_lines.append(
                f"- {student['name']} ({student['esis']}) homeroom={student['homeroom'] or 'N/A'}"
            )
        structured = {
            "students": students,
            "summary_lines": summary_lines,
        }
        return summary_lines, structured

    if tool_name == "list_incidents":
        after = _coerce_iso_datetime(arguments.get("after"))
        before = _coerce_iso_datetime(arguments.get("before"))
        incidents = repo.list_incidents(
            esis=arguments.get("esis"),
            grade=arguments.get("grade"),
            homeroom=arguments.get("homeroom"),
            after=after,
            before=before,
            limit=arguments.get("limit", 20),
        )
        summary_lines = [f"Incidents found: {len(incidents)}"]
        for incident in incidents[:10]:
            summary_lines.append(
                f"- {incident['date_of_incident']} | {incident['student_name']} | {incident['incident_grade']} ({incident['severity']})"
            )
        structured = {
            "incidents": incidents,
            "summary_lines": summary_lines,
            "filters": {
                "esis": arguments.get("esis"),
                "grade": arguments.get("grade"),
                "homeroom": arguments.get("homeroom"),
                "after": arguments.get("after"),
                "before": arguments.get("before"),
                "limit": arguments.get("limit", 20),
            },
        }
        return summary_lines, structured

    if tool_name == "get_student_history":
        include = arguments.get("include")
        history = repo.get_student_history(
            esis=arguments["esis"],
            include=include,
            max_records=arguments.get("max_records", 25),
        )
        summary = history.get("summary", {})
        summary_text = (
            f"{history['student']['name']} ({history['student']['esis']}) | "
            f"Incidents: {summary.get('total_incidents', 0)} | "
            f"Suspensions: {len(history.get('suspensions', [])) if 'suspensions' in history else 0}"
        )
        summary_lines = [summary_text]
        history["text_summary"] = summary_text
        history["summary_lines"] = summary_lines
        return summary_lines, history

    if tool_name == "list_absence_requests":
        after = _coerce_iso_date(arguments.get("after"))
        before = _coerce_iso_date(arguments.get("before"))
        result = repo.list_absence_requests(
            status=arguments.get("status"),
            leave_type=arguments.get("leave_type"),
            teacher_email=arguments.get("teacher_email"),
            after=after,
            before=before,
            limit=arguments.get("limit", 50),
        )
        summary_lines = [
            f"Absence requests returned: {result['summary']['records_returned']}",
            f"Status filter: {result['summary']['status_filter']}",
            f"Leave type filter: {result['summary']['leave_type_filter']}",
        ]
        return summary_lines, result

    if tool_name == "list_teacher_whereabouts":
        target_date = _coerce_iso_date(arguments.get("date"))
        result = repo.list_teacher_whereabouts(
            as_of=target_date,
            leave_type=arguments.get("leave_type"),
        )
        summary_lines = [
            f"Teacher whereabouts for {result['header']['date']}",
            f"Excused teachers: {result['header']['excused_count']}",
            f"Present teachers: {result['header']['present_count']}",
        ]
        return summary_lines, result

    if tool_name == "update_absence_request_status":
        updated = repo.update_absence_request_status(
            request_id=arguments["request_id"],
            status=arguments["status"],
            admin_comment=arguments.get("admin_comment"),
            reviewer=arguments.get("reviewer"),
        )
        summary_lines = [
            f"Updated request {updated['id']} to {updated['status']}.",
        ]
        return summary_lines, updated

    raise ValueError(f"Unknown tool: {tool_name}")


def _resolve_resource_payload(repo: BehaviorRepository, target: str, path: str, params: dict[str, str]) -> dict[str, Any]:
    if target == "overview" and path in {"", "summary"}:
        return repo.get_overview()

    if target == "incidents" and path in {"recent", ""}:
        limit = _clamp_int(params.get("limit"), default=20, minimum=1, maximum=200)
        grade = params.get("grade")
        return repo.recent_incidents_resource(limit=limit, grade=grade)

    if target == "student":
        esis = path or params.get("esis")
        if not esis:
            raise ValueError("Student resource requires an ESIS identifier in the path or query string.")
        include_raw = params.get("include")
        include = [item.strip() for item in include_raw.split(",") if item.strip()] if include_raw else None
        max_records = _clamp_int(params.get("max_records"), default=25, minimum=1, maximum=200)
        return repo.get_student_history(esis=esis, include=include, max_records=max_records)

    if target == "absence":
        if path in {"", "requests"}:
            after = _coerce_iso_date(params.get("after"))
            before = _coerce_iso_date(params.get("before"))
            limit = _clamp_int(params.get("limit"), default=50, minimum=1, maximum=200)
            return repo.list_absence_requests(
                status=params.get("status"),
                leave_type=params.get("leave_type"),
                teacher_email=params.get("teacher_email"),
                after=after,
                before=before,
                limit=limit,
            )
        if path == "whereabouts":
            target_date = _coerce_iso_date(params.get("date"))
            return repo.list_teacher_whereabouts(
                as_of=target_date,
                leave_type=params.get("leave_type"),
            )

    raise ValueError(f"Unsupported resource target: {target}")


def _register_classic_handlers(server: Server, repo: BehaviorRepository) -> None:
    tool_definitions = _build_tool_definitions()

    @server.list_tools()
    async def _(_: types.ListToolsRequest) -> types.ListToolsResult:
        return types.ListToolsResult(tools=tool_definitions)

    @server.call_tool()
    async def _(tool_name: str, arguments: dict[str, Any]):
        summary_lines, structured = _execute_tool(repo, tool_name, arguments)
        summary_text = "\n".join(summary_lines) if summary_lines else ""
        content: list[types.ContentBlock] = []
        if summary_text:
            content.append(types.TextContent(type="text", text=summary_text))
        return content, structured

    @server.list_resources()
    async def _(_: types.ListResourcesRequest) -> types.ListResourcesResult:
        return types.ListResourcesResult(
            resources=[
                types.Resource(
                    uri="behavior://overview/summary",
                    description="High-level behavior stats (updated on request).",
                    mimeType="application/json",
                ),
                types.Resource(
                    uri="behavior://incidents/recent",
                    description="Most recent incidents (use query parameters limit and grade to filter).",
                    mimeType="application/json",
                ),
                types.Resource(
                    uri="behavior://absence/requests",
                    description="Absence requests submitted by teachers (supports filtering via query parameters).",
                    mimeType="application/json",
                ),
                types.Resource(
                    uri="behavior://absence/whereabouts",
                    description="Teacher whereabouts summary (approved absences and present staff) for a specific date.",
                    mimeType="application/json",
                ),
            ]
        )

    @server.list_resource_templates()
    async def _() -> list[types.ResourceTemplate]:
        return [
            types.ResourceTemplate(
                uriTemplate="behavior://student/{esis}",
                description="Detailed behavior history for a specific student (ESIS).",
                mimeType="application/json",
            ),
            types.ResourceTemplate(
                uriTemplate="behavior://incidents/recent",
                description="Recent incidents with optional limit and grade filters.",
                mimeType="application/json",
            ),
            types.ResourceTemplate(
                uriTemplate="behavior://absence/requests",
                description="List absence requests (filterable by status, leave_type, teacher_email, after, before, limit).",
                mimeType="application/json",
            ),
            types.ResourceTemplate(
                uriTemplate="behavior://absence/whereabouts",
                description="Current teacher whereabouts for a date (date query parameter, optional leave_type).",
                mimeType="application/json",
            ),
        ]

    @server.read_resource()
    async def _(uri: str):
        parsed = urlparse(uri)
        if parsed.scheme != "behavior":
            raise ValueError("Unsupported resource scheme; expected behavior://")

        target = parsed.netloc
        path = parsed.path.strip("/")
        params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        payload = _resolve_resource_payload(repo, target, path, params)
        return [ReadResourceContents(content=json.dumps(payload, indent=2), mime_type="application/json")]


def build_server(db_path: Path) -> tuple[Server, BehaviorRepository]:
    repo = BehaviorRepository(db_path)
    server = Server(
        name="behavior-mcp",
        version="0.1.0",
        instructions=(
            "Provides read-only access to the behavior management database. "
            "Use the tools to search for students, inspect their history, and "
            "retrieve recent incident summaries."
        ),
        website_url="https://github.com/modelcontextprotocol",
    )
    _register_classic_handlers(server, repo)
    return server, repo


def build_fast_server(
    db_path: Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    stream_path: str = "/mcp",
    debug: bool = False,
) -> tuple[FastMCP, BehaviorRepository]:
    if FastMCP is None:
        raise RuntimeError("FastMCP transport is unavailable; ensure the mcp package extras are installed.")

    repo = BehaviorRepository(db_path)
    app = FastMCP(
        name="behavior-mcp",
        instructions=(
            "Provides read-only access to the behavior management database. "
            "Use the tools to search for students, inspect their history, and "
            "retrieve recent incident summaries."
        ),
        website_url="https://github.com/modelcontextprotocol",
        host=host,
        port=port,
        streamable_http_path=stream_path,
        debug=debug,
    )

    tool_definitions = {tool.name: tool for tool in _build_tool_definitions()}

    @app.tool(
        name="search_students",
        description=tool_definitions["search_students"].description,
        structured_output=True,
    )
    def _search_students(query: str | None = None, limit: int = 20) -> dict[str, Any]:  # noqa: ANN001
        _, structured = _execute_tool(repo, "search_students", {"query": query, "limit": limit})
        return structured

    @app.tool(
        name="list_incidents",
        description=tool_definitions["list_incidents"].description,
        structured_output=True,
    )
    def _list_incidents(  # noqa: ANN001
        esis: str | None = None,
        grade: str | None = None,
        homeroom: str | None = None,
        after: str | None = None,
        before: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        _, structured = _execute_tool(
            repo,
            "list_incidents",
            {
                "esis": esis,
                "grade": grade,
                "homeroom": homeroom,
                "after": after,
                "before": before,
                "limit": limit,
            },
        )
        return structured

    @app.tool(
        name="get_student_history",
        description=tool_definitions["get_student_history"].description,
        structured_output=True,
    )
    def _get_student_history(  # noqa: ANN001
        esis: str,
        include: Iterable[str] | None = None,
        max_records: int = 25,
    ) -> dict[str, Any]:
        include_list = list(include) if include is not None else None
        _, structured = _execute_tool(
            repo,
            "get_student_history",
            {
                "esis": esis,
                "include": include_list,
                "max_records": max_records,
            },
        )
        return structured

    @app.resource(
        "behavior://overview/summary",
        description="High-level behavior stats (updated on request).",
        mime_type="application/json",
    )
    def _overview_resource():  # noqa: ANN001
        return repo.get_overview()

    @app.resource(
        "behavior://incidents/recent",
        description="Recent incidents with optional limit and grade filters.",
        mime_type="application/json",
    )
    def _recent_incidents_resource():  # noqa: ANN001
        return repo.recent_incidents_resource()

    @app.resource(
        "behavior://student/{esis}",
        description="Detailed behavior history for a specific student (ESIS).",
        mime_type="application/json",
    )
    def _student_history_resource(esis: str):  # noqa: ANN001
        return repo.get_student_history(esis=esis, include=None, max_records=25)

    return app, repo


def _default_db_path() -> Path:
    env_path = os.environ.get("BEHAVIOR_DB_PATH")
    if env_path:
        return Path(env_path)
    return Path(__file__).resolve().parent / "behaviour.db"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Behavior MCP server.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=os.environ.get("BEHAVIOR_MCP_TRANSPORT", "stdio").lower(),
        help="Select the transport to run (stdio for local agents or http for FastMCP).",
    )
    parser.add_argument("--host", default=os.environ.get("BEHAVIOR_MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("BEHAVIOR_MCP_PORT", "8000")))
    parser.add_argument(
        "--path",
        default=os.environ.get("BEHAVIOR_MCP_STREAM_PATH", "/mcp"),
        help="HTTP path to mount the Streamable HTTP endpoint (FastMCP transport).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=os.environ.get("BEHAVIOR_MCP_DEBUG", "0").lower() in {"1", "true", "yes"},
        help="Enable FastMCP debug mode.",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    db_path = _default_db_path()

    if args.transport == "stdio":
        server, _repo = build_server(db_path)
        initialization_options = server.create_initialization_options()
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, initialization_options)
        return

    if args.transport == "http":
        app, _repo = build_fast_server(
            db_path,
            host=args.host,
            port=args.port,
            stream_path=args.path,
            debug=args.debug,
        )
        await app.run_streamable_http_async()
        return

    raise ValueError(f"Unsupported transport: {args.transport}")


if __name__ == "__main__":
    anyio.run(main)
