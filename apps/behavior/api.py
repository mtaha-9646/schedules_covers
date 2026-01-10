from datetime import datetime, time

from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from flask import Blueprint, jsonify, request

from extensions import db
from behaviour import Incident, Students, Teacher, TeacherExcuse
from leave_bp import (
    LEGACY_ABSENCE_TYPES,
    LEAVE_TYPE_EARLY,
    LEAVE_TYPE_SICK,
    TIMED_LEAVE_TYPES,
    UAE_TZ,
    VALID_LEAVE_TYPES,
)


api_bp = Blueprint('api_bp', __name__)


@api_bp.route('/api/student-incidents', methods=['GET'])
def get_student_incidents_api():
    """Return incident summary for a single student identified by ESIS."""
    esis = (request.args.get('esis') or '').strip()
    if not esis:
        return jsonify({
            "error": "Missing required query parameter: esis"
        }), 400

    records = (
        db.session.query(Incident, Teacher)
        .join(Teacher, Incident.teacher_id == Teacher.id, isouter=True)
        .filter(Incident.esis == esis)
        .order_by(Incident.date_of_incident.desc(), Incident.id.desc())
        .all()
    )

    incident_items = []
    student_name = None
    homeroom = None

    if records:
        incident, _ = records[0]
        student_name = incident.name
        homeroom = incident.homeroom
        for incident, teacher in records:
            incident_items.append({
                "id": incident.id,
                "date_of_incident": incident.date_of_incident.strftime('%Y-%m-%d %H:%M'),
                "description": incident.incident_description,
                "grade": incident.incident_grade,
                "action_taken": incident.action_taken,
                "place": incident.place_of_incident,
                "submitted_by": teacher.name if teacher else None
            })
    else:
        student = db.session.query(Students).filter(Students.esis == esis).first()
        if student:
            student_name = student.name
            homeroom = student.homeroom
        else:
            return jsonify({
                "error": "No student found for provided ESIS",
                "esis": esis
            }), 404

    return jsonify({
        "esis": esis,
        "student_name": student_name,
        "homeroom": homeroom,
        "incident_count": len(incident_items),
        "incidents": incident_items,
        "generated_at": datetime.utcnow().isoformat() + 'Z'
    })


def _format_time_value(value: time | None) -> str | None:
    return value.strftime('%H:%M') if value else None


def _reference_datetime(target_date: datetime.date, now_uae: datetime) -> datetime:
    if target_date == now_uae.date():
        return now_uae
    return datetime.combine(target_date, time(12, 0), tzinfo=UAE_TZ)


def _is_excuse_active(excuse: TeacherExcuse, reference_dt: datetime) -> bool:
    if not excuse or excuse.status != 'approved':
        return False
    ref_date = reference_dt.date()
    leave_type = (excuse.leave_type or '').lower()
    if leave_type == LEAVE_TYPE_SICK or leave_type in LEGACY_ABSENCE_TYPES:
        end_date = excuse.end_date or excuse.leave_date
        return excuse.leave_date <= ref_date <= end_date
    if leave_type in TIMED_LEAVE_TYPES:
        if excuse.leave_date != ref_date:
            return False
        start_time = excuse.start_time or time(6, 0)
        end_time = excuse.end_time or time(23, 59)
        start_dt = datetime.combine(excuse.leave_date, start_time, tzinfo=UAE_TZ)
        end_dt = datetime.combine(excuse.leave_date, end_time, tzinfo=UAE_TZ)
        if end_dt <= start_dt:
            end_dt = datetime.combine(excuse.leave_date, time(23, 59), tzinfo=UAE_TZ)
        return start_dt <= reference_dt <= end_dt
    if leave_type == LEAVE_TYPE_EARLY:
        return excuse.leave_date == ref_date
    return False


def _serialize_teacher(teacher: Teacher) -> dict:
    return {
        "teacher_id": teacher.id,
        "name": teacher.name,
        "email": teacher.email,
        "subject": teacher.subject,
        "grade": teacher.grade,
    }


def _serialize_excused_teacher(excuse: TeacherExcuse) -> dict:
    teacher = excuse.teacher
    leave_type = (excuse.leave_type or '').lower()
    return {
        "excuse_id": excuse.id,
        "teacher": _serialize_teacher(teacher) if teacher else None,
        "leave_type": leave_type,
        "type_label": excuse.type_label,
        "leave_date": excuse.leave_date.isoformat(),
        "end_date": excuse.end_date.isoformat() if excuse.end_date else None,
        "date_label": excuse.date_range_label,
        "start_time": _format_time_value(excuse.start_time),
        "end_time": _format_time_value(excuse.end_time),
        "time_label": excuse.time_range_label,
        "reason": excuse.reason,
        "status": excuse.status,
        "admin_comment": excuse.admin_comment,
        "reviewed_by": excuse.reviewed_by,
        "reviewed_at": excuse.reviewed_at.isoformat() if excuse.reviewed_at else None,
        "is_early_leave": leave_type == LEAVE_TYPE_EARLY,
        "has_time_window": bool(excuse.start_time or excuse.end_time),
        "spans_multiple_days": excuse.spans_multiple_days,
        "created_at": excuse.created_at.isoformat(),
        "updated_at": excuse.updated_at.isoformat(),
    }


@api_bp.route('/api/teacher-whereabouts', methods=['GET'])
def get_teacher_whereabouts_api():
    """Return a live snapshot of absent and present teachers for a given day."""
    now_uae = datetime.now(UAE_TZ)
    date_value = request.args.get('date')
    try:
        target_date = datetime.strptime(date_value, '%Y-%m-%d').date() if date_value else now_uae.date()
        reference_dt = _reference_datetime(target_date, now_uae)
    except ValueError:
        return jsonify({
            "error": "Invalid date format. Use YYYY-MM-DD."
        }), 400

    leave_type_hint = (request.args.get('leave_type') or '').strip().lower()
    filter_leave_type = leave_type_hint if leave_type_hint in VALID_LEAVE_TYPES else ''

    candidates = (
        db.session.query(TeacherExcuse)
        .options(joinedload(TeacherExcuse.teacher))
        .filter(TeacherExcuse.status == 'approved')
        .filter(TeacherExcuse.leave_date <= target_date)
        .filter(or_(TeacherExcuse.end_date.is_(None), TeacherExcuse.end_date >= target_date))
        .all()
    )

    active_excuses = {}
    for excuse in candidates:
        if not excuse.teacher_id or not excuse.teacher:
            continue
        if filter_leave_type and (excuse.leave_type or '').lower() != filter_leave_type:
            continue
        if not _is_excuse_active(excuse, reference_dt):
            continue
        active_excuses[excuse.teacher_id] = excuse

    teachers = db.session.query(Teacher).order_by(Teacher.name).all()
    excused_teachers = []
    present_teachers = []
    for teacher in teachers:
        if teacher.id in active_excuses:
            excused_teachers.append(_serialize_excused_teacher(active_excuses[teacher.id]))
        else:
            present_teachers.append(_serialize_teacher(teacher))

    summary = {
        "total_teachers": len(teachers),
        "excused_count": len(excused_teachers),
        "present_count": len(present_teachers),
    }

    return jsonify({
        "date": target_date.isoformat(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "reference_time": reference_dt.isoformat(),
        "timezone": "Asia/Dubai",
        "filter_leave_type": filter_leave_type or None,
        "summary": summary,
        "excused_teachers": excused_teachers,
        "present_teachers": present_teachers,
    })
