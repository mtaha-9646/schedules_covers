from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
import os
import random
import subprocess
import uuid

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from typing import Any

from assignment_settings import AssignmentSettingsManager
from cover_assignment import CoverAssignmentManager
from covers_service import CoversManager
from db import get_session, init_db
from models import DutyAssignment
from pod_duty import PodDutyManager
from schedule_service import (
    DAY_LABELS,
    DAY_ORDER,
    ORDERED_PERIODS,
    ScheduleManager,
    WEEKDAY_TO_DAY_CODE,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "schedules.xlsx")
LEAVE_WEBHOOK_SECRET = os.getenv("LEAVE_WEBHOOK_SECRET")
DUTY_ASSIGNMENT_WEBHOOK_SECRET = os.getenv("DUTY_ASSIGNMENT_WEBHOOK_SECRET")
DUTY_ASSIGNMENT_WEBHOOK_SECRET_HEADER = os.getenv(
    "DUTY_ASSIGNMENT_WEBHOOK_SECRET_HEADER",
    "X-Duty-Webhook-Secret",
)
DEPLOY_WEBHOOK_SECRET = os.getenv("DEPLOY_WEBHOOK_SECRET")
DEPLOY_SCRIPT = os.path.join(BASE_DIR, "deploy.sh")

app = Flask(__name__)
app.config["DUTY_ASSIGNMENT_WEBHOOK_URL"] = "https://behavioralreef.pythonanywhere.com/external/duty-assignments"
app.config["DUTY_ASSIGNMENT_WEBHOOK_SECRET"] = "12345"
app.config["DUTY_ASSIGNMENT_WEBHOOK_TIMEOUT"] = 5
init_db()
session_factory = get_session
manager = ScheduleManager(DATA_FILE, session_factory=session_factory)
covers_manager = CoversManager(session_factory=session_factory)
settings_manager = AssignmentSettingsManager(session_factory=session_factory)
assignment_manager = CoverAssignmentManager(
    manager,
    covers_manager,
    settings_manager,
    session_factory=session_factory,
)
assignment_manager.sync_existing_records()
pod_duty_manager = PodDutyManager(
    manager,
    session_factory=session_factory,
    covers_manager=covers_manager,
    excluded_slugs_source=assignment_manager.excluded_teacher_slugs,
)


def _to_int(value: str | None) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _prepare_leave_payload(
    payload: dict[str, Any],
    allow_unknown_teacher: bool = False,
) -> dict[str, Any]:
    if not payload or not isinstance(payload, dict):
        raise ValueError("expected JSON payload")
    missing = []
    email = payload.get("email") or payload.get("teacher_email")
    if not email:
        missing.append("email")
    for field in ("leave_type", "leave_start", "leave_end", "submitted_at"):
        if not payload.get(field):
            missing.append(field)
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")
    teacher_meta = manager.find_teacher_by_email(email)
    if not teacher_meta:
        if not allow_unknown_teacher:
            raise LookupError(f"teacher not found for email {email}")
        payload = payload.copy()
        payload["teacher"] = payload.get("teacher") or payload.get("name") or payload.get("employee") or email
        payload["email"] = email
        return payload
    payload = payload.copy()
    payload["teacher"] = teacher_meta["name"]
    payload["email"] = teacher_meta["email"]
    payload["teacher_slug"] = teacher_meta["slug"]
    payload["subject"] = teacher_meta["subject"]
    payload["level_label"] = teacher_meta["level_label"]
    return payload


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _normalize_duty_period(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        period_num = int(value)
        if period_num > 0:
            return f"P{period_num}"
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return f"P{text}"
    if text[0].lower() == "p" and text[1:].isdigit():
        return f"P{text[1:]}"
    return text


def _build_random_absences(
    count: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    if count <= 0:
        return []
    teachers = [
        teacher
        for teacher in manager.teacher_cards
        if teacher.get("email") and (teacher.get("day_count") or 0) > 0
    ]
    if not teachers:
        return []
    date_choices = [
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
        if (start_date + timedelta(days=offset)).weekday() < 5
    ]
    if not date_choices:
        return []
    records: list[dict[str, Any]] = []
    for _ in range(count):
        teacher = random.choice(teachers)
        chosen_date = random.choice(date_choices)
        records.append(
            {
                "request_id": f"test-{uuid.uuid4().hex}",
                "teacher": teacher["name"],
                "teacher_email": teacher["email"],
                "teacher_slug": teacher["slug"],
                "subject": teacher.get("subject"),
                "level_label": teacher.get("level_label"),
                "leave_type": "test",
                "leave_start": chosen_date.isoformat(),
                "leave_end": chosen_date.isoformat(),
                "status": "approved",
                "submitted_at": datetime.utcnow().isoformat(),
            }
        )
    return records
@app.route("/")
def index():
    coverage_counts = Counter()
    assignments = assignment_manager.get_assignments()
    for day_key, rows in assignments.items():
        try:
            weekday = datetime.fromisoformat(day_key).weekday()
        except ValueError:
            continue
        code = WEEKDAY_TO_DAY_CODE.get(weekday)
        if code:
            coverage_counts[code] += len(rows)
    today_code = WEEKDAY_TO_DAY_CODE.get(date.today().weekday())
    coverage_summary = [
        {
            "code": code,
            "label": DAY_LABELS.get(code, code),
            "count": coverage_counts.get(code, 0),
            "is_today": code == today_code,
        }
        for code in DAY_ORDER
    ]
    export_status = request.args.get("export_status")
    export_count = _to_int(request.args.get("export_count"))
    return render_template(
        "teachers.html",
        teachers=manager.teacher_cards,
        stats=manager.stats,
        days=DAY_LABELS,
        period_options=ORDERED_PERIODS,
        coverage_summary=coverage_summary,
        export_status=export_status,
        export_count=export_count,
    )


@app.route("/refresh-schedules")
def refresh_schedules():
    count = manager.import_from_excel()
    manager.reload_data()
    pod_duty_manager.refresh_dynamic_rows()
    app.logger.info("Schedule data imported from %s (%d rows)", DATA_FILE, count)
    return redirect(url_for("index"))


@app.route("/schedules/export", methods=["POST"])
def export_schedules():
    count = manager.export_to_excel()
    app.logger.info("Schedule data exported to %s (%d rows)", DATA_FILE, count)
    return redirect(
        url_for(
            "index",
            export_status="success" if count else "failed",
            export_count=count,
        )
    )


@app.route("/teachers/<slug>")
def teacher_detail(slug: str):
    info = manager.get_schedule_for_teacher(slug)
    if not info:
        abort(404)
    schedule_entries = manager.get_entries_for_teacher(slug)
    update_status = request.args.get("update_status")
    update_message = request.args.get("update_message")
    return render_template(
        "teacher_detail.html",
        teacher=info["meta"],
        schedule=info["schedule"],
        schedule_entries=schedule_entries,
        day_labels=DAY_LABELS,
        period_options=ORDERED_PERIODS,
        update_status=update_status,
        update_message=update_message,
    )


@app.route("/pod-duty")
def pod_duty_dashboard():
    period_raw = request.args.get("period") or "P1"
    date_raw = request.args.get("date")
    assignment_date = _parse_date(date_raw) or date.today()
    period_label = manager.normalize_period(period_raw) or period_raw.strip()
    assignments = pod_duty_manager.list_assignments(assignment_date, period_label)
    available = pod_duty_manager.available_teachers(assignment_date, period_label)
    allowed_by_pod = pod_duty_manager.allowed_slugs_by_pod(assignment_date, period_label)
    allowed_union = set()
    for slugs in allowed_by_pod.values():
        allowed_union.update(slugs)
    cached_assignments = pod_duty_manager.get_cached_assignments(assignment_date, period_label)
    cached_map = {entry["pod_label"]: entry for entry in cached_assignments}
    pods = []
    for pod in pod_duty_manager.pods:
        label = pod["label"]
        pods.append(
            {
                "label": label,
                "key": pod["key"],
                "grade": pod["grade"],
                "assigned": assignments.get(label),
                "suggested": cached_map.get(label),
            }
        )
    status = request.args.get("status")
    message = request.args.get("message")
    day_code = WEEKDAY_TO_DAY_CODE.get(assignment_date.weekday())
    day_label = DAY_LABELS.get(day_code, day_code or "")
    return render_template(
        "pod_duty.html",
        assignment_date=assignment_date,
        period_label=period_label,
        period_options=ORDERED_PERIODS,
        pods=pods,
        teachers=manager.teacher_cards,
        allowed_by_pod=allowed_by_pod,
        status=status,
        message=message,
        day_label=day_label,
        assigned_count=len(assignments),
        available_count=len(allowed_union) if allowed_by_pod else len(available),
        cached_assignments_map=cached_map,
    )


@app.route("/pod-duty/auto", methods=["POST"])
def pod_duty_auto_assign():
    date_raw = request.form.get("date")
    period_raw = request.form.get("period") or ""
    assignment_date = _parse_date(date_raw)
    if not assignment_date:
        return redirect(url_for("pod_duty_dashboard", status="failed", message="Invalid date."))
    period_label = manager.normalize_period(period_raw) or period_raw.strip()
    if not period_label:
        return redirect(url_for("pod_duty_dashboard", status="failed", message="Invalid period."))
    assignments, errors = pod_duty_manager.plan_auto_assign(assignment_date, period_label)
    if assignments:
        pod_duty_manager.cache_assignments(assignment_date, period_label, assignments)
    if errors:
        message = "; ".join(errors)
        return redirect(
            url_for(
                "pod_duty_dashboard",
                date=assignment_date.isoformat(),
                period=period_label,
                status="failed",
                message=message,
            )
        )
    summary = f"Suggested assignments for {len(assignments)} pods."
    return redirect(
        url_for(
            "pod_duty_dashboard",
            date=assignment_date.isoformat(),
            period=period_label,
            status="success",
            message=summary,
        )
    )


@app.route("/pod-duty/save", methods=["POST"])
def pod_duty_save():
    date_raw = request.form.get("date")
    period_raw = request.form.get("period") or ""
    assignment_date = _parse_date(date_raw)
    if not assignment_date:
        return redirect(url_for("pod_duty_dashboard", status="failed", message="Invalid date."))
    period_label = manager.normalize_period(period_raw) or period_raw.strip()
    if not period_label:
        return redirect(url_for("pod_duty_dashboard", status="failed", message="Invalid period."))
    selections = {}
    for pod in pod_duty_manager.pods:
        slug = (request.form.get(f"pod_{pod['key']}") or "").strip()
        if slug:
            selections[pod["label"]] = slug
    success, errors = pod_duty_manager.save_assignments(assignment_date, period_label, selections)
    if not success:
        message = "; ".join(errors) if errors else "Unable to save pod duties."
        return redirect(
            url_for(
                "pod_duty_dashboard",
                date=assignment_date.isoformat(),
                period=period_label,
                status="failed",
                message=message,
            )
        )
    return redirect(
        url_for(
            "pod_duty_dashboard",
            date=assignment_date.isoformat(),
            period=period_label,
            status="success",
            message="Pod duties updated.",
        )
    )


@app.route("/pod-duty/auto-day", methods=["POST"])
def pod_duty_auto_assign_full_day():
    date_raw = request.form.get("date")
    assignment_date = _parse_date(date_raw)
    if not assignment_date:
        return redirect(
            url_for("pod_duty_dashboard", status="failed", message="Invalid date.")
        )

    periods = [period for period in ORDERED_PERIODS if period != "Homeroom"]
    total_suggested = 0
    errors: list[str] = []
    for period in periods:
        current = pod_duty_manager.list_assignments(assignment_date, period)
        assigned_labels = set(current.keys())
        missing_pods = [
            pod["label"]
            for pod in pod_duty_manager.pods
            if pod["label"] not in assigned_labels
        ]
        if not missing_pods:
            continue
        assignments, period_errors = pod_duty_manager.plan_auto_assign(
            assignment_date,
            period,
            target_pods=missing_pods,
        )
        if assignments:
            pod_duty_manager.cache_assignments(assignment_date, period, assignments)
            total_suggested += len(assignments)
        if period_errors:
            for err in period_errors:
                errors.append(f"{period}: {err}")

    status = "success" if not errors else "failed"
    summary_parts = [
        f"Suggested {total_suggested} pod duties across {len(periods)} periods"
    ]
    if errors:
        displayed = errors[:4]
        summary_parts.append("Errors: " + "; ".join(displayed))
        if len(errors) > len(displayed):
            summary_parts.append(f"...and {len(errors) - len(displayed)} more errors")
    message = ". ".join(summary_parts)

    return redirect(
        url_for(
            "pod_duty_dashboard",
            date=assignment_date.isoformat(),
            status=status,
            message=message,
        )
    )


@app.route("/pod-duty/full-day")
def pod_duty_full_day():
    date_raw = request.args.get("date")
    assignment_date = _parse_date(date_raw) or date.today()
    periods = [period for period in ORDERED_PERIODS if period != "Homeroom"]
    rows = []
    for period in periods:
        assignments = pod_duty_manager.list_assignments(assignment_date, period)
        assignments_by_pod = []
        pods = []
        for pod in pod_duty_manager.pods:
            assigned = assignments.get(pod["label"])
            pods.append(
                {
                    "label": pod["label"],
                    "grade": pod["grade"],
                    "teacher_name": assigned.get("teacher_name") if assigned else None,
                    "teacher_email": assigned.get("teacher_email") if assigned else None,
                    "teacher_slug": assigned.get("teacher_slug") if assigned else None,
                }
            )
        assigned_count = sum(1 for pod in pods if pod["teacher_name"])
        rows.append({"period": period, "pods": pods, "assigned": assigned_count})

    day_code = WEEKDAY_TO_DAY_CODE.get(assignment_date.weekday())
    day_label = DAY_LABELS.get(day_code, day_code or "")

    return render_template(
        "pod_duty_full_day.html",
        assignment_date=assignment_date,
        day_label=day_label,
        rows=rows,
    )


@app.route("/teachers/<slug>/update", methods=["POST"])
def teacher_update(slug: str):
    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip() or None
    subject = (request.form.get("subject") or "").strip() or None
    course_total_raw = request.form.get("course_total")
    course_total = _to_int(course_total_raw) if course_total_raw else None
    new_slug = manager.update_teacher_info(slug, name, email, subject, course_total)
    if not new_slug:
        return redirect(
            url_for(
                "teacher_detail",
                slug=slug,
                update_status="failed",
                update_message="Unable to update teacher info.",
            )
        )
    return redirect(
        url_for(
            "teacher_detail",
            slug=new_slug,
            update_status="success",
            update_message="Teacher info updated.",
        )
    )


@app.route("/teachers/<slug>/schedule/add", methods=["POST"])
def schedule_entry_add(slug: str):
    day_code = request.form.get("day_code") or ""
    period_label = (request.form.get("period_label") or "").strip()
    period_raw = (request.form.get("period_raw") or "").strip()
    details = (request.form.get("details") or "").strip()
    subject = (request.form.get("subject") or "").strip() or None
    success = manager.add_schedule_entry(
        slug,
        day_code,
        period_label,
        period_raw,
        details,
        subject,
    )
    return redirect(
        url_for(
            "teacher_detail",
            slug=slug,
            update_status="success" if success else "failed",
            update_message="Schedule entry added." if success else "Unable to add schedule entry.",
        )
    )


@app.route("/teachers/<slug>/schedule/<int:entry_id>/update", methods=["POST"])
def schedule_entry_update(slug: str, entry_id: int):
    day_code = request.form.get("day_code") or ""
    period_label = (request.form.get("period_label") or "").strip()
    period_raw = (request.form.get("period_raw") or "").strip()
    details = (request.form.get("details") or "").strip()
    subject = (request.form.get("subject") or "").strip()
    subject_value = subject if subject else None
    success = manager.update_schedule_entry(
        entry_id,
        day_code,
        period_label,
        period_raw,
        details,
        subject_value,
    )
    return redirect(
        url_for(
            "teacher_detail",
            slug=slug,
            update_status="success" if success else "failed",
            update_message="Schedule entry updated." if success else "Unable to update schedule entry.",
        )
    )


@app.route("/teachers/<slug>/schedule/<int:entry_id>/delete", methods=["POST"])
def schedule_entry_delete(slug: str, entry_id: int):
    success = manager.delete_schedule_entry(entry_id)
    return redirect(
        url_for(
            "teacher_detail",
            slug=slug,
            update_status="success" if success else "failed",
            update_message="Schedule entry removed." if success else "Unable to remove schedule entry.",
        )
    )


@app.route("/api/availability")
def availability():
    period_raw = request.args.get("period")
    day_raw = request.args.get("day", "Mo")
    if not period_raw:
        return jsonify({"error": "Missing 'period' query parameter"}), 400
    period_label = manager.normalize_period(period_raw) or period_raw.strip()
    day_code = manager.normalize_day(day_raw)
    if not day_code:
        return jsonify({"error": f"Could not parse day '{day_raw}'"}), 400
    result = manager.available_for_slot_api(day_code, period_label)
    return jsonify(result)


@app.route("/api/check-availability")
def check_availability():
    period_raw = request.args.get("period")
    day_raw = request.args.get("day")
    date_raw = request.args.get("date")
    if not period_raw or not day_raw:
        return jsonify({"error": "Parameters 'day' and 'period' are required"}), 400
    period_label = manager.normalize_period(period_raw) or period_raw.strip()
    normalized_day = manager.normalize_day(day_raw)
    if not normalized_day:
        return jsonify({"error": f"Could not parse day '{day_raw}'"}), 400
    if date_raw:
        try:
            requested_date = datetime.fromisoformat(date_raw).date()
        except ValueError:
            return jsonify({"error": f"Could not parse date '{date_raw}'"}), 400
        derived_day_code = WEEKDAY_TO_DAY_CODE.get(requested_date.weekday())
        if not derived_day_code:
            return jsonify({"error": "Date falls outside supported weekdays"}), 400
        if normalized_day != derived_day_code:
            return (
                jsonify(
                    {
                        "error": "Specified day does not match provided date",
                        "date_day": derived_day_code,
                        "requested_day": normalized_day,
                    }
                ),
                400,
            )
        slot_payload = manager.available_for_slot_api(
            derived_day_code,
            period_label,
            assignment_date=requested_date,
        )
        day_code = derived_day_code
        response_date = requested_date.isoformat()
    else:
        slot_payload = manager.available_for_slot_api(normalized_day, period_label)
        day_code = normalized_day
        response_date = None
    return jsonify(
        {
            "date": response_date,
            "day": day_code,
            "day_label": DAY_LABELS.get(day_code, day_code),
            "period": period_label,
            "count": len(slot_payload["available"]),
            "available": slot_payload["available"],
            "occupied": slot_payload["occupied"],
        }
    )


@app.route("/external/leave-approvals", methods=["POST"])
def external_leave_approvals():
    if LEAVE_WEBHOOK_SECRET:
        provided_secret = request.headers.get("X-Leave-Webhook-Secret")
        if provided_secret != LEAVE_WEBHOOK_SECRET:
            app.logger.warning("Leave webhook secret missing or invalid")
            return jsonify({"error": "missing or invalid secret"}), 403
    payload = request.get_json(silent=True)
    try:
        payload = _prepare_leave_payload(payload, allow_unknown_teacher=True)
    except ValueError as exc:
        app.logger.warning("Leave webhook payload invalid: %s", exc)
        return jsonify({"error": str(exc)}), 400
    except LookupError as exc:
        app.logger.warning("Leave webhook payload missing teacher: %s", exc)
        return jsonify({"error": "teacher not found"}), 404
    try:
        record = covers_manager.record_leave(payload)
    except ValueError as exc:
        app.logger.warning("Invalid leave payload: %s", exc)
        return jsonify({"error": str(exc)}), 400
    except Exception:
        app.logger.exception("Failed to record leave payload")
        return jsonify({"error": "unable to process leave webhook"}), 500
    assignment_manager.assign_for_record(record)
    app.logger.info(
        "Recorded leave for %s on %s (request %s)",
        record["teacher"],
        record["leave_start"],
        record["request_id"],
    )
    return jsonify({"status": "recorded", "teacher": record["teacher"], "date": record["leave_start"]})


@app.route("/external/duty-assignments", methods=["POST"])
def external_duty_assignments():
    if DUTY_ASSIGNMENT_WEBHOOK_SECRET:
        provided_secret = request.headers.get(DUTY_ASSIGNMENT_WEBHOOK_SECRET_HEADER)
        if provided_secret != DUTY_ASSIGNMENT_WEBHOOK_SECRET:
            app.logger.warning("Duty assignment webhook secret missing or invalid")
            return jsonify({"error": "missing or invalid secret"}), 403
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "expected JSON payload"}), 400
    assignment_date = _parse_date(payload.get("assignment_date"))
    if not assignment_date:
        return jsonify({"error": "assignment_date is required (YYYY-MM-DD)"}), 400
    grade = (payload.get("grade") or "").strip() or None
    teachers_payload = payload.get("teachers")
    if not isinstance(teachers_payload, list):
        return jsonify({"error": "teachers must be a list"}), 400
    records: list[DutyAssignment] = []
    for teacher_entry in teachers_payload:
        if not isinstance(teacher_entry, dict):
            continue
        teacher_name = (
            teacher_entry.get("teacher")
            or teacher_entry.get("name")
            or teacher_entry.get("teacher_name")
            or ""
        ).strip()
        teacher_email = (
            teacher_entry.get("email")
            or teacher_entry.get("teacher_email")
            or ""
        ).strip()
        assignments = teacher_entry.get("assignments") or []
        if not isinstance(assignments, list):
            continue
        for assignment in assignments:
            if not isinstance(assignment, dict):
                continue
            slot_type = (assignment.get("slot_type") or "period").strip().lower()
            period_label = _normalize_duty_period(assignment.get("period") or assignment.get("period_label"))
            if slot_type == "period" and not period_label:
                continue
            pod = (assignment.get("pod") or "").strip()
            label = (assignment.get("label") or "").strip()
            if not label:
                label = "Duty assignment"
                if pod and period_label:
                    label = f"{pod} {period_label}".strip()
            record = DutyAssignment(
                assignment_date=assignment_date,
                grade=(assignment.get("grade") or grade),
                slot_type=slot_type,
                period_label=period_label,
                pod=pod,
                label=label,
                break_location=assignment.get("break_location") or assignment.get("location"),
                teacher_name=teacher_name or None,
                teacher_email=teacher_email or None,
                created_at=datetime.utcnow(),
            )
            records.append(record)
    with session_factory() as session:
        query = session.query(DutyAssignment).filter(
            DutyAssignment.assignment_date == assignment_date
        )
        if grade:
            query = query.filter(DutyAssignment.grade == grade)
        query.delete(synchronize_session=False)
        if records:
            session.bulk_save_objects(records)
        session.commit()
    return jsonify(
        {
            "status": "recorded",
            "assignment_date": assignment_date.isoformat(),
            "grade": grade,
            "count": len(records),
        }
    )


@app.route("/covers/assignments")
def covers_assignments():
    assignments = assignment_manager.get_assignments()
    date_keys = sorted(assignments.keys())
    requested_date = request.args.get("date")
    selected_date = requested_date if requested_date in assignments else None
    if not selected_date and date_keys:
        selected_date = date_keys[-1]
    selected_rows = assignments.get(selected_date, [])
    reassign_status = request.args.get("reassign_status")
    reassign_reason = request.args.get("reassign_reason")

    date_options: list[dict[str, str | None]] = []
    for date_key in date_keys:
        label: str | None = None
        try:
            code = WEEKDAY_TO_DAY_CODE.get(datetime.fromisoformat(date_key).weekday())
            if code:
                label = DAY_LABELS.get(code, code)
        except ValueError:
            pass
        date_options.append({"key": date_key, "label": label})

    selected_day_label: str | None = None
    if selected_date:
        try:
            code = WEEKDAY_TO_DAY_CODE.get(datetime.fromisoformat(selected_date).weekday())
            if code:
                selected_day_label = DAY_LABELS.get(code, code)
        except ValueError:
            selected_day_label = None

    return render_template(
        "covers_assignments.html",
        rows=selected_rows,
        date_options=date_options,
        selected_date=selected_date,
        selected_day_label=selected_day_label,
        reassign_status=reassign_status,
        reassign_reason=reassign_reason,
    )


@app.route("/assignments/settings", methods=["GET", "POST"])
def assignment_settings():
    field_names = [
        "max_covers_default",
        "max_covers_high",
        "max_covers_high_friday",
        "max_covers_middle",
        "max_covers_middle_friday",
        "highschool_full_threshold",
        "middleschool_full_threshold",
    ]
    if request.method == "POST":
        updates: dict[str, int] = {}
        for field in field_names:
            value = request.form.get(field)
            if not value:
                continue
            try:
                number = max(1, int(value))
            except ValueError:
                continue
            updates[field] = number
        if updates:
            settings_manager.update(updates)
        return redirect(url_for("assignment_settings"))
    exclusions_updated = request.args.get("exclusions_updated")
    return render_template(
        "assignment_settings.html",
        settings=settings_manager.to_dict(),
        teachers=manager.teacher_cards,
        excluded_slugs=assignment_manager.excluded_teacher_slugs(),
        exclusions_updated=exclusions_updated,
    )


@app.route("/assignments/exclusions", methods=["POST"])
def assignment_exclusions():
    slugs = request.form.getlist("excluded_slugs")
    assignment_manager.update_excluded_teachers(slugs)
    return redirect(url_for("assignment_settings", exclusions_updated="1"))


def _build_leaderboard_entries() -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    all_assignments = assignment_manager.get_assignments()
    for rows in all_assignments.values():
        for entry in rows:
            slug = entry.get("cover_slug")
            name = entry.get("cover_teacher") or slug
            if not name:
                continue
            key = slug or name
            meta = manager.get_teacher(slug) if slug else None
            if key not in stats:
                subject = meta.get("subject") if meta else entry.get("cover_subject") or "General"
                level_label = meta.get("level_label") if meta else "General"
                grade_levels = meta.get("grade_levels") if meta else []
                grade_levels_sorted = sorted(set(grade_levels)) if grade_levels else []
                if grade_levels_sorted:
                    grade_display = ", ".join(f"G{level}" for level in grade_levels_sorted)
                else:
                    grade_display = level_label
                stats[key] = {
                    "slug": slug,
                    "name": name,
                    "subject": subject,
                    "level_label": level_label,
                    "grade_levels": grade_levels or [],
                    "grade_display": grade_display,
                    "total_covers": 0,
                }
            stats[key]["total_covers"] += 1
    leaderboard = sorted(
        stats.values(),
        key=lambda item: item["total_covers"],
        reverse=True,
    )
    return leaderboard


@app.route("/leaderboards")
def leaderboards():
    entries = _build_leaderboard_entries()
    return render_template("leaderboards.html", entries=entries)


@app.route("/leaderboards/<slug>")
def leaderboard_detail(slug: str):
    if not slug:
        abort(404)
    teacher_meta = manager.get_teacher(slug)
    if not teacher_meta:
        abort(404)
    covered: dict[str, dict[str, Any]] = {}
    assignments = assignment_manager.get_assignments()
    total_covers = 0
    for date_key, rows in assignments.items():
        for entry in rows:
            if entry.get("cover_slug") != slug:
                continue
            total_covers += 1
            absent = entry.get("absent_teacher") or "Unknown"
            record = covered.setdefault(absent, {"count": 0, "dates": [], "subjects": set()})
            record["count"] += 1
            record["dates"].append(date_key)
            record["subjects"].add(entry.get("class_subject") or entry.get("subject") or "General")
    covered_list = sorted(
        [
            {
                "name": name,
                "count": meta["count"],
                "dates": sorted(set(meta["dates"])),
                "subjects": sorted(meta["subjects"]),
            }
            for name, meta in covered.items()
        ],
        key=lambda item: item["count"],
        reverse=True,
    )
    grade_levels = teacher_meta.get("grade_levels") or []
    grade_levels_sorted = sorted(set(grade_levels)) if grade_levels else []
    grade_display = (
        ", ".join(f"G{level}" for level in grade_levels_sorted)
        if grade_levels_sorted
        else teacher_meta.get("level_label", "General")
    )
    return render_template(
        "leaderboards_detail.html",
        teacher=teacher_meta,
        total_covers=total_covers,
        covered_list=covered_list,
        grade_display=grade_display,
    )


@app.route("/availability")
def availability_page():
    return render_template(
        "availability.html",
        days=DAY_LABELS,
        period_options=ORDERED_PERIODS,
    )


@app.route("/testing/assignments", methods=["GET", "POST"])
def testing_assignments():
    today = date.today()
    form_count = request.form.get("count") if request.method == "POST" else None
    count = _to_int(form_count) if form_count else 10
    start_value = request.form.get("start_date") if request.method == "POST" else None
    end_value = request.form.get("end_date") if request.method == "POST" else None
    start_date = _parse_date(start_value) or today
    if request.method == "POST" and not end_value:
        end_date = start_date
    else:
        end_date = _parse_date(end_value) or (today + timedelta(days=7))
    status = None
    message = None
    records: list[dict[str, Any]] = []
    assignments: dict[str, list[dict[str, Any]]] = {}
    if request.method == "POST":
        count = max(1, count)
        if end_date < start_date:
            status = "failed"
            message = "End date must be on or after the start date."
        else:
            records = _build_random_absences(count, start_date, end_date)
            if not records:
                status = "failed"
                message = "No eligible teachers or dates found for the test run."
            else:
                assignments = assignment_manager.simulate_assignments(records)
                status = "success"
                message = "Test assignments generated."
    flattened = []
    for date_key in sorted(assignments.keys()):
        for entry in assignments[date_key]:
            flattened.append(entry)
    per_request: dict[str, int] = {}
    for entry in flattened:
        request_id = entry.get("request_id")
        if request_id:
            per_request[request_id] = per_request.get(request_id, 0) + 1
    for record in records:
        record["assigned_count"] = per_request.get(record.get("request_id"), 0)
    unassigned_count = sum(1 for record in records if record.get("assigned_count") == 0)
    return render_template(
        "testing_assignments.html",
        status=status,
        message=message,
        records=records,
        assignments=flattened,
        total_assignments=len(flattened),
        unassigned_count=unassigned_count,
        count=count,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )


@app.route("/absences")
def absences_overview():
    records_by_date = covers_manager.get_all_records()
    date_keys = sorted(records_by_date.keys())
    requested_date = request.args.get("date")
    selected_date = requested_date if requested_date in records_by_date else None
    if not selected_date and date_keys:
        selected_date = date_keys[-1]
    rows = records_by_date.get(selected_date, [])

    sync_status = request.args.get("sync_status")
    sync_added = _to_int(request.args.get("sync_added"))
    sync_skipped = _to_int(request.args.get("sync_skipped"))
    absences_request_enabled = covers_manager.can_request_absences()
    manual_status = request.args.get("manual_status")
    manual_reason = request.args.get("manual_reason")
    clear_status = request.args.get("clear_status")
    clear_count = _to_int(request.args.get("clear_count"))

    assigned_ids = assignment_manager.assigned_request_ids()
    assigned_count = sum(1 for entry in rows if entry.get("request_id") in assigned_ids)
    pending_count = len(rows) - assigned_count

    date_options: list[dict[str, str | None]] = []
    for date_key in date_keys:
        label: str | None = None
        try:
            code = WEEKDAY_TO_DAY_CODE.get(datetime.fromisoformat(date_key).weekday())
            if code:
                label = DAY_LABELS.get(code, code)
        except ValueError:
            pass
        date_options.append({"key": date_key, "label": label})

    selected_day_label: str | None = None
    if selected_date:
        try:
            code = WEEKDAY_TO_DAY_CODE.get(datetime.fromisoformat(selected_date).weekday())
            if code:
                selected_day_label = DAY_LABELS.get(code, code)
        except ValueError:
            selected_day_label = None

    return render_template(
        "absences.html",
        rows=rows,
        date_options=date_options,
        selected_date=selected_date,
        selected_day_label=selected_day_label,
        assigned_ids=assigned_ids,
        assigned_count=assigned_count,
        pending_count=pending_count,
        sync_status=sync_status,
        sync_added=sync_added,
        sync_skipped=sync_skipped,
        absences_request_enabled=absences_request_enabled,
        manual_status=manual_status,
        manual_reason=manual_reason,
        clear_status=clear_status,
        clear_count=clear_count,
    )


@app.route("/absences/assign-missing", methods=["POST"])
def assign_missing_absences():
    count = assignment_manager.assign_missing_records()
    app.logger.info("Triggered assignment for %d pending absences", count)
    return redirect(url_for("absences_overview"))


@app.route("/absences/manual", methods=["POST"])
def manual_absence():
    email = (request.form.get("email") or "").strip()
    start_value = request.form.get("start_date")
    end_value = request.form.get("end_date") or start_value
    leave_type = (request.form.get("leave_type") or "manual").strip()
    reason = (request.form.get("reason") or "").strip()

    start_date = _parse_date(start_value)
    end_date = _parse_date(end_value)
    if not email or not start_date or not end_date:
        return redirect(
            url_for(
                "absences_overview",
                manual_status="failed",
                manual_reason="missing or invalid fields",
            )
        )
    if end_date < start_date:
        return redirect(
            url_for(
                "absences_overview",
                manual_status="failed",
                manual_reason="end date before start date",
            )
        )
    payload = {
        "request_id": f"manual-{uuid.uuid4().hex}",
        "email": email,
        "leave_type": leave_type,
        "leave_start": start_date.isoformat(),
        "leave_end": end_date.isoformat(),
        "submitted_at": datetime.utcnow().isoformat(),
        "status": "approved",
    }
    if reason:
        payload["reason"] = reason
    try:
        payload = _prepare_leave_payload(payload)
        record = covers_manager.record_leave(payload)
    except LookupError:
        return redirect(
            url_for(
                "absences_overview",
                manual_status="failed",
                manual_reason="teacher not found",
            )
        )
    except ValueError:
        return redirect(
            url_for(
                "absences_overview",
                manual_status="failed",
                manual_reason="invalid request",
            )
        )
    assignment_manager.assign_for_record(record)
    return redirect(
        url_for(
            "absences_overview",
            date=start_date.isoformat(),
            manual_status="success",
        )
    )


@app.route("/absences/request-webhook", methods=["POST"])
def request_absences_webhook():
    result = covers_manager.request_absences_webhook()
    added = 0
    skipped = 0
    records = result.get("records") or []
    if isinstance(records, list):
        for item in records:
            if not isinstance(item, dict):
                skipped += 1
                continue
            try:
                payload = _prepare_leave_payload(item, allow_unknown_teacher=True)
            except (ValueError, LookupError) as exc:
                skipped += 1
                app.logger.warning("Skipped absence payload from sync: %s", exc)
                continue
            try:
                record = covers_manager.record_leave(payload)
            except ValueError as exc:
                skipped += 1
                app.logger.warning("Invalid synced absence payload: %s", exc)
                continue
            except Exception:
                skipped += 1
                app.logger.exception("Failed to record synced absence payload")
                continue
            assignment_manager.assign_for_record(record)
            added += 1
    return redirect(
        url_for(
            "absences_overview",
            sync_status=result.get("status"),
            sync_added=added,
            sync_skipped=skipped,
        )
    )


@app.route("/absences/clear-assignments", methods=["POST"])
def clear_absence_assignments():
    request_id = (request.form.get("request_id") or "").strip()
    date_value = request.form.get("date") or ""
    removed = assignment_manager.clear_assignments_for_request(request_id)
    return redirect(
        url_for(
            "absences_overview",
            date=date_value or None,
            clear_status="success" if removed else "failed",
            clear_count=removed,
        )
    )


@app.route("/assignments/edit/<path:date_key>/<int:item_idx>", methods=["GET", "POST"])
def assignment_edit(date_key: str, item_idx: int):
    assignments = assignment_manager.get_assignments()
    rows = assignments.get(date_key)
    if not rows or item_idx < 0 or item_idx >= len(rows):
        abort(404)
    entry = rows[item_idx]
    if request.method == "POST":
        updates: dict[str, Any] = {}
        cover_slug = request.form.get("cover_slug")
        if cover_slug:
            updates["cover_slug"] = cover_slug.strip()
        for field in (
            "cover_subject",
            "status",
            "class_subject",
            "class_details",
            "class_time",
            "period_label",
            "period_raw",
        ):
            updates[field] = request.form.get(field) or ""
        success = assignment_manager.update_assignment(date_key, item_idx, updates)
        if not success:
            abort(404)
        return redirect(url_for("covers_assignments", date=date_key))
    teachers = manager.teacher_cards
    return render_template(
        "assignment_edit.html",
        entry=entry,
        teachers=teachers,
        date_key=date_key,
        item_idx=item_idx,
    )


@app.route("/assignments/reassign/<path:date_key>/<int:item_idx>", methods=["POST"])
def assignment_reassign(date_key: str, item_idx: int):
    success, reason = assignment_manager.reassign_assignment(date_key, item_idx)
    if not success:
        app.logger.warning("Reassign failed for %s #%s: %s", date_key, item_idx, reason)
    return redirect(
        url_for(
            "covers_assignments",
            date=date_key,
            reassign_status="success" if success else "failed",
            reassign_reason=None if success else reason,
        )
    )


@app.route("/internal/deploy", methods=["POST"])
def trigger_deploy():
    if not os.path.exists(DEPLOY_SCRIPT):
        app.logger.error("Deploy script missing at %s", DEPLOY_SCRIPT)
        return jsonify({"error": "deploy script missing"}), 404
    if DEPLOY_WEBHOOK_SECRET:
        provided = request.headers.get("X-Deploy-Secret")
        if provided != DEPLOY_WEBHOOK_SECRET:
            app.logger.warning("Unauthorized deploy request")
            return jsonify({"error": "unauthorized"}), 403
    app.logger.info("Deploy trigger received, running %s", DEPLOY_SCRIPT)
    try:
        completed = subprocess.run(
            ["/bin/bash", DEPLOY_SCRIPT],
            check=True,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.CalledProcessError as exc:
        output = (exc.stdout or "") + (exc.stderr or "")
        app.logger.error("Deploy script failed (%s): %s", exc.returncode, output)
        return (
            jsonify(
                {
                    "error": "deploy failed",
                    "exit_code": exc.returncode,
                    "output": output,
                }
            ),
            500,
        )
    except subprocess.TimeoutExpired:
        app.logger.error("Deploy script timed out")
        return jsonify({"error": "deploy timed out"}), 504
    app.logger.info("Deploy script completed successfully")
    return jsonify(
        {
            "status": "deployed",
            "output": completed.stdout,
            "errors": completed.stderr,
        }
    )


@app.route("/print/all")
def print_all():
    schedules = manager.all_teacher_schedules()
    return render_template("print_all.html", schedules=schedules)


if __name__ == "__main__":
    app.run(debug=True)
