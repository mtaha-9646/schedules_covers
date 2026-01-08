from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import os
import subprocess

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from typing import Any

from assignment_settings import AssignmentSettingsManager
from cover_assignment import CoverAssignmentManager
from covers_service import CoversManager
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
DEPLOY_WEBHOOK_SECRET = os.getenv("DEPLOY_WEBHOOK_SECRET")
DEPLOY_SCRIPT = os.path.join(BASE_DIR, "deploy.sh")

app = Flask(__name__)
manager = ScheduleManager(DATA_FILE)
covers_manager = CoversManager()
settings_manager = AssignmentSettingsManager()
assignment_manager = CoverAssignmentManager(manager, covers_manager, settings_manager)
assignment_manager.sync_existing_records()


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
    return render_template(
        "teachers.html",
        teachers=manager.teacher_cards,
        stats=manager.stats,
        days=DAY_LABELS,
        period_options=ORDERED_PERIODS,
        coverage_summary=coverage_summary,
    )


@app.route("/refresh-schedules")
def refresh_schedules():
    manager.reload_data()
    app.logger.info("Schedule data reloaded from %s", DATA_FILE)
    return redirect(url_for("index"))


@app.route("/teachers/<slug>")
def teacher_detail(slug: str):
    info = manager.get_schedule_for_teacher(slug)
    if not info:
        abort(404)
    return render_template(
        "teacher_detail.html",
        teacher=info["meta"],
        schedule=info["schedule"],
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
    result = manager.available_for_slot(day_code, period_label)
    return jsonify(result)


@app.route("/api/check-availability")
def check_availability():
    period_raw = request.args.get("period")
    day_raw = request.args.get("day")
    day_raw = request.args.get("day")
    date_raw = request.args.get("date")
    if not period_raw or not day_raw or not date_raw:
        return jsonify({"error": "Parameters 'date', 'day', and 'period' are all required"}), 400
    try:
        requested_date = datetime.fromisoformat(date_raw).date()
    except ValueError:
        return jsonify({"error": f"Could not parse date '{date_raw}'"}), 400
    period_label = manager.normalize_period(period_raw) or period_raw.strip()
    derived_day_code = DAY_CODE_BY_WEEKDAY.get(requested_date.weekday())
    if not derived_day_code:
        return jsonify({"error": "Date falls outside supported weekdays"}), 400
    normalized_day = manager.normalize_day(day_raw)
    if not normalized_day:
        return jsonify({"error": f"Could not parse day '{day_raw}'"}), 400
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
    available = manager.teachers_available(derived_day_code, period_label)
    return jsonify(
        {
            "date": requested_date.isoformat(),
            "day": derived_day_code,
            "day_label": DAY_LABELS.get(derived_day_code, derived_day_code),
            "period": period_label,
            "count": len(available),
            "available": available,
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
    if not payload:
        app.logger.warning("Leave webhook received without JSON payload")
        return jsonify({"error": "expected JSON payload"}), 400
    required_fields = ["email", "leave_type", "leave_start", "leave_end", "submitted_at"]
    missing = [field for field in required_fields if not payload.get(field)]
    if missing:
        msg = f"missing fields: {', '.join(missing)}"
        app.logger.warning(msg)
        return jsonify({"error": msg}), 400
    teacher_meta = manager.find_teacher_by_email(payload["email"])
    if not teacher_meta:
        app.logger.warning("No teacher found for email %s", payload["email"])
        return jsonify({"error": "teacher not found"}), 404
    payload = payload.copy()
    payload["teacher"] = teacher_meta["name"]
    payload["email"] = teacher_meta["email"]
    payload["teacher_slug"] = teacher_meta["slug"]
    payload["subject"] = teacher_meta["subject"]
    payload["level_label"] = teacher_meta["level_label"]
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


@app.route("/covers/assignments")
def covers_assignments():
    assignments = assignment_manager.get_assignments()
    date_keys = sorted(assignments.keys())
    requested_date = request.args.get("date")
    selected_date = requested_date if requested_date in assignments else None
    if not selected_date and date_keys:
        selected_date = date_keys[-1]
    selected_rows = assignments.get(selected_date, [])

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
    return render_template(
        "assignment_settings.html",
        settings=settings_manager.to_dict(),
    )


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


@app.route("/absences")
def absences_overview():
    records_by_date = covers_manager.get_all_records()
    date_keys = sorted(records_by_date.keys())
    requested_date = request.args.get("date")
    selected_date = requested_date if requested_date in records_by_date else None
    if not selected_date and date_keys:
        selected_date = date_keys[-1]
    rows = records_by_date.get(selected_date, [])
    enriched_rows: list[dict[str, Any]] = []
    for entry in rows:
        slug = entry.get("teacher_slug")
        meta = manager.get_teacher(slug) if slug else None
        level_label = meta.get("level_label") if meta else entry.get("level_label") or "General"
        grade_levels = meta.get("grade_levels") if meta else []
        grade_levels_sorted = sorted(set(grade_levels)) if grade_levels else []
        if grade_levels_sorted:
            grade_display = ", ".join(f"G{level}" for level in grade_levels_sorted)
        else:
            grade_display = level_label
        enriched_entry = entry.copy()
        enriched_entry["level_label"] = level_label
        enriched_entry["grade_display"] = grade_display
        enriched_rows.append(enriched_entry)

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
        rows=enriched_rows,
        date_options=date_options,
        selected_date=selected_date,
        selected_day_label=selected_day_label,
        assigned_ids=assigned_ids,
        assigned_count=assigned_count,
        pending_count=pending_count,
    )


@app.route("/absences/assign-missing", methods=["POST"])
def assign_missing_absences():
    count = assignment_manager.assign_missing_records()
    app.logger.info("Triggered assignment for %d pending absences", count)
    return redirect(url_for("absences_overview"))


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
