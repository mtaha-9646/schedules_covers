from __future__ import annotations

import os

import subprocess

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from typing import Any

from cover_assignment import CoverAssignmentManager
from covers_service import CoversManager
from schedule_service import DAY_LABELS, ORDERED_PERIODS, ScheduleManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "schedules.xlsx")
LEAVE_WEBHOOK_SECRET = os.getenv("LEAVE_WEBHOOK_SECRET")
DEPLOY_WEBHOOK_SECRET = os.getenv("DEPLOY_WEBHOOK_SECRET")
DEPLOY_SCRIPT = os.path.join(BASE_DIR, "deploy.sh")

app = Flask(__name__)
manager = ScheduleManager(DATA_FILE)
covers_manager = CoversManager()
assignment_manager = CoverAssignmentManager(manager, covers_manager)


@app.route("/")
def index():
    return render_template(
        "teachers.html",
        teachers=manager.teacher_cards,
        stats=manager.stats,
        days=DAY_LABELS,
        period_options=ORDERED_PERIODS,
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


@app.route("/covers")
def covers_dashboard():
    return render_template("covers.html")


@app.route("/api/covers")
def covers_list():
    return jsonify(covers_manager.get_all_records())


@app.route("/covers/absent")
def covers_absent():
    records = covers_manager.get_all_records()
    return render_template("covers_absent.html", records=records)


@app.route("/covers/assignments")
def covers_assignments():
    assignments = assignment_manager.get_assignments()
    return render_template("covers_assignments.html", assignments=assignments)


@app.route("/covers/manual")
def covers_manual():
    assignments = assignment_manager.get_assignments()
    return render_template("covers_manual.html", assignments=assignments)


@app.route("/covers/manual/edit/<path:date_key>/<int:item_idx>", methods=["GET", "POST"])
def covers_manual_edit(date_key: str, item_idx: int):
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
        return redirect(url_for("covers_manual"))
    teachers = manager.teacher_cards
    return render_template(
        "covers_manual_edit.html",
        entry=entry,
        teachers=teachers,
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
