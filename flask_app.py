from __future__ import annotations

import os

import subprocess

from flask import Flask, abort, jsonify, render_template, request

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


@app.route("/")
def index():
    return render_template(
        "teachers.html",
        teachers=manager.teacher_cards,
        stats=manager.stats,
        days=DAY_LABELS,
        period_options=ORDERED_PERIODS,
    )


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
    try:
        record = covers_manager.record_leave(payload)
    except ValueError as exc:
        app.logger.warning("Invalid leave payload: %s", exc)
        return jsonify({"error": str(exc)}), 400
    except Exception:
        app.logger.exception("Failed to record leave payload")
        return jsonify({"error": "unable to process leave webhook"}), 500
    app.logger.info(
        "Recorded leave for %s on %s (request %s)",
        record["teacher"],
        record["leave_date"],
        record["request_id"],
    )
    return jsonify({"status": "recorded", "teacher": record["teacher"], "date": record["leave_date"]})


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
