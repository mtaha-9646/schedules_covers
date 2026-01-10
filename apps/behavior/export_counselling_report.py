"""
Generate a counseling session report PDF for a given staff member.

Usage:
    python scripts/export_counseling_report.py "Staff Name"

Optional arguments:
    --db-path   Path to the behaviour database (defaults to behaviour/behaviour.db)
    --output    Output PDF filename (defaults to counseling_<staff>.pdf in CWD)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import List

from flask import Flask
from reportlab.lib.pagesizes import A4  # type: ignore
from reportlab.lib.units import mm  # type: ignore
from reportlab.pdfgen import canvas  # type: ignore

# Local imports
from extensions import db
from behaviour import Teacher, CounselingSession


def _resolve_db_path(provided: str | None) -> str:
    if provided:
        return os.path.abspath(provided)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    return os.path.join(base_dir, "behaviour.db")


def _configure_app(db_path: str) -> Flask:
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_BINDS"] = {"teachers_bind": app.config["SQLALCHEMY_DATABASE_URI"]}
    db.init_app(app)
    return app


def _pick_teacher(name: str) -> Teacher | None:
    # Try exact, then partial matches
    exact = (
        db.session.query(Teacher)
        .filter(db.func.lower(Teacher.name) == name.strip().lower())
        .first()
    )
    if exact:
        return exact
    return (
        db.session.query(Teacher)
        .filter(Teacher.name.ilike(f"%{name.strip()}%"))
        .order_by(Teacher.name.asc())
        .first()
    )


def _focus_labels(session: CounselingSession) -> List[str]:
    focus_fields = [
        ("focus_study_skills", "Study skills"),
        ("focus_time_management", "Time management"),
        ("focus_academic_goal_setting", "Academic goals"),
        ("focus_self_esteem", "Self-esteem"),
        ("focus_decision_making", "Decision making"),
        ("focus_mindfulness_relaxation", "Mindfulness"),
        ("focus_stress_management", "Stress management"),
        ("focus_coping_anxiety", "Coping with anxiety"),
        ("focus_conflict_resolution", "Conflict resolution"),
        ("focus_immediate_crisis_support", "Crisis support"),
        ("focus_grief_loss", "Grief/Loss"),
        ("focus_support_trauma", "Trauma support"),
        ("focus_managing_anger", "Managing anger"),
        ("focus_improving_communication", "Communication"),
        ("focus_positive_habits", "Positive habits"),
        ("group_building_friendships", "Building friendships"),
        ("group_developing_empathy", "Developing empathy"),
        ("group_grief_loss_support", "Group grief support"),
        ("group_anxiety_depression_support", "Anxiety/Depression support"),
        ("group_learning_disabilities_support", "LD support"),
        ("group_stress_management", "Group stress management"),
        ("group_leadership_training", "Leadership training"),
        ("group_team_building", "Team building"),
        ("group_community_service", "Community service"),
        ("group_mediation_skills", "Mediation skills"),
        ("group_role_play_conflict", "Role-play conflict"),
        ("group_communication_strategies", "Communication strategies"),
    ]
    labels = []
    for field, label in focus_fields:
        try:
            if getattr(session, field):
                labels.append(label)
        except Exception:
            continue
    return labels


def _wrap_text(text: str, max_chars: int = 90) -> List[str]:
    words = text.split()
    lines: List[str] = []
    current: List[str] = []
    for word in words:
        if sum(len(w) for w in current) + len(current) + len(word) > max_chars:
            lines.append(" ".join(current))
            current = [word]
        else:
            current.append(word)
    if current:
        lines.append(" ".join(current))
    return lines if lines else [""]


def _render_report(staff: Teacher, sessions: List[CounselingSession], output_path: str):
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4
    margin = 20 * mm
    y = height - margin

    def draw_header():
        nonlocal y
        c.setFont("Helvetica-Bold", 16)
        c.drawString(margin, y, "Counseling Sessions Report")
        y -= 18
        c.setFont("Helvetica", 11)
        c.drawString(margin, y, f"Counselor: {staff.name} ({staff.email})")
        y -= 14
        c.drawString(margin, y, f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        y -= 22

    def draw_session(idx: int, session: CounselingSession):
        nonlocal y
        if y < 100:  # start new page
            c.showPage()
            y = height - margin
            draw_header()

        c.setFont("Helvetica-Bold", 12)
        c.drawString(margin, y, f"{idx}. {session.student_name} (ESIS: {session.esis})")
        y -= 14
        c.setFont("Helvetica", 10)
        meta = [
            f"Date: {session.session_date.strftime('%Y-%m-%d') if session.session_date else 'N/A'}",
            f"Homeroom: {session.homeroom or 'N/A'}",
            f"Duration: {session.duration_minutes or 0} min",
        ]
        c.drawString(margin, y, " | ".join(meta))
        y -= 12

        focus = _focus_labels(session)
        if focus:
            c.setFont("Helvetica", 9)
            c.drawString(margin, y, "Focus: " + "; ".join(focus))
            y -= 12

        summary = session.summary_of_progress or session.progress_toward_goals or session.follow_up_support or ""
        if summary:
            c.setFont("Helvetica", 9)
            c.drawString(margin, y, "Summary:")
            y -= 12
            for line in _wrap_text(summary, 95):
                c.drawString(margin + 12, y, line)
                y -= 11

        notes = session.counselor_observations or session.additional_support_needed or ""
        if notes:
            c.setFont("Helvetica", 9)
            c.drawString(margin, y, "Notes:")
            y -= 12
            for line in _wrap_text(notes, 95):
                c.drawString(margin + 12, y, line)
                y -= 11

        y -= 6

    draw_header()
    for i, sess in enumerate(sessions, start=1):
        draw_session(i, sess)

    c.save()


def main():
    parser = argparse.ArgumentParser(description="Export counseling sessions for a staff member to PDF.")
    parser.add_argument("staff_name", help="Full or partial staff name (matches Teacher.name)")
    parser.add_argument("--db-path", dest="db_path", help="Path to behaviour.db")
    parser.add_argument("--output", dest="output", help="Output PDF filename")
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path)
    app = _configure_app(db_path)

    with app.app_context():
        staff = _pick_teacher(args.staff_name)
        if not staff:
            sys.stderr.write(f"No teacher found matching '{args.staff_name}'\n")
            sys.exit(1)

        sessions = (
            db.session.query(CounselingSession)
            .filter(CounselingSession.created_by_teacher_id == staff.id)
            .order_by(CounselingSession.session_date.desc(), CounselingSession.id.desc())
            .all()
        )
        if not sessions:
            sys.stderr.write(f"No counseling sessions logged by '{staff.name}'.\n")
            sys.exit(1)

        safe_name = staff.name.strip().lower().replace(" ", "_")
        output_file = args.output or f"counseling_{safe_name}.pdf"
        output_path = os.path.abspath(output_file)

        _render_report(staff, sessions, output_path)
        print(f"Generated report with {len(sessions)} session(s): {output_path}")


if __name__ == "__main__":
    main()
