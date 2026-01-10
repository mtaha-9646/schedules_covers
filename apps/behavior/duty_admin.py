from __future__ import annotations



from datetime import date, datetime, timedelta
from urllib.parse import quote

from typing import Dict, Iterable, List, Optional, Set, Tuple



from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template_string,
    request,
    session,
    url_for,
)


import requests


from behaviour import Teacher
try:
    from behaviour import TeacherRole
except Exception:
    TeacherRole = None

from extensions import db



DUTY_TYPES: Tuple[str, ...] = ("morning", "dismissal")

LOCATION_CHOICES: Tuple[Tuple[str, str], ...] = (

    ("gate_1", "Gate 1"),

    ("gate_2", "Gate 2"),

    ("gate_3", "Gate 3"),

    ("gate_4", "Gate 4"),

    ("reception_gate", "Reception Gate"),

    ("g12_courtyard", "G12 Courtyard"),

    ("g6_g7_courtyard", "G6 & G7 Courtyard"),

    ("ground_floor", "Ground Floor"),

    ("first_floor", "First Floor"),

    ("second_floor", "Second Floor"),

    ("canteen", "Canteen"),

)

LOCATIONS_MAP = {value: label for value, label in LOCATION_CHOICES}



STATUS_LABELS = {

    "pending": "Pending",

    "present": "Checked in",

    "unavailable": "Excused",

}

STATUS_BADGES = {

    "pending": "bg-slate-200 text-slate-700",

    "present": "bg-emerald-100 text-emerald-700",

    "unavailable": "bg-amber-100 text-amber-700",

}


EXCLUDED_DAILY_ROLES: Set[str] = {"administrator"}





class DailyDutyAssignment(db.Model):

    __tablename__ = "daily_duty_assignments"

    __bind_key__ = "teachers_bind"



    id = db.Column(db.Integer, primary_key=True)

    assignment_date = db.Column(db.Date, nullable=False, index=True)

    duty_type = db.Column(db.String(20), nullable=False, index=True)

    location = db.Column(db.String(50), nullable=False)

    teacher_id = db.Column(db.Integer, db.ForeignKey("teachers.id"), nullable=False, index=True)

    created_by_id = db.Column(db.Integer, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)



    teacher = db.relationship("Teacher", foreign_keys=[teacher_id])

    acknowledgement = db.relationship(

        "DailyDutyAcknowledgement",

        back_populates="assignment",

        uselist=False,

        cascade="all, delete-orphan",

    )



    __table_args__ = (

        db.UniqueConstraint("assignment_date", "duty_type", "teacher_id", name="uq_daily_duty_teacher"),

    )





class DailyDutyAcknowledgement(db.Model):

    __tablename__ = "daily_duty_acknowledgements"

    __bind_key__ = "teachers_bind"



    id = db.Column(db.Integer, primary_key=True)

    assignment_id = db.Column(db.Integer, db.ForeignKey("daily_duty_assignments.id"), nullable=False, unique=True, index=True)

    teacher_id = db.Column(db.Integer, nullable=False, index=True)

    status = db.Column(db.String(20), nullable=False, default="pending")

    note = db.Column(db.Text, nullable=True)

    updated_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)



    assignment = db.relationship("DailyDutyAssignment", back_populates="acknowledgement")





duty_admin_bp = Blueprint("duty_admin_bp", __name__, url_prefix="/duty-admin")





def _week_start(target: Optional[date]) -> date:

    base = target or date.today()

    return base - timedelta(days=base.weekday())





def _week_dates(week_start: date) -> List[date]:

    return [week_start + timedelta(days=offset) for offset in range(5)]


AVAILABILITY_API_URL = "http://coveralreef.pythonanywhere.com/api/check-availability"
_DAY_CODE_MAP: Dict[str, str] = {
    "Monday": "Mo",
    "Tuesday": "Tu",
    "Wednesday": "We",
    "Thursday": "Th",
    "Friday": "Fr",
}
_DUTY_PERIODS: Dict[str, Tuple[str, ...]] = {
    "morning": ("P1",),
    "dismissal": ("P6", "P7"),
}


def _day_code_for_date(target: date) -> str:
    return _DAY_CODE_MAP.get(target.strftime("%A"), target.strftime("%a")[:2])


def _fetch_availability_records(day_code: str, period: str) -> List[Dict[str, object]]:
    if not day_code or not period:
        return []
    try:
        response = requests.get(
            AVAILABILITY_API_URL,
            params={"day": day_code, "period": period},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload.get("available") or []
        return []
    except Exception as exc:
        current_app.logger.warning("Failed to load availability (%s %s): %s", day_code, period, exc)
        return []


def _dedupe_by_email(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
    seen: Dict[str, Dict[str, object]] = {}
    for record in records:
        email = (record.get("email") or "").strip().lower()
        if email and email not in seen:
            seen[email] = record
    return list(seen.values())


def _format_api_info(record: Dict[str, object]) -> str:
    parts: List[str] = []
    for key in ("level_label", "subject", "primary_class"):
        value = record.get(key)
        if value:
            parts.append(str(value))
    return " • ".join(parts)


def _build_availability_options(
    records: List[Dict[str, object]],
    email_option_map: Dict[str, Dict[str, object]],
    assignment_summary: Dict[int, str],
) -> List[Dict[str, object]]:
    options: List[Dict[str, object]] = []
    seen: Set[int] = set()
    for record in records:
        email = (record.get("email") or "").strip().lower()
        base = email_option_map.get(email)
        if not base:
            continue
        teacher_id = base["id"]
        if teacher_id in seen:
            continue
        seen.add(teacher_id)
        option = base.copy()
        summary = assignment_summary.get(teacher_id)
        info = summary if summary and summary != "Available" else _format_api_info(record) or summary or "Available"
        option["info"] = info
        options.append(option)
    return options





def _ensure_tables() -> None:

    DailyDutyAssignment.__table__.create(bind=db.engine, checkfirst=True)

    DailyDutyAcknowledgement.__table__.create(bind=db.engine, checkfirst=True)



def _normalize_role(role_value: Optional[str]) -> str:
    return (role_value or "").strip().lower()


def _load_teacher_roles() -> Dict[int, str]:
    if TeacherRole is None:
        return {}
    try:
        rows = db.session.query(TeacherRole).all()
    except Exception:
        return {}
    return {row.teacher_id: _normalize_role(row.role) for row in rows if row.teacher_id}


def _role_allowed_for_daily(role: Optional[str], duty_type: Optional[str] = None) -> bool:
    normalized = _normalize_role(role)
    if duty_type == "dismissal":
        return True
    return normalized not in EXCLUDED_DAILY_ROLES


def _filter_options_for_duty(
    options: List[Dict[str, object]], duty_type: str, role_map: Dict[int, str]
) -> List[Dict[str, object]]:
    if duty_type == "dismissal":
        return options
    filtered: List[Dict[str, object]] = []
    for option in options:
        teacher_id = option.get("id")
        if not isinstance(teacher_id, int):
            continue
        if _role_allowed_for_daily(role_map.get(teacher_id), duty_type):
            filtered.append(option)
    return filtered


def _ensure_admin_access() -> bool:

    if session.get("is_admin") or session.get("role") == "admin":

        return True

    flash("Admin access required.", "error")

    return False





def _latest_daily_duty_date() -> Optional[date]:

    row = (

        db.session.query(DailyDutyAssignment.assignment_date)

        .order_by(DailyDutyAssignment.assignment_date.desc())

        .limit(1)

        .first()

    )

    return row[0] if row else None


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


def _build_plan_duty_email_payload(
    assignments: Iterable[DailyDutyAssignment],
    assignment_date: date,
) -> Optional[Dict[str, object]]:
    teacher_map: Dict[int, Dict[str, object]] = {}
    for assignment in assignments:
        teacher = assignment.teacher
        if not teacher or not teacher.email:
            continue
        entry = teacher_map.setdefault(
            teacher.id,
            {"teacher": teacher, "assignments": []},
        )
        duty_label = assignment.duty_type.title()
        if assignment.location:
            duty_label = f"{duty_label} at {assignment.location}"
        entry["assignments"].append(duty_label)

    if not teacher_map:
        return None

    recipients = sorted(
        {entry["teacher"].email for entry in teacher_map.values()},
        key=lambda value: value.lower(),
    )
    subject = f"Daily duty assignments for {assignment_date.strftime('%A %d %b %Y')}"
    body_lines = [
        "Hello team,",
        "",
        f"Here are the duty assignments for {assignment_date.strftime('%A, %d %B %Y')}:",
        "",
    ]
    for entry in sorted(teacher_map.values(), key=lambda item: (item["teacher"].name or "").lower()):
        teacher = entry["teacher"]
        labels = ", ".join(sorted(entry["assignments"]))
        body_lines.append(f"- {teacher.name}: {labels}")
    body_lines.extend(
        [
            "",
            "Please let us know if anything needs to be adjusted.",
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


@duty_admin_bp.route("/", methods=["GET"])

def dashboard() -> str:

    if not _ensure_admin_access():

        return redirect(url_for("behaviour_bp.behaviour_dashboard"))



    _ensure_tables()

    redirect_date = request.form.get("date")



    requested_date = request.args.get("date")



    parsed_date = None

    try:

        if requested_date:

            parsed_date = datetime.strptime(requested_date, "%Y-%m-%d").date()

    except ValueError:

        parsed_date = None



    fallback_date = _latest_daily_duty_date()

    focus_date = parsed_date or fallback_date or date.today()

    week_start = _week_start(focus_date)

    week_dates = _week_dates(week_start)

    week_end = week_dates[-1]

    if focus_date < week_start or focus_date > week_end:

        focus_date = week_start



    assignments = (

        db.session.query(DailyDutyAssignment)

        .filter(DailyDutyAssignment.assignment_date.in_(week_dates))

        .all()

    )

    role_map = _load_teacher_roles()
    assignments = [
        assignment
        for assignment in assignments
        if _role_allowed_for_daily(role_map.get(assignment.teacher_id), assignment.duty_type)
    ]

    assignment_ids = [assignment.id for assignment in assignments]

    ack_map: Dict[int, DailyDutyAcknowledgement] = {}

    if assignment_ids:

        ack_rows = (

            db.session.query(DailyDutyAcknowledgement)

            .filter(DailyDutyAcknowledgement.assignment_id.in_(assignment_ids))

            .all()

        )

        ack_map = {ack.assignment_id: ack for ack in ack_rows}

    else:

        ack_map = {}



    for assignment in assignments:

        assignment.ack = ack_map.get(assignment.id)

        assignment.display_label = _teacher_display_label(assignment.teacher)



    day_assignments = [assignment for assignment in assignments if assignment.assignment_date == focus_date]

    assignments_by_type: Dict[str, List[DailyDutyAssignment]] = {duty: [] for duty in DUTY_TYPES}

    for assignment in day_assignments:

        assignments_by_type.setdefault(assignment.duty_type, []).append(assignment)

    assigned_types_map: Dict[int, Set[str]] = {}
    for assignment in day_assignments:
        assigned_types_map.setdefault(assignment.teacher_id, set()).add(assignment.duty_type)



    teachers = db.session.query(Teacher).order_by(Teacher.name).all()

    teacher_options: List[Dict[str, object]] = []
    assignment_summary_map: Dict[int, str] = {}

    for teacher in teachers:

        entries = [

            assignment

            for assignment in day_assignments

            if assignment.teacher_id == teacher.id

        ]

        summary_parts = []

        for assignment in entries:

            ack = ack_map.get(assignment.id)

            status = (ack.status if ack and ack.status else "pending") if ack else "pending"

            status_label = STATUS_LABELS.get(status, "Pending")

            summary_parts.append(f"{assignment.duty_type.title()} ? {assignment.location} ({status_label})")

        summary = ", ".join(summary_parts)
        display_label = _teacher_display_label(teacher)

        assignment_summary_map[teacher.id] = summary if summary else "Available"
        teacher_options.append(

            {

                "id": teacher.id,

                "name": teacher.name,

                "email": teacher.email,
                "display_name": display_label,

                "info": summary if summary else "Available",

                "search": f"{display_label.lower()} {teacher.name.lower()} {(teacher.email or '').lower()}",

                "assigned_types": {assignment.duty_type for assignment in entries},

            }

        )



    email_option_map: Dict[str, Dict[str, object]] = {}
    for option in teacher_options:
        email = (option.get("email") or "").strip().lower()
        if email:
            email_option_map[email] = option

    fallback_options = [dict(option) for option in teacher_options]
    availability_options: Dict[str, List[Dict[str, object]]] = {}
    day_code = _day_code_for_date(focus_date)
    for duty in DUTY_TYPES:
        if duty == "dismissal":
            availability_options[duty] = [dict(option) for option in fallback_options]
            continue
        records: List[Dict[str, object]] = []
        periods = _DUTY_PERIODS.get(duty, ())
        for period in periods:
            records.extend(_fetch_availability_records(day_code, period))
        deduped = _dedupe_by_email(records)
        options = _build_availability_options(deduped, email_option_map, assignment_summary_map)
        options = _filter_options_for_duty(options, duty, role_map)
        if not options and deduped:
            options = _filter_options_for_duty(
                _build_availability_options(deduped, email_option_map, assignment_summary_map),
                duty,
                role_map,
            )
        if not options:
            options = _filter_options_for_duty(fallback_options, duty, role_map)
        availability_options[duty] = options

    email_payload = _build_plan_duty_email_payload(day_assignments, focus_date)

    template = """

<!DOCTYPE html>

<html lang="en">

<head>

  <meta charset="UTF-8" />

  <meta name="viewport" content="width=device-width, initial-scale=1.0" />

  <title>Daily Duty Planner</title>

  <script src="https://cdn.tailwindcss.com"></script>

  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">

</head>

<body class="bg-slate-100 min-h-screen">

  <div class="max-w-5xl mx-auto px-4 py-10 space-y-6">

    <header class="flex flex-wrap items-center justify-between gap-4">

      <div>

        <h1 class="text-3xl font-bold text-slate-900">Daily Duty Planner</h1>

        <p class="text-sm text-slate-600 mt-1">

          Week of <span class="font-semibold text-slate-900">{{ week_start.strftime('%d %b %Y') }}</span> - <span class="font-semibold text-slate-900">{{ week_end.strftime('%d %b %Y') }}</span> |

          Selected day: <span class="font-semibold text-slate-900">{{ focus_date.strftime('%A') }}</span>

        </p>
        <p class="text-xs text-slate-500">
          These assignments repeat each week until you update them for a day.
        </p>

      </div>

      <div class="flex items-center gap-2">
        {% if email_payload %}
        <a href="{{ email_payload.mailto }}" class="px-4 py-2 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 transition">
          Email Assigned Teachers
        </a>
        {% endif %}
        <a href="{{ url_for('behaviour_bp.behaviour_dashboard') }}" class="px-4 py-2 bg-slate-600 text-white text-sm font-medium rounded-lg hover:bg-slate-700 transition">
          Behaviour Dashboard
        </a>
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

      <form method="get" class="flex flex-wrap items-end gap-4">

        <div>

          <label class="block text-sm font-medium text-slate-700 mb-1">Day of the week</label>

          <select name="date" class="px-3 py-2 border border-slate-300 rounded-lg">

            {% for day in week_dates %}

            <option value="{{ day.isoformat() }}" {% if day == focus_date %}selected{% endif %}>{{ day.strftime('%A') }}</option>

            {% endfor %}

          </select>

          <p class="text-xs text-slate-500 mt-1">These assignments repeat each week until you update them.</p>

        </div>

        <button class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition">Load day</button>

      </form>

    </section>



    <section class="space-y-6 bg-white border border-slate-200 rounded-xl shadow-sm p-6">

      {% for duty_type in duty_types %}

      {% set duty_label = duty_type.title() + ' Duty' %}

      <div class="space-y-4">

        <div class="flex items-center justify-between">

          <div>

            <h2 class="text-xl font-semibold text-slate-900">{{ duty_label }}</h2>

            <p class="text-xs uppercase tracking-wide text-slate-500">Assign teachers to {{ duty_type }} duty for this day.</p>

          </div>

        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">

          <div class="space-y-2">

            {% set duty_assignments = assignments_by_type.get(duty_type, []) %}

            {% if duty_assignments %}

              {% for item in duty_assignments %}
              {% set ack = item.ack or item.acknowledgement %}
              {% set status = (ack.status if ack and ack.status else 'pending') %}
              {% set note = ack.note if ack and ack.note else '' %}
              {% set status_label = status_labels.get(status, 'Pending') %}
              {% set status_badge = status_badges.get(status, 'bg-slate-200 text-slate-700') %}
              <div class="space-y-3 bg-slate-50 border border-slate-200 px-4 py-3 rounded-lg">
                <div class="flex flex-wrap items-start justify-between gap-3">
                  <div class="space-y-1">
                    <p class="font-medium text-slate-800">{{ item.display_label }}</p>
                    <p class="text-xs text-slate-500">{{ item.teacher.email }}</p>
                    <p class="text-xs text-slate-600">Location: {{ item.location }}</p>
                  </div>
                  <div class="flex flex-col items-end gap-2">
                    <span class="inline-flex items-center px-2 py-1 rounded-full text-xs font-semibold uppercase tracking-wide {{ status_badge }}">{{ status_label }}</span>
                    {% if note %}
                    <p class="text-xs text-slate-500 text-right max-w-xs">Note: {{ note }}</p>
                    {% endif %}
                    {% if ack and ack.updated_at %}
                    <p class="text-xs text-slate-400">Updated {{ ack.updated_at.strftime('%d %b %Y %H:%M') }}</p>
                    {% endif %}
                    <form method="post" action="{{ url_for('duty_admin_bp.remove_assignment') }}">
                      <input type="hidden" name="assignment_id" value="{{ item.id }}">
                      <input type="hidden" name="date" value="{{ focus_date.isoformat() }}">
                      <button class="text-xs px-3 py-1 bg-red-100 text-red-700 rounded hover:bg-red-200 transition">Remove</button>
                    </form>
                  </div>
                </div>
                <form method="post" action="{{ url_for('duty_admin_bp.update_duty_status') }}" class="space-y-2">
                <input type="hidden" name="assignment_id" value="{{ item.id }}">
                  <input type="hidden" name="date" value="{{ focus_date.isoformat() }}">
                  <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <div>
                      <label class="block text-xs font-medium text-slate-700 mb-1">Status</label>
                      <select name="status" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-white">
                        {% for key, label in status_labels.items() %}
                        <option value="{{ key }}" {% if key == status %}selected{% endif %}>{{ label }}</option>
                        {% endfor %}
                      </select>
                    </div>
                    <div>
                      <label class="block text-xs font-medium text-slate-700 mb-1">Note {% if status == 'unavailable' %}(required){% else %}(optional){% endif %}</label>
                      <textarea name="note" rows="2" class="w-full px-3 py-2 border border-slate-300 rounded-lg" placeholder="Example: Covering arrival duty">{{ note }}</textarea>
                    </div>
                  </div>
                  <div class="flex flex-wrap items-center justify-between gap-2">
                    <button class="inline-flex items-center px-3 py-2 bg-emerald-600 text-white text-xs font-semibold rounded-lg hover:bg-emerald-700 transition">Save status</button>
                    <p class="text-xs text-slate-500">Update when a teacher checks in or is excused.</p>
                  </div>
                </form>
              </div>
              {% endfor %}

            {% else %}

              <div class="text-slate-500 text-sm italic bg-slate-50 border border-slate-200 px-4 py-3 rounded-lg">No assignments yet.</div>

            {% endif %}

          </div>

          <div>

            <form method="post" action="{{ url_for('duty_admin_bp.assign_teacher') }}" class="space-y-2">

              <input type="hidden" name="assignment_date" value="{{ focus_date.isoformat() }}">

              <input type="hidden" name="date" value="{{ focus_date.isoformat() }}">

              <input type="hidden" name="duty_type" value="{{ duty_type }}">

              <label class="block text-sm font-medium text-slate-700">Location</label>

              <select name="location" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-white" required>

                <option value="">Select location</option>

                {% for value, label in location_choices %}

                <option value="{{ value }}">{{ label }}</option>

                {% endfor %}

              </select>

              <label class="block text-sm font-medium text-slate-700">Teacher</label>

              <input type="text" class="teacher-search w-full px-3 py-2 border border-slate-300 rounded-lg" placeholder="Search teachers..." />

              {% set available_teachers = availability_options.get(duty_type, []) %}

              <select name="teacher_id" class="w-full px-3 py-2 border border-slate-300 rounded-lg bg-white duty-teacher-select" required>

                {% if available_teachers %}

                <option value="">Select teacher</option>

                {% for teacher in available_teachers %}

                <option value="{{ teacher.id }}" data-search="{{ teacher.search }}" title="{{ teacher.info }}" {% if duty_type in teacher.assigned_types %}disabled{% endif %}>

                  {{ teacher.display_name }} - {{ teacher.info }}

                </option>

                {% endfor %}

                {% else %}

                <option value="">No available teachers</option>

                {% endif %}

              </select>

              <button class="w-full px-3 py-2 bg-emerald-600 text-white rounded-lg text-sm font-medium hover:bg-emerald-700 transition">Assign</button>

            </form>

          </div>

        </div>

      </div>

      {% if not loop.last %}

      <hr class="border-slate-200">

      {% endif %}

      {% endfor %}

    </section>

  </div>

  <script>

    document.querySelectorAll('.teacher-search').forEach(function (input) {

      input.addEventListener('input', function () {

        const select = input.nextElementSibling;

        if (!select) return;

        const query = input.value.trim().toLowerCase();

        Array.from(select.options).forEach(function (option, index) {

          if (index === 0) return;

          const search = option.dataset.search || '';

          option.hidden = query && !search.includes(query);

        });

        if (query) {

          const firstVisible = Array.from(select.options).find(function (opt) { return !opt.hidden && opt.value; });

          if (firstVisible) {

            select.value = firstVisible.value;

          }

        }

      });

    });

  </script>

</body>

</html>

"""



    return render_template_string(

        template,

        week_start=week_start,

        week_end=week_end,

        focus_date=focus_date,

        week_dates=week_dates,

        duty_types=DUTY_TYPES,

        assignments_by_type=assignments_by_type,

        location_choices=LOCATION_CHOICES,

        availability_options=availability_options,
        status_labels=STATUS_LABELS,
        status_badges=STATUS_BADGES,
        email_payload=email_payload,

    )







@duty_admin_bp.route("/update-status", methods=["POST"])
def update_duty_status():
    if not _ensure_admin_access():
        return redirect(url_for("behaviour_bp.behaviour_dashboard"))

    _ensure_tables()

    date_value = request.form.get("date")

    def _redirect_to_dashboard(date_hint: Optional[str] = None):

        redirect_kwargs = {}

        if date_hint:

            redirect_kwargs["date"] = date_hint

        return redirect(url_for("duty_admin_bp.dashboard", **redirect_kwargs))

    try:
        assignment_id = int(request.form.get("assignment_id", ""))
    except ValueError:
        flash("Invalid assignment.", "error")
        return _redirect_to_dashboard(date_value)

    assignment = db.session.get(DailyDutyAssignment, assignment_id)
    if not assignment:
        flash("Assignment not found.", "error")
        return _redirect_to_dashboard(date_value)

    status = (request.form.get("status") or "pending").strip().lower()
    if status not in STATUS_LABELS:
        flash("Invalid status selected.", "error")
        return _redirect_to_dashboard(date_value)

    note = (request.form.get("note") or "").strip()
    if status == "unavailable" and not note:
        flash("Please add a brief reason when excusing a duty.", "warning")
        return _redirect_to_dashboard(date_value)

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
        flash("Duty marked as excused.", "warning")
    elif status == "present":
        flash("Duty confirmed as completed.", "success")
    else:
        flash("Duty status reset to pending.", "info")

    redirect_date = date_value or assignment.assignment_date.isoformat()
    return _redirect_to_dashboard(redirect_date)
@duty_admin_bp.route("/assign", methods=["POST"])

def assign_teacher():

    if not _ensure_admin_access():

        return redirect(url_for("behaviour_bp.behaviour_dashboard"))



    _ensure_tables()



    try:

        assignment_date = datetime.strptime(request.form.get("assignment_date", ""), "%Y-%m-%d").date()

    except (TypeError, ValueError):

        flash("Invalid date supplied.", "error")

        redirect_kwargs = {}
        if redirect_date:
            redirect_kwargs["date"] = redirect_date
        return redirect(url_for("duty_admin_bp.dashboard", **redirect_kwargs))

    redirect_date = request.form.get("date") or assignment_date.isoformat()


    duty_type = (request.form.get("duty_type") or "").strip().lower()

    if duty_type not in DUTY_TYPES:

        flash("Invalid duty type.", "error")

        return redirect(

            url_for(

                "duty_admin_bp.dashboard",

                date=redirect_date,

            )

        )



    location_key = (request.form.get("location") or "").strip().lower()

    if location_key not in LOCATIONS_MAP:

        flash("Please choose a valid location.", "error")

        return redirect(

            url_for(

                "duty_admin_bp.dashboard",

                date=redirect_date,

            )

        )

    location_label = LOCATIONS_MAP[location_key]



    try:

        teacher_id = int(request.form.get("teacher_id", ""))

    except ValueError:

        flash("Please select a teacher.", "error")

        return redirect(

            url_for(

                "duty_admin_bp.dashboard",

                date=redirect_date,

            )

        )



    teacher = db.session.get(Teacher, teacher_id)

    if not teacher:

        flash("Teacher not found.", "error")

        return redirect(

            url_for(

                "duty_admin_bp.dashboard",

                date=redirect_date,

            )

        )



    existing = (

        db.session.query(DailyDutyAssignment)

        .filter(

            DailyDutyAssignment.assignment_date == assignment_date,

            DailyDutyAssignment.duty_type == duty_type,

            DailyDutyAssignment.teacher_id == teacher_id,

        )

        .first()

    )

    if existing:

        flash(f"{teacher.name} is already assigned to {duty_type} duty on {assignment_date}.", "warning")

    else:

        assignment = DailyDutyAssignment(

            assignment_date=assignment_date,

            duty_type=duty_type,

            location=location_label,

            teacher_id=teacher_id,

            created_by_id=session.get("teacher_id"),

        )

        db.session.add(assignment)

        db.session.commit()

        flash("Duty assigned.", "success")



    return redirect(

        url_for(

            "duty_admin_bp.dashboard",

            date=redirect_date,

        )

    )





@duty_admin_bp.route("/remove", methods=["POST"])

def remove_assignment():

    if not _ensure_admin_access():

        return redirect(url_for("behaviour_bp.behaviour_dashboard"))



    _ensure_tables()

    redirect_date = request.form.get("date")

    try:

        assignment_id = int(request.form.get("assignment_id", ""))

    except ValueError:

        flash("Invalid assignment.", "error")

        return redirect(url_for("duty_admin_bp.dashboard"))



    assignment = db.session.get(DailyDutyAssignment, assignment_id)

    if not assignment:

        flash("Assignment not found.", "error")

        redirect_kwargs = {}

        if redirect_date:

            redirect_kwargs["date"] = redirect_date

        return redirect(

            url_for(

                "duty_admin_bp.dashboard",

                **redirect_kwargs,

            )

        )

    redirect_date = redirect_date or assignment.assignment_date.isoformat()



    db.session.delete(assignment)

    db.session.commit()

    flash("Assignment removed.", "success")



    redirect_kwargs = {}

    if redirect_date:

        redirect_kwargs["date"] = redirect_date

    return redirect(

        url_for(

            "duty_admin_bp.dashboard",

            **redirect_kwargs,

        )

    )
