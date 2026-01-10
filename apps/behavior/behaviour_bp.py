# behaviour_bp.py
from flask import (
    Blueprint, render_template, request, jsonify,
    redirect, url_for, session, flash, current_app, send_file
)
from dateutil.parser import parse
from collections import Counter
from datetime import datetime
import re
from datetime import timedelta, date
import threading
import html
import json
import os
import uuid
from pathlib import Path

from sqlalchemy import func, inspect, text
from werkzeug.utils import secure_filename

from extensions import db
from behaviour import (
    Incident, Teacher, Students,
    Suspension, Signature, TeacherSignature,
    ParentMeeting, ParentAcknowledgment,
    StudentStatement, StaffStatement,
    SafeguardingConcern, PhoneViolationContract, CounselingSession, BehaviorContract, StudentConductPledge, GradeNotificationSetting, IncidentNotificationSetting,
    AcademicTerm
)
from ai import generate_action_plan
from ms_email import send_mail
from auth import login_required
from dynamic_models import DynamicForm, DynamicFormSubmission
from dynamic_forms_service import ensure_dynamic_form_tables

behaviour_bp = Blueprint('behaviour_bp', __name__)

INCIDENT_GRADE_MAPPING = {'C1': 'Minor', 'C2': 'Minor', 'C3': 'Major', 'C4': 'Major'}

STUDENT_STATEMENT_UPLOAD_SUBDIR = Path('uploads') / 'student_statements'
STUDENT_STATEMENT_ALLOWED_EXTENSIONS = {'.pdf', '.png', '.jpg', '.jpeg', '.doc', '.docx'}
MAX_STUDENT_STATEMENT_FILE_SIZE = 15 * 1024 * 1024  # 15MB

PARENT_MEETING_SIGNATURE_PRESETS = [
    {'key': 'parent_guardian', 'label': 'Parent / Guardian', 'role': 'Parent/Guardian', 'default': True, 'prefill_field': 'parent_name'},
    {'key': 'school_rep', 'label': 'School Representative', 'role': 'School Representative', 'default': True, 'prefill_field': 'attended_by'},
    {'key': 'student', 'label': 'Student', 'role': 'Student', 'default': False, 'prefill_field': ''},
]

STAFF_STATEMENT_SIGNATURE_PRESETS = [
    {'key': 'reporting_staff', 'label': 'Reporting Staff', 'role': 'Reporting Staff', 'default': True, 'prefill_field': 'staff_name'},
    {'key': 'slt_reviewer', 'label': 'SLT Reviewer', 'role': 'SLT Reviewer', 'default': True, 'prefill_field': 'slt_name'},
]

def _preset_locations():
    return [
        "Inside Classroom", "Outside school premises", "Bus campus", "Gym", "Corridor or staircase",
        "Bathroom/washroom", "Canteen", "Pod area"
    ]

ACADEMIC_TERM_SPECS = (
    {'slug': 'term1', 'name': 'Term 1'},
    {'slug': 'term2', 'name': 'Term 2'},
)


def _get_homerooms_grades():
    try:
        homerooms = [hr[0] for hr in db.session.query(Students.homeroom).distinct().all() if hr[0]]
        grades = sorted({re.match(r'G(\d+)', hr).group(1) for hr in homerooms if re.match(r'G(\d+)', hr)})
    except Exception:
        homerooms = []
        grades = []
    return homerooms, grades
def admin_required():
    # Accept either explicit admin flag or role stored in session
    if session.get('is_admin'):
        return True
    role = session.get('role')
    if role == 'admin':
        return True
    flash('Admin access required.', 'error')
    return False


INCIDENT_ALERT_THRESHOLD = 3


def _parse_datetime_local(dt_str: str):
    """Parse HTML datetime-local value robustly.
    Accepts standard 'YYYY-MM-DDTHH:MM' and fixes malformed variants like
    'YYYYMM-DD-DDTHH:MM' seen on some clients.
    """
    if not dt_str:
        return None
    try:
        # Standard HTML datetime-local
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M')
    except Exception:
        pass
    try:
        # Extract numeric groups: YYYY MM DD HH MM, ignoring stray separators
        m = re.match(r'^(\d{4})\D?(\d{2})\D?(\d{2})[T\s](\d{2}):(\d{2})', dt_str)
        if m:
            y, mo, d, h, mi = m.groups()
            return datetime(int(y), int(mo), int(d), int(h), int(mi))
    except Exception:
        pass
    try:
        # Fallback to dateutil if available
        return parse(dt_str)
    except Exception:
        return None

def _extract_grade_from_homeroom(homeroom: str):
    if not homeroom:
        return None
    match = re.match(r'G(\d+)', homeroom or '')
    return match.group(1) if match else None


def _parse_recipient_list(raw: str):
    if not raw:
        return []
    return [part.strip() for part in re.split(r'[;,\n]+', raw) if part and part.strip()]


def _ensure_columns(table_name: str, column_sql):
    """Ensure columns exist for legacy SQLite tables."""
    try:
        inspector = inspect(db.engine)
        existing = {col['name'] for col in inspector.get_columns(table_name)}
    except Exception as exc:
        current_app.logger.warning('Unable to inspect %s: %s', table_name, exc)
        return
    statements = [
        f"ALTER TABLE {table_name} ADD COLUMN {name} {ddl}"
        for name, ddl in column_sql.items()
        if name not in existing
    ]
    if not statements:
        return
    try:
        with db.engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
    except Exception as exc:
        current_app.logger.warning('Failed to add columns to %s: %s', table_name, exc)


def _load_required_signers(raw: str):
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _parse_required_signatures_payload(payload: str, presets):
    if not payload:
        return []
    try:
        parsed = json.loads(payload)
    except Exception:
        raise ValueError('Invalid signature payload.')
    if not isinstance(parsed, list):
        raise ValueError('Invalid signature payload.')
    preset_map = {p['key']: p for p in presets}
    required_keys = {p['key'] for p in presets if p.get('default')}
    cleaned = []
    for entry in parsed:
        key = (entry.get('key') or '').strip()
        signer_name = (entry.get('signer_name') or '').strip()
        image_data = (entry.get('image_data') or '').strip()
        if not key:
            continue
        preset = preset_map.get(key)
        if not preset:
            continue
        if not signer_name or not image_data:
            raise ValueError(f"Missing signature for {preset['label']}.")
        cleaned.append({
            'key': key,
            'label': preset['label'],
            'signer_role': preset['role'],
            'signer_name': signer_name,
            'image_data': image_data,
        })
    provided_keys = {entry['key'] for entry in cleaned}
    missing_required = [preset_map[k]['label'] for k in required_keys if k not in provided_keys]
    if missing_required:
        raise ValueError(f"Capture required signatures: {', '.join(missing_required)}.")
    return cleaned


def _create_initial_signatures(entity_type: str, entity_id: int, entries):
    if not entries:
        return
    _ensure_signatures_table()
    for entry in entries:
        sig = Signature(
            entity_type=entity_type,
            entity_id=entity_id,
            signer_name=entry['signer_name'],
            signer_role=entry['signer_role'],
            image_data=entry['image_data'],
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(sig)


def _student_statement_upload_dir() -> Path:
    root = Path(current_app.root_path)
    target = root / STUDENT_STATEMENT_UPLOAD_SUBDIR
    target.mkdir(parents=True, exist_ok=True)
    return target


def _save_student_statement_file(file_storage):
    if not file_storage or not file_storage.filename:
        raise ValueError('Select a file to upload.')
    original_name = secure_filename(file_storage.filename)
    ext = Path(original_name).suffix.lower()
    if ext not in STUDENT_STATEMENT_ALLOWED_EXTENSIONS:
        raise ValueError('Unsupported file type. Upload PDF, DOC(X), or image files.')
    file_storage.stream.seek(0, os.SEEK_END)
    size = file_storage.stream.tell()
    file_storage.stream.seek(0)
    if size <= 0:
        raise ValueError('Uploaded file is empty.')
    if size > MAX_STUDENT_STATEMENT_FILE_SIZE:
        raise ValueError('File is too large. Limit is 15MB.')
    unique_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}{ext}"
    destination = _student_statement_upload_dir() / unique_name
    file_storage.save(destination)
    rel_path = (STUDENT_STATEMENT_UPLOAD_SUBDIR / unique_name).as_posix()
    return rel_path, original_name, file_storage.mimetype or 'application/octet-stream', size


def _signature_role_counts(signatures):
    counts = {}
    for sig in signatures or []:
        role = (sig.signer_role or '').strip().lower()
        if not role:
            continue
        counts[role] = counts.get(role, 0) + 1
    return counts


# ---------------- Add Behaviour Page ----------------
@behaviour_bp.route('/behaviour/add', methods=['GET', 'POST'])
@login_required
def add_behaviour():
    # ---------------- GET REQUEST ----------------
    if request.method == 'GET':
        locations = [
            "Inside Classroom", "Outside school premises", "Bus campus", "Gym", "Corridor or staircase",
            "Bathroom/washroom", "Canteen", "Pod area"
        ]

        # Incident info (unchanged)
        incident_grades = ["C1", "C2", "C3", "C4"]
        incidents_by_grade = {
            "C1": ["Disrupting learning, not following rules", "Out of Bounds", "Incomplete or no homework or assignments",
                   "Late to school or class", "Leaving the lesson without permission, truancy",
                   "Eating or sleeping during the lesson", "Late to lesson", "Having a mobile phone",
                   "Misuse of electronic devices, headphones", "Noncompliance with school and PE uniform"],
            "C2": ["Repeated level 1 violations", "Leaving the school without permission, truancy",
                   "Fighting, threatening, and intimidating students/staff", "Violating public morals or school values",
                   "Taking pictures of students and staff without permission", "Verbal Abuse",
                   "Smoking in school or being in possession of smoking tools", "Insulting or defying teacher",
                   "Tribes and family members offensive"],
            "C3": ["Repeated level 2 offenses", "Possession of weapons", "Physical assault",
                   "Displaying or promoting materials, or media that violates the values and morals of the school",
                   "Defaming and insulting students and staff in social media", "Verbal Abuse",
                   "Smoking in school or being in possession of smoking tools", "Insulting or defying teacher",
                   "Tribes and family members offensive", "Sexual harassment within the school",
                   "Destruction/Theft/Damaging school devices", "Insulting religion or provoking religious strife",
                   "Damaging or concealment of property"],
            "C4": ["Repeated level 3 offences", "Possession of firearms", "Sexual assault within the school",
                   "Physical assault leading to injury", "Leaking exam questions or participation in this",
                   "Causing fires within school premises", "Falsifying documents",
                   "Defaming political, religious, and social symbols of the UAE",
                   "Broadcast or promote extremist or atheistic ideas and beliefs",
                   "Possession of/ Intent to supply prohibited items/substances/weapons"]
        }
        grade_by_incident = {i: g for g, lst in incidents_by_grade.items() for i in lst}
        actions_taken = [
            "Reminder: Teacher provides several verbal reminders to the student regarding the expected behavior when an issue arises.",
            "Warning: If the behavior persists after the initial reminder, I will issue a clear warning to the student.",
            "Parent Communication (School Voice Message)",
            "Contact the counsellor for advice via a message on Teams.",
            "Removal (The Pod Duty Teacher, Grade Level Lead, or the Counsellor is responsible for escorting the removed student to a different class for this lesson only).",
            "Report the case to the grade level lead. This includes providing detailed documentation of the behavior and the steps followed.",
            "Contact the PLO to inform the parent of the student's challenging behavior.",
            "Parents meeting has been held.",
            "Grade level lead reports the case to the head of section.",
            "The head of section reports the case to the behavior coordinator.",
            "Creating an intervention plan by the behavior coordinator."
        ]

        # ---------------- derive student grades from homerooms -----------------
        try:
            homerooms = [hr[0] for hr in db.session.query(Students.homeroom).distinct().all() if hr[0]]
            grades = sorted(set(re.match(r'G(\d+)', hr).group(1) for hr in homerooms if re.match(r'G(\d+)', hr)))
        except Exception as e:
            current_app.logger.error(f"Error fetching grades from homerooms: {e}")
            grades = []

        admin_teachers = []
        if session.get('is_admin') and not session.get('teacher_id'):
            try:
                admin_teachers = db.session.query(Teacher).order_by(Teacher.name.asc()).all()
            except Exception as err:
                current_app.logger.warning(f"Unable to load teachers for admin submission: {err}")

        return render_template(
            'add_behaviour.html',
            locations=locations,
            incident_grades=incident_grades,
            incidents_by_grade=incidents_by_grade,
            grade_by_incident=grade_by_incident,
            actions_taken=actions_taken,
            grades=grades,
            teacher_name=session.get('teacher_name', ''),
            admin_teachers=admin_teachers,
        )

    # ---------------- POST REQUEST ----------------
    if request.method == 'POST':
        try:
            # Get form data
            esis = request.form.get('esis')
            name = request.form.get('name')
            homeroom = request.form.get('homeroom')
            date_of_incident = _parse_datetime_local(request.form.get('date_of_incident'))
            if not date_of_incident:
                return jsonify({'success': False, 'message': 'Invalid date/time format. Please use the picker.'}), 400
            place_of_incident = request.form.get('place_of_incident')
            incident_description = request.form.get('incident_description')
            additional_notes = (request.form.get('additional_notes') or '').strip()
            incident_grade = request.form.get('incident_grade')
            action_taken = request.form.get('action_taken')
            teacher_id = session.get('teacher_id')
            if not teacher_id and session.get('is_admin'):
                admin_teacher_id = (request.form.get('admin_teacher_id') or '').strip()
                if admin_teacher_id:
                    try:
                        admin_teacher = db.session.query(Teacher).filter_by(id=int(admin_teacher_id)).first()
                    except Exception:
                        admin_teacher = None
                    if admin_teacher:
                        teacher_id = admin_teacher.id
                        session['admin_selected_teacher'] = admin_teacher.id
                if not teacher_id:
                    return jsonify({'success': False, 'message': 'Please select the reporting teacher before submitting.'}), 400

            # Validate required fields
            required_fields = {
                'esis': esis,
                'name': name,
                'homeroom': homeroom,
                'date_of_incident': date_of_incident,
                'place_of_incident': place_of_incident,
                'incident_description': incident_description,
                'action_taken': action_taken,
                'teacher_id': teacher_id,
            }
            missing = [key for key, value in required_fields.items() if not value]
            if missing:
                return jsonify({'success': False, 'message': f"Missing required fields: {', '.join(missing)}"}), 400

            # Compose description with optional notes (non-breaking change)
            full_description = incident_description
            if additional_notes:
                full_description = f"{incident_description} | Notes: {additional_notes}"

            # Save to database
            new_incident = Incident(
                esis=esis,
                name=name,
                homeroom=homeroom,
                date_of_incident=date_of_incident,
                place_of_incident=place_of_incident,
                incident_grade=incident_grade or 'C1',
                incident_description=full_description,
                action_taken=action_taken,
                teacher_id=teacher_id
            )
            db.session.add(new_incident)
            db.session.commit()

            _schedule_incident_alert(new_incident.id)
            _schedule_incident_notification(new_incident.id)

            return jsonify({'success': True, 'message': 'Incident submitted successfully'})
        except Exception as e:
            import traceback
            current_app.logger.error(traceback.format_exc())
            return jsonify({'success': False, 'message': str(e), 'trace': traceback.format_exc()}), 500
# ---------------- Dashboard ----------------
@behaviour_bp.route('/behaviour/dashboard', methods=['GET'])
@login_required
def behaviour_dashboard():
    try:
        homerooms = sorted(hr[0] for hr in db.session.query(Students.homeroom).distinct().all() if hr[0])
        # Derive student grades from homerooms like 'G11-B'
        grades = sorted({re.match(r'G(\d+)', hr).group(1) for hr in homerooms if re.match(r'G(\d+)', hr)})
    except Exception as e:
        current_app.logger.error(f"Error fetching homerooms/grades: {e}")
        homerooms = []
        grades = []
    
    dynamic_forms = []
    try:
        dynamic_forms = db.session.query(DynamicForm).order_by(DynamicForm.name).all()
    except Exception as e:
        current_app.logger.error(f"Error fetching dynamic forms: {e}")

    academic_terms_payload = []
    default_term_slug = None
    try:
        terms = _load_academic_terms()
        academic_terms_payload = [_serialize_term(term) for term in terms]
        default_term_slug = _determine_default_term_slug(terms)
    except Exception as e:
        current_app.logger.warning(f"Error loading academic terms: {e}")

    return render_template(
        'behaviour_dashboard.html',
        homerooms=homerooms,
        grades=grades,
        dynamic_forms=dynamic_forms,
        academic_terms=academic_terms_payload,
        default_term_slug=default_term_slug,
    )


@behaviour_bp.route('/admin/grade-notifications', methods=['GET', 'POST'])
@login_required
def manage_grade_notifications():
    if not admin_required():
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))

    _ensure_grade_notification_table()
    homerooms, detected_grades = _get_homerooms_grades()
    detected_set = set(detected_grades)
    existing_settings = db.session.query(GradeNotificationSetting).order_by(GradeNotificationSetting.grade).all()
    existing_grades = {setting.grade for setting in existing_settings}
    grade_options = sorted(detected_set.union(existing_grades))
    settings_payload = [
        {"grade": s.grade, "lead_email": s.lead_email or '', "recipient_emails": s.recipient_emails or ''}
        for s in existing_settings
    ]

    if request.method == 'POST':
        grade = (request.form.get('grade') or '').strip()
        lead_email = (request.form.get('lead_email') or '').strip()
        recipient_emails = (request.form.get('recipient_emails') or '').strip()

        if not grade:
            flash('Grade is required.', 'error')
            return redirect(url_for('behaviour_bp.manage_grade_notifications'))

        setting = db.session.query(GradeNotificationSetting).filter_by(grade=grade).first()
        if not lead_email and not recipient_emails:
            if setting:
                db.session.delete(setting)
                db.session.commit()
                flash(f'Notification settings cleared for Grade {grade}.', 'success')
            else:
                flash('No settings to clear for that grade.', 'info')
            return redirect(url_for('behaviour_bp.manage_grade_notifications'))

        if not setting:
            setting = GradeNotificationSetting(grade=grade)
        setting.lead_email = lead_email or None
        setting.recipient_emails = recipient_emails or None
        db.session.add(setting)
        db.session.commit()
        flash(f'Notification settings saved for Grade {grade}.', 'success')
        return redirect(url_for('behaviour_bp.manage_grade_notifications'))

    return render_template(
        'grade_notifications.html',
        grade_options=grade_options,
        settings=existing_settings,
        settings_payload=settings_payload
    )


def _ensure_suspensions_table():
    try:
        Suspension.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_incident_notification_table():
    try:
        IncidentNotificationSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_grade_notification_table():
    try:
        GradeNotificationSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_signatures_table():
    try:
        Signature.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_teacher_signatures_table():
    try:
        TeacherSignature.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_parent_meetings_table():
    try:
        ParentMeeting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass
    _ensure_columns('parent_meetings', {'required_signature_roles': 'TEXT'})


def _ensure_parent_ack_table():
    try:
        ParentAcknowledgment.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_student_statements_table():
    try:
        StudentStatement.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass
    _ensure_columns('student_statements', {
        'file_path': 'VARCHAR(500)',
        'file_name': 'VARCHAR(255)',
        'file_mime': 'VARCHAR(120)',
        'file_size': 'INTEGER'
    })


def _ensure_staff_statements_table():
    try:
        StaffStatement.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass
    _ensure_columns('staff_statements', {'required_signature_roles': 'TEXT'})


def _ensure_safeguarding_table():
    try:
        SafeguardingConcern.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_phone_violation_table():
    try:
        PhoneViolationContract.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_counseling_sessions_table():
    try:
        CounselingSession.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_behavior_contracts_table():
    try:
        BehaviorContract.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_student_conduct_pledge_table():
    try:
        StudentConductPledge.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_academic_terms_table():
    try:
        AcademicTerm.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _load_academic_terms():
    _ensure_academic_terms_table()
    term_rows = db.session.query(AcademicTerm).all()
    existing = {term.slug: term for term in term_rows}
    has_changes = False
    for spec in ACADEMIC_TERM_SPECS:
        slug = spec['slug']
        term = existing.get(slug)
        if term is None:
            term = AcademicTerm(slug=slug, name=spec['name'])
            db.session.add(term)
            existing[slug] = term
            has_changes = True
        elif term.name != spec['name']:
            term.name = spec['name']
            has_changes = True
    if has_changes:
        db.session.commit()
    return [existing[spec['slug']] for spec in ACADEMIC_TERM_SPECS if spec['slug'] in existing]


def _serialize_term(term):
    return {
        'slug': term.slug,
        'name': term.name,
        'start_date': term.start_date.strftime('%Y-%m-%d') if term.start_date else None,
        'end_date': term.end_date.strftime('%Y-%m-%d') if term.end_date else None,
    }


def _determine_default_term_slug(terms):
    if not terms:
        return None
    today = date.today()
    dated_terms = sorted((term for term in terms if term.start_date), key=lambda t: t.start_date)
    selected = None
    for term in dated_terms:
        if term.start_date <= today:
            selected = term
        else:
            break
    if selected:
        return selected.slug
    return terms[0].slug


FOCUS_LABELS = {
    "focus_study_skills": "Study skills development",
    "focus_time_management": "Time management strategies",
    "focus_academic_goal_setting": "Academic goal setting",
    "focus_self_esteem": "Self-esteem building",
    "focus_decision_making": "Decision-making skills",
    "focus_mindfulness_relaxation": "Mindfulness and relaxation techniques",
    "focus_stress_management": "Stress management techniques",
    "focus_coping_anxiety": "Coping strategies for anxiety and depression",
    "focus_conflict_resolution": "Conflict resolution skills",
    "focus_immediate_crisis_support": "Immediate crisis support",
    "focus_grief_loss": "Grief and loss counseling",
    "focus_support_trauma": "Support for trauma or abuse",
    "focus_managing_anger": "Managing anger and frustration",
    "focus_improving_communication": "Improving communication skills",
    "focus_positive_habits": "Developing positive habits",
    "group_building_friendships": "Building friendships and social interactions",
    "group_developing_empathy": "Developing empathy and cooperation",
    "group_grief_loss_support": "Group grief and loss support",
    "group_anxiety_depression_support": "Support groups for anxiety or depression",
    "group_learning_disabilities_support": "Support groups for learning disabilities",
    "group_stress_management": "Stress management group work",
    "group_leadership_training": "Leadership development",
    "group_team_building": "Team-building activities",
    "group_community_service": "Community service projects",
    "group_mediation_skills": "Mediation skills training",
    "group_role_play_conflict": "Conflict-resolution role play",
    "group_communication_strategies": "Communication strategies workshops",
}


BEHAVIOR_CONTRACT_LABELS = {
    "cons_warning": "Warning issued",
    "cons_parent_meeting": "Parent meeting scheduled",
    "cons_detention": "Detention assigned",
    "cons_referral": "Referral to leadership",
    "cons_further_action": "Additional action required",
}


def _summarize_behavior_contract(contract: BehaviorContract) -> str:
    selected = [label for attr, label in BEHAVIOR_CONTRACT_LABELS.items() if getattr(contract, attr, False)]
    if contract.cons_further_action and contract.cons_further_action_text:
        selected.append(f"Further action notes: {contract.cons_further_action_text}")
    return ", ".join(selected) if selected else "No specific consequences recorded"


def _summarize_counseling_focus(session: CounselingSession) -> str:
    active = [label for attr, label in FOCUS_LABELS.items() if getattr(session, attr, False)]
    return ", ".join(active) if active else "General counseling support"


def _prepare_action_plan_profile(esis: str) -> dict:
    student = db.session.query(Students).filter_by(esis=esis).first()
    if not student:
        raise ValueError("Student not found")

    _ensure_suspensions_table()
    _ensure_parent_meetings_table()
    _ensure_parent_ack_table()
    _ensure_student_statements_table()
    _ensure_staff_statements_table()
    _ensure_safeguarding_table()
    _ensure_phone_violation_table()
    _ensure_behavior_contracts_table()
    _ensure_counseling_sessions_table()
    _ensure_student_conduct_pledge_table()
    ensure_dynamic_form_tables()

    incident_rows = db.session.query(Incident, Teacher).join(Teacher, Incident.teacher_id == Teacher.id, isouter=True).filter(Incident.esis == esis).order_by(Incident.date_of_incident.desc()).all()
    incidents = [
        {
            "id": inc.id,
            "date": inc.date_of_incident.strftime('%Y-%m-%d %H:%M'),
            "grade": inc.incident_grade,
            "place": inc.place_of_incident,
            "teacher": teacher.name if teacher else 'Unknown',
            "action": inc.action_taken,
            "desc": inc.incident_description,
        }
        for inc, teacher in incident_rows
    ]

    summary = {
        'total': len(incidents),
        'minor': sum(1 for inc in incidents if inc['grade'] in ('C1', 'C2')),
        'major': sum(1 for inc in incidents if inc['grade'] in ('C3', 'C4')),
    }

    parent_meetings = db.session.query(ParentMeeting).filter_by(esis=esis).order_by(ParentMeeting.date.desc(), ParentMeeting.created_at.desc()).all()
    parent_meetings_full = [
        {
            'date': pm.date.strftime('%Y-%m-%d'),
            'parent_name': pm.parent_name or '',
            'attended_by': pm.attended_by or '',
            'parent_concerns': pm.parent_concerns or '',
            'school_concerns': pm.school_concerns or '',
            'solutions_parent': pm.solutions_parent or '',
            'solutions_school': pm.solutions_school or '',
            'agreed_next_steps': pm.agreed_next_steps or '',
        }
        for pm in parent_meetings
    ]

    student_statements = db.session.query(StudentStatement).filter_by(esis=esis).order_by(StudentStatement.date.desc(), StudentStatement.created_at.desc()).all()
    student_statements_full = [
        {
            'date': st.date.strftime('%Y-%m-%d'),
            'location': st.location or '',
            'statement': st.statement or '',
            'other_details': st.other_details or '',
        }
        for st in student_statements
    ]

    staff_statements = db.session.query(StaffStatement).filter_by(esis=esis).order_by(StaffStatement.created_at.desc()).all()
    staff_statements_full = [
        {
            'date': (st.date_of_statement or st.date_of_incident).strftime('%Y-%m-%d') if (st.date_of_statement or st.date_of_incident) else '',
            'staff_name': st.staff_name or '',
            'position': st.position or '',
            'details': st.details or '',
            'actions_taken': st.actions_taken or '',
        }
        for st in staff_statements
    ]

    safeguarding_rows = db.session.query(SafeguardingConcern).filter_by(esis=esis).order_by(SafeguardingConcern.report_date.desc(), SafeguardingConcern.created_at.desc()).all()
    safeguarding_full = [
        {
            'report_date': sg.report_date.strftime('%Y-%m-%d'),
            'reporting_name': sg.reporting_name,
            'reporting_role': sg.reporting_role,
            'description': sg.description or '',
            'follow_up_actions': sg.follow_up_actions or '',
        }
        for sg in safeguarding_rows
    ]

    suspensions = db.session.query(Suspension).filter_by(esis=esis).order_by(Suspension.date_of_suspension.desc(), Suspension.created_at.desc()).all()
    suspensions_full = [
        {
            'date_of_suspension': sp.date_of_suspension.strftime('%Y-%m-%d'),
            'duration': sp.duration,
            'reason': sp.reason or '',
            'behavior_plan': sp.behavior_plan or '',
            'reintegration_plan': sp.reintegration_plan or '',
        }
        for sp in suspensions
    ]

    counseling_sessions = db.session.query(CounselingSession).filter_by(esis=esis).order_by(CounselingSession.session_date.desc(), CounselingSession.created_at.desc()).all()
    counseling_sessions_full = [
        {
            'session_date': cs.session_date.strftime('%Y-%m-%d'),
            'counselors': cs.counselors or '',
            'focus_summary': _summarize_counseling_focus(cs),
            'summary_of_progress': cs.summary_of_progress or '',
            'progress_toward_goals': cs.progress_toward_goals or '',
            'future_sessions_planned': cs.future_sessions_planned or '',
            'additional_support_needed': cs.additional_support_needed or '',
        }
        for cs in counseling_sessions
    ]

    behavior_contracts = db.session.query(BehaviorContract).filter_by(esis=esis).order_by(BehaviorContract.date.desc(), BehaviorContract.created_at.desc()).all()
    behavior_contracts_full = [
        {
            'date': bc.date.strftime('%Y-%m-%d'),
            'grade': bc.grade or '',
            'consequences': _summarize_behavior_contract(bc),
        }
        for bc in behavior_contracts
    ]

    dynamic_submissions = (
        db.session.query(DynamicFormSubmission)
        .filter_by(student_esis=esis)
        .order_by(DynamicFormSubmission.created_at.desc())
        .all()
    )
    dynamic_form_titles = []
    for submission in dynamic_submissions:
        form_name = submission.form.name if submission.form else 'Custom form'
        dynamic_form_titles.append(f"{form_name} - submitted {submission.created_at.strftime('%Y-%m-%d %H:%M')}")

    profile = {
        'esis': esis,
        'student_name': student.name,
        'homeroom': student.homeroom,
        'summary': summary,
        'incidents': incidents,
        'parent_meetings_full': parent_meetings_full,
        'student_statements_full': student_statements_full,
        'staff_statements_full': staff_statements_full,
        'safeguarding_full': safeguarding_full,
        'suspensions_full': suspensions_full,
        'counseling_sessions_full': counseling_sessions_full,
        'behavior_contracts_full': behavior_contracts_full,
        'dynamic_form_titles': dynamic_form_titles,
    }
    return profile


def _parse_duration_days(duration: str) -> int:
    try:
        if not duration:
            return 1
        m = re.search(r"(\d+)", duration)
        if m:
            n = int(m.group(1))
            return max(1, n)
    except Exception:
        pass
    return 1


# ---------------- Suspensions API ----------------
@behaviour_bp.route('/api/suspensions', methods=['GET'])
@login_required
def get_suspensions_api():
    _ensure_suspensions_table()
    page = request.args.get('page', 1, type=int)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    student_grade = request.args.get('student_grade')  # numeric e.g., '6'
    homeroom = request.args.get('homeroom')
    esis = request.args.get('esis')
    student_name = request.args.get('student_name')

    query = db.session.query(Suspension)

    if start_date:
        try:
            query = query.filter(Suspension.date_of_suspension >= datetime.strptime(start_date, '%Y-%m-%d').date())
        except Exception:
            pass
    if end_date:
        try:
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            query = query.filter(Suspension.date_of_suspension <= end)
        except Exception:
            pass
    if student_grade:
        query = query.filter(Suspension.grade_class.like(f'G{student_grade}%'))
    if homeroom:
        query = query.filter(Suspension.grade_class == homeroom)
    if esis:
        query = query.filter(Suspension.esis == esis)
    if student_name:
        like = f"%{student_name.strip()}%"
        query = query.filter(Suspension.student_name.ilike(like))

    all_rows = query.order_by(Suspension.date_of_suspension.desc(), Suspension.created_at.desc()).all()
    total = len(all_rows)

    per_page = 10
    start = (page - 1) * per_page
    end = start + per_page
    page_rows = all_rows[start:end]

    suspensions_list = [s.to_dict() for s in page_rows]

    return jsonify({
        'suspensions': suspensions_list,
        'total': total,
        'pages': (total + per_page - 1) // per_page,
        'current_page': page
    })


@behaviour_bp.route('/api/suspensions/summary', methods=['GET'])
@login_required
def get_suspensions_summary_api():
    _ensure_suspensions_table()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    student_grade = request.args.get('student_grade')
    homeroom = request.args.get('homeroom')
    esis = request.args.get('esis')
    student_name = request.args.get('student_name')

    query = db.session.query(Suspension)
    if start_date:
        try:
            query = query.filter(Suspension.date_of_suspension >= datetime.strptime(start_date, '%Y-%m-%d').date())
        except Exception:
            pass
    if end_date:
        try:
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            query = query.filter(Suspension.date_of_suspension <= end)
        except Exception:
            pass
    if student_grade:
        query = query.filter(Suspension.grade_class.like(f'G{student_grade}%'))
    if homeroom:
        query = query.filter(Suspension.grade_class == homeroom)
    if esis:
        query = query.filter(Suspension.esis == esis)
    if student_name:
        like = f"%{student_name.strip()}%"
        query = query.filter(Suspension.student_name.ilike(like))

    rows = query.all()
    # Chart by student grade (prefix of grade_class)
    from collections import Counter
    grade_counts = Counter()
    for s in rows:
        label = ''
        try:
            m = re.match(r'G(\d+)', s.grade_class or '')
            label = f"G{m.group(1)}" if m else (s.grade_class or '')
        except Exception:
            label = s.grade_class or ''
        grade_counts[label] += 1
    labels = sorted([k for k in grade_counts.keys() if k])
    grade_chart = {
        'labels': labels,
        'counts': [grade_counts.get(k, 0) for k in labels]
    }
    # Time chart (by date)
    time_counts = Counter()
    for s in rows:
        try:
            time_counts[s.date_of_suspension.strftime('%Y-%m-%d')] += 1
        except Exception:
            continue
    dates_sorted = sorted(time_counts.keys())
    time_chart = {
        'labels': dates_sorted,
        'counts': [time_counts[d] for d in dates_sorted]
    }
    return jsonify({
        'total_suspensions': len(rows),
        'grade_chart': grade_chart,
        'time_chart': time_chart
    })


@behaviour_bp.route('/api/suspensions/current', methods=['GET'])
@login_required
def get_current_suspensions_api():
    _ensure_suspensions_table()
    today = date.today()
    rows = db.session.query(Suspension).all()
    current = []
    for s in rows:
        try:
            days = _parse_duration_days(s.duration or '')
            end_date = s.date_of_suspension + timedelta(days=max(1, days) - 1)
            if s.date_of_suspension <= today <= end_date:
                current.append({
                    'esis': s.esis,
                    'student_name': s.student_name,
                    'grade_class': s.grade_class or '',
                    'start_date': s.date_of_suspension.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'duration': s.duration or ''
                })
        except Exception:
            continue
    # Sort by start date desc then name
    current.sort(key=lambda x: (x['start_date'], x['student_name']))
    return jsonify(current)


@behaviour_bp.route('/suspensions/export', methods=['GET'])
@login_required
def export_suspensions():
    # Only admins can export
    if not admin_required():
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_suspensions_table()

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    student_grade = request.args.get('student_grade')
    homeroom = request.args.get('homeroom')
    esis = request.args.get('esis')
    student_name = request.args.get('student_name')

    q = db.session.query(Suspension)
    if start_date:
        try:
            q = q.filter(Suspension.date_of_suspension >= datetime.strptime(start_date, '%Y-%m-%d').date())
        except Exception:
            pass
    if end_date:
        try:
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            q = q.filter(Suspension.date_of_suspension <= end)
        except Exception:
            pass
    if student_grade:
        q = q.filter(Suspension.grade_class.like(f'G{student_grade}%'))
    if homeroom:
        q = q.filter(Suspension.grade_class == homeroom)
    if esis:
        q = q.filter(Suspension.esis == esis)
    if student_name:
        like = f"%{student_name.strip()}%"
        q = q.filter(Suspension.student_name.ilike(like))

    rows = q.order_by(Suspension.date_of_suspension.desc()).all()

    import io, csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'ID','ESIS','Student Name','Class','Date','Duration','Reason','Incident Details','Parent Contacted','Parent Meeting','Behavior Plan','Assigned Staff','Reintegration Plan','Notes'
    ])
    for s in rows:
        writer.writerow([
            s.id, s.esis, s.student_name, s.grade_class or '',
            s.date_of_suspension.strftime('%Y-%m-%d') if s.date_of_suspension else '',
            s.duration or '', s.reason or '', s.incident_details or '',
            'Yes' if s.parent_contacted else 'No', 'Yes' if s.parent_meeting else 'No',
            s.behavior_plan or '', s.assigned_staff or '', s.reintegration_plan or '', s.notes or ''
        ])

    mem = io.BytesIO()
    mem.write(output.getvalue().encode('utf-8-sig'))
    mem.seek(0)
    return send_file(mem, mimetype='text/csv', as_attachment=True, download_name='suspensions_export.csv')


# ---------------- Signatures API ----------------
@behaviour_bp.route('/api/signatures', methods=['GET'])
@login_required
def get_signatures_api():
    _ensure_signatures_table()
    entity_type = (request.args.get('entity_type') or '').lower()
    entity_id = request.args.get('entity_id', type=int)
    if entity_type not in (
        'incident', 'suspension', 'parent_meeting', 'parent_ack', 'student_statement', 'staff_statement', 'safeguarding', 'phone_violation', 'counseling_session', 'behavior_contract', 'student_conduct_pledge'
    ) or not entity_id:
        return jsonify({'error': 'Invalid parameters'}), 400
    rows = db.session.query(Signature).filter_by(entity_type=entity_type, entity_id=entity_id).order_by(Signature.created_at.asc()).all()
    return jsonify([r.to_dict() for r in rows])


@behaviour_bp.route('/api/signatures', methods=['POST'])
@login_required
def add_signature_api():
    _ensure_signatures_table()
    data = request.get_json(silent=True) or {}
    entity_type = (data.get('entity_type') or '').lower()
    entity_id = data.get('entity_id')
    signer_name = (data.get('signer_name') or '').strip()
    signer_role = (data.get('signer_role') or '').strip()
    image_data = (data.get('image_data') or '').strip()

    if entity_type not in (
        'incident', 'suspension', 'parent_meeting', 'parent_ack', 'student_statement', 'staff_statement', 'safeguarding', 'phone_violation', 'counseling_session', 'behavior_contract', 'student_conduct_pledge'
    ) or not entity_id:
        return jsonify({'error': 'Invalid entity'}), 400
    if not signer_name or not signer_role or not image_data:
        return jsonify({'error': 'Missing fields'}), 400

    # Validate entity exists
    if entity_type == 'incident':
        exists = db.session.query(Incident.id).filter_by(id=entity_id).first()
    elif entity_type == 'suspension':
        exists = db.session.query(Suspension.id).filter_by(id=entity_id).first()
    elif entity_type == 'parent_meeting':
        _ensure_parent_meetings_table()
        exists = db.session.query(ParentMeeting.id).filter_by(id=entity_id).first()
    elif entity_type == 'parent_ack':
        _ensure_parent_ack_table()
        exists = db.session.query(ParentAcknowledgment.id).filter_by(id=entity_id).first()
    elif entity_type == 'student_statement':
        _ensure_student_statements_table()
        exists = db.session.query(StudentStatement.id).filter_by(id=entity_id).first()
    elif entity_type == 'staff_statement':
        _ensure_staff_statements_table()
        exists = db.session.query(StaffStatement.id).filter_by(id=entity_id).first()
    elif entity_type == 'safeguarding':
        _ensure_safeguarding_table()
        exists = db.session.query(SafeguardingConcern.id).filter_by(id=entity_id).first()
    elif entity_type == 'phone_violation':
        _ensure_phone_violation_table()
        exists = db.session.query(PhoneViolationContract.id).filter_by(id=entity_id).first()
    elif entity_type == 'counseling_session':
        _ensure_counseling_sessions_table()
        exists = db.session.query(CounselingSession.id).filter_by(id=entity_id).first()
    elif entity_type == 'behavior_contract':
        _ensure_behavior_contracts_table()
        exists = db.session.query(BehaviorContract.id).filter_by(id=entity_id).first()
    else: # student_conduct_pledge
        _ensure_student_conduct_pledge_table()
        exists = db.session.query(StudentConductPledge.id).filter_by(id=entity_id).first()

    if not exists:
        return jsonify({'error': 'Entity not found'}), 404

    # Teachers may only sign as themselves for incidents/suspensions
    if entity_type in ('incident', 'suspension'):
        if not session.get('is_admin'):
            teacher_name = (session.get('teacher_name') or '').strip()
            if not teacher_name or signer_name.lower() != teacher_name.lower():
                return jsonify({'error': 'Teachers may only sign using their own name'}), 403
            # Teachers cannot choose role; force to 'Teacher'
            signer_role = 'Teacher'

    # Normalize image: allow full data URL or raw base64
    if image_data.startswith('data:'):
        prefix = 'data:image/png;base64,'
        if image_data.startswith(prefix):
            base64_part = image_data[len(prefix):]
        else:
            base64_part = image_data.split(',', 1)[-1]
    else:
        base64_part = image_data

    sig = Signature(
        entity_type=entity_type,
        entity_id=int(entity_id),
        signer_name=signer_name,
        signer_role=signer_role,
        image_data=base64_part,
        created_by_teacher_id=session.get('teacher_id')
    )
    db.session.add(sig)
    db.session.commit()
    return jsonify(sig.to_dict())


@behaviour_bp.route('/api/my-signature', methods=['GET'])
@login_required
def get_my_signature():
    if session.get('is_admin') and not session.get('teacher_id'):
        # super admin without teacher context has no personal signature
        return jsonify({}), 404
    _ensure_teacher_signatures_table()
    tid = session.get('teacher_id')
    if not tid:
        return jsonify({}), 404
    row = db.session.query(TeacherSignature).filter_by(teacher_id=tid).first()
    if not row:
        return jsonify({}), 404
    return jsonify(row.to_dict())


@behaviour_bp.route('/api/my-signature', methods=['POST'])
@login_required
def save_my_signature():
    _ensure_teacher_signatures_table()
    tid = session.get('teacher_id')
    if not tid:
        return jsonify({'error': 'Only teachers can save a personal signature'}), 403
    data = request.get_json(silent=True) or {}
    image_data = (data.get('image_data') or '').strip()
    if not image_data:
        return jsonify({'error': 'Missing image_data'}), 400
    if image_data.startswith('data:'):
        image_data = image_data.split(',', 1)[-1]
    row = db.session.query(TeacherSignature).filter_by(teacher_id=tid).first()
    if row:
        row.image_data = image_data
    else:
        db.session.add(TeacherSignature(teacher_id=tid, image_data=image_data))
    db.session.commit()
    return jsonify({'ok': True})


# ---------------- Add Suspension (Admin only) ----------------
@behaviour_bp.route('/suspensions/add', methods=['GET', 'POST'])
@login_required
def add_suspension():
    if not admin_required():
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_suspensions_table()

    if request.method == 'GET':
        # Reuse homerooms and grades for simple filtering/selects
        try:
            homerooms = sorted(hr[0] for hr in db.session.query(Students.homeroom).distinct().all() if hr[0])
            grades = sorted({re.match(r'G(\d+)', hr).group(1) for hr in homerooms if re.match(r'G(\d+)', hr)})
        except Exception:
            homerooms = []
            grades = []
        return render_template('add_suspension.html', homerooms=homerooms, grades=grades)

    # POST
    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade_class = (request.form.get('grade_class') or '').strip()
        date_str = (request.form.get('date_of_suspension') or '').strip()
        duration = (request.form.get('duration') or '').strip()
        reason = (request.form.get('reason') or '').strip()
        incident_details = (request.form.get('incident_details') or '').strip()
        parent_contacted = request.form.get('parent_contacted') is not None
        parent_meeting = request.form.get('parent_meeting') is not None
        behavior_plan = (request.form.get('behavior_plan') or '').strip()
        assigned_staff = (request.form.get('assigned_staff') or '').strip()
        reintegration_plan = (request.form.get('reintegration_plan') or '').strip()
        notes = (request.form.get('notes') or '').strip()

        # All fields required per request, including both checkboxes checked
        if not all([esis, student_name, grade_class, date_str, duration, reason, incident_details, behavior_plan, assigned_staff, reintegration_plan, notes]) or not (parent_contacted and parent_meeting):
            flash('All fields are required. Please complete the entire form.', 'error')
            return redirect(url_for('behaviour_bp.add_suspension'))

        try:
            date_of_suspension = datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            flash('Invalid date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('behaviour_bp.add_suspension'))

        created_by_teacher_id = session.get('teacher_id') if session.get('is_admin') else None

        s = Suspension(
            esis=esis,
            student_name=student_name,
            grade_class=grade_class,
            date_of_suspension=date_of_suspension,
            duration=duration,
            reason=reason,
            incident_details=incident_details,
            parent_contacted=parent_contacted,
            parent_meeting=parent_meeting,
            behavior_plan=behavior_plan,
            assigned_staff=assigned_staff,
            reintegration_plan=reintegration_plan,
            notes=notes,
            created_by_teacher_id=created_by_teacher_id
        )
        db.session.add(s)
        db.session.commit()
        flash('Suspension recorded successfully.', 'success')
        return redirect(url_for('behaviour_bp.behaviour_dashboard') + '#tab=suspensions')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to add suspension: {e}', 'error')
        return redirect(url_for('behaviour_bp.add_suspension'))


@behaviour_bp.route('/suspensions/<int:suspension_id>', methods=['GET'])
@login_required
def view_suspension(suspension_id):
    _ensure_suspensions_table()
    s = db.session.query(Suspension).filter_by(id=suspension_id).first()
    if not s:
        flash('Suspension not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='suspension', entity_id=suspension_id).order_by(Signature.created_at.asc()).all()
    return render_template('suspension_detail.html', s=s, signatures=signatures)


@behaviour_bp.route('/suspensions/student/<string:esis>', methods=['GET'])
@login_required
def student_suspensions_page(esis):
    _ensure_suspensions_table()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    student = db.session.query(Students).filter_by(esis=esis).first()
    student_name = student.name if student else None
    homeroom = student.homeroom if student else None

    q = db.session.query(Suspension).filter(Suspension.esis == esis)
    if start_date:
        try:
            q = q.filter(Suspension.date_of_suspension >= datetime.strptime(start_date, '%Y-%m-%d').date())
        except Exception:
            pass
    if end_date:
        try:
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            q = q.filter(Suspension.date_of_suspension <= end)
        except Exception:
            pass
    rows = q.order_by(Suspension.date_of_suspension.desc()).all()
    data = [
        {
            'id': s.id,
            'date': s.date_of_suspension.strftime('%Y-%m-%d'),
            'class': s.grade_class or '',
            'duration': s.duration or '',
            'reason': s.reason or '',
        } for s in rows
    ]
    if not student_name and rows:
        student_name = rows[0].student_name
        homeroom = rows[0].grade_class

    return render_template('student_suspensions.html', esis=esis, student_name=student_name or '', homeroom=homeroom or '', suspensions=data)

# ---------------- APIs ----------------
@behaviour_bp.route('/api/student/by-esis', methods=['GET'])
@login_required
def search_student_by_esis_api():
    esis = request.args.get('esis', '')
    if not esis:
        return jsonify({'error': 'ESIS parameter is required'}), 400
    try:
        student = db.session.query(Students).filter_by(esis=esis).first()
        if student:
            return jsonify({'esis': student.esis, 'name': student.name, 'homeroom': student.homeroom})
        return jsonify({}), 404
    except Exception as e:
        current_app.logger.error(f"Error in search_student_by_esis_api: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@behaviour_bp.route('/api/students/search', methods=['GET'])
@login_required
def students_search():
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify([])
    try:
        like = f"%{q}%"
        rows = db.session.query(Students).filter((Students.name.ilike(like)) | (Students.esis.ilike(like))).order_by(Students.name).limit(20).all()
        return jsonify([{'esis': s.esis, 'name': s.name, 'homeroom': s.homeroom} for s in rows])
    except Exception as e:
        current_app.logger.error(f"Error in students_search: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@behaviour_bp.route('/api/students/by-homeroom', methods=['GET'])
@login_required
def get_students_by_homeroom_api():
    homeroom = request.args.get('homeroom', '')
    if not homeroom:
        return jsonify([])
    try:
        students = db.session.query(Students).filter_by(homeroom=homeroom).order_by(Students.name).all()
        return jsonify([{'esis': s.esis, 'name': s.name, 'homeroom': s.homeroom} for s in students])
    except Exception as e:
        current_app.logger.error(f"Error in get_students_by_homeroom_api: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@behaviour_bp.route('/api/homerooms/by-grade', methods=['GET'])
@login_required
def get_homerooms_by_grade_api():
    grade = request.args.get('grade', '')
    if not grade:
        return jsonify([])
    try:
        homerooms_all = db.session.query(Students.homeroom).distinct().all()
        filtered = [hr[0] for hr in homerooms_all if hr[0] and hr[0].startswith(f'G{grade}')]
        return jsonify(filtered)
    except Exception as e:
        current_app.logger.error(f"Error in get_homerooms_by_grade_api: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# ---------------- Incidents API ----------------
@behaviour_bp.route('/api/incidents', methods=['GET'])
@login_required
def get_incidents_api():
    page = request.args.get('page', 1, type=int)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    grade = request.args.get('grade')  # incident grade (C1..C4)
    student_grade = request.args.get('student_grade')  # numeric e.g. '6'
    homeroom = request.args.get('homeroom')
    esis = request.args.get('esis')
    student_name = request.args.get('student_name')

    query = db.session.query(Incident, Teacher).join(Teacher, Incident.teacher_id == Teacher.id)

    if start_date:
        query = query.filter(Incident.date_of_incident >= datetime.strptime(start_date, '%Y-%m-%d'))
    if end_date:
        end = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(Incident.date_of_incident <= end)
    if grade:
        query = query.filter(Incident.incident_grade == grade)
    if student_grade:
        # Filter by student grade derived from homeroom prefix 'G{grade}'
        query = query.filter(Incident.homeroom.like(f'G{student_grade}%'))
    if homeroom:
        query = query.filter(Incident.homeroom == homeroom)
    if esis:
        query = query.filter(Incident.esis == esis)
    if student_name:
        like = f"%{student_name.strip()}%"
        query = query.filter(Incident.name.ilike(like))

    all_incidents = query.all()
    total_incidents = len(all_incidents)
    minor_incidents = sum(1 for inc, _ in all_incidents if inc.incident_grade in ['C1', 'C2'])
    major_incidents = sum(1 for inc, _ in all_incidents if inc.incident_grade in ['C3', 'C4'])
    unique_students = len(set(inc.esis for inc, _ in all_incidents))

    # Chart data by grade
    grade_counts = Counter(inc.incident_grade for inc, _ in all_incidents)
    grade_chart = {
        "labels": ["C1", "C2", "C3", "C4"],
        "counts": [grade_counts.get("C1", 0), grade_counts.get("C2", 0), grade_counts.get("C3", 0), grade_counts.get("C4", 0)]
    }

    # Chart data over time (daily)
    time_counts = Counter(inc.date_of_incident.strftime('%Y-%m-%d') for inc, _ in all_incidents)
    sorted_dates = sorted(time_counts.keys())
    time_chart = {
        "labels": sorted_dates,
        "counts": [time_counts[d] for d in sorted_dates]
    }

    # Frequent violators (top 5 by incident count)
    from collections import defaultdict
    sv_counts = Counter()
    sv_name = {}
    for inc, _ in all_incidents:
        sv_counts[inc.esis] += 1
        if inc.esis not in sv_name:
            sv_name[inc.esis] = inc.name
    frequent_violators = [
        {"esis": esis, "name": sv_name.get(esis, ""), "count": cnt}
        for esis, cnt in sv_counts.most_common(5)
    ]

    # Pagination
    per_page = 10
    start = (page - 1) * per_page
    end = start + per_page
    paginated_incidents = all_incidents[start:end]
    incidents_list = [
        {
            "id": inc.id,
            "esis": inc.esis,
            "name": inc.name,
            "homeroom": inc.homeroom,
            "date_of_incident": inc.date_of_incident.strftime('%Y-%m-%d %H:%M'),
            "incident_grade": inc.incident_grade,
            "teacher_name": teacher.name
        } for inc, teacher in paginated_incidents
    ]

    return jsonify({
        "summary": {
            "total_incidents": total_incidents,
            "minor_incidents": minor_incidents,
            "major_incidents": major_incidents,
            "unique_students": unique_students
        },
        "grade_chart": grade_chart,
        "time_chart": time_chart,
        "frequent_violators": frequent_violators,
        "incidents": incidents_list,
        "total": total_incidents,
        "pages": (total_incidents + per_page - 1) // per_page,
        "current_page": page
    })

# ---------------- Export Endpoints ----------------
@behaviour_bp.route('/behaviour/export', methods=['GET'])
@login_required
def export_incidents():
    # Only admins can export
    if not admin_required():
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    grade = request.args.get('grade')  # incident grade
    student_grade = request.args.get('student_grade')  # student grade level
    homeroom = request.args.get('homeroom')
    esis = request.args.get('esis')
    student_name = request.args.get('student_name')

    query = db.session.query(Incident, Teacher).join(Teacher, Incident.teacher_id == Teacher.id)

    if start_date:
        query = query.filter(Incident.date_of_incident >= datetime.strptime(start_date, '%Y-%m-%d'))
    if end_date:
        end = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(Incident.date_of_incident <= end)
    if grade:
        query = query.filter(Incident.incident_grade == grade)
    if student_grade:
        query = query.filter(Incident.homeroom.like(f'G{student_grade}%'))
    if homeroom:
        query = query.filter(Incident.homeroom == homeroom)
    if esis:
        query = query.filter(Incident.esis == esis)
    if student_name:
        like = f"%{student_name.strip()}%"
        query = query.filter(Incident.name.ilike(like))

    rows = query.order_by(Incident.date_of_incident.desc()).all()

    # Build CSV in-memory
    import io, csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'ID', 'ESIS', 'Student Name', 'Homeroom', 'Date/Time', 'Place', 'Incident Grade',
        'Action Taken', 'Description', 'Submitted By'
    ])
    for inc, teacher in rows:
        writer.writerow([
            inc.id,
            inc.esis,
            inc.name,
            inc.homeroom,
            inc.date_of_incident.strftime('%Y-%m-%d %H:%M'),
            inc.place_of_incident,
            inc.incident_grade,
            inc.action_taken,
            inc.incident_description,
            teacher.name if teacher else 'Unknown'
        ])

    mem = io.BytesIO()
    mem.write(output.getvalue().encode('utf-8-sig'))
    mem.seek(0)
    filename = 'incidents_export.csv'
    return send_file(
        mem,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

@behaviour_bp.route('/behaviour/export/student/<string:esis>', methods=['GET'])
@login_required
def export_incidents_by_student(esis):
    if not admin_required():
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    # Reuse export with esis param
    return redirect(url_for('behaviour_bp.export_incidents', esis=esis))

@behaviour_bp.route('/report/incident/<int:incident_id>', methods=['GET'])
@login_required
def view_incident_report(incident_id):
    # Fetch incident and teacher
    incident = db.session.query(Incident).filter_by(id=incident_id).first()
    if not incident:
        flash('Incident not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))

    teacher = db.session.query(Teacher).filter_by(id=incident.teacher_id).first()
    teacher_name = teacher.name if teacher else "Unknown"

    # Fetch signatures for this incident
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='incident', entity_id=incident_id).order_by(Signature.created_at.asc()).all()
    # Render an HTML report
    return render_template('incident_report.html', incident=incident, teacher_name=teacher_name, signatures=signatures)


# ---------------- Parent Meeting Forms ----------------
@behaviour_bp.route('/forms/parent-meeting/add', methods=['GET', 'POST'])
@login_required
def add_parent_meeting():
    _ensure_parent_meetings_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        homerooms, grades = _get_homerooms_grades()
        return render_template(
            'add_parent_meeting.html',
            esis=esis,
            student=student,
            homerooms=homerooms,
            grades=grades,
            signature_presets=PARENT_MEETING_SIGNATURE_PRESETS
        )

    # POST
    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade_session = (request.form.get('grade_session') or '').strip()
        parent_name = (request.form.get('parent_name') or '').strip()
        attended_by = (request.form.get('attended_by') or '').strip()
        date_str = (request.form.get('date') or '').strip()
        time_val = (request.form.get('time') or '').strip()
        requested_by = (request.form.get('requested_by') or '').strip()
        parent_concerns = (request.form.get('parent_concerns') or '').strip()
        school_concerns = (request.form.get('school_concerns') or '').strip()
        solutions_parent = (request.form.get('solutions_parent') or '').strip()
        solutions_school = (request.form.get('solutions_school') or '').strip()
        agreed_next_steps = (request.form.get('agreed_next_steps') or '').strip()
        signatures_payload = (request.form.get('required_signatures_payload') or '').strip()

        if not all([esis, student_name, date_str]):
            flash('ESIS, Student Name, and Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_parent_meeting', esis=esis))
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            flash('Invalid date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('behaviour_bp.add_parent_meeting', esis=esis))

        try:
            signer_entries = _parse_required_signatures_payload(signatures_payload, PARENT_MEETING_SIGNATURE_PRESETS)
        except ValueError as exc:
            flash(str(exc), 'error')
            return redirect(url_for('behaviour_bp.add_parent_meeting', esis=esis))
        if not signer_entries:
            flash('Capture at least one required signature before submitting.', 'error')
            return redirect(url_for('behaviour_bp.add_parent_meeting', esis=esis))

        required_snapshot = [
            {'key': entry['key'], 'label': entry['label'], 'signer_role': entry['signer_role']}
            for entry in signer_entries
        ]

        # Auto-detect grade/session if missing
        if not grade_session and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                grade_session = s.homeroom

        row = ParentMeeting(
            esis=esis,
            student_name=student_name,
            grade_session=grade_session,
            parent_name=parent_name,
            attended_by=attended_by,
            date=date_obj,
            time=time_val,
            requested_by=requested_by,
            parent_concerns=parent_concerns,
            school_concerns=school_concerns,
            solutions_parent=solutions_parent,
            solutions_school=solutions_school,
            agreed_next_steps=agreed_next_steps,
            required_signature_roles=json.dumps(required_snapshot),
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.flush()
        _create_initial_signatures('parent_meeting', row.id, signer_entries)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_parent_meeting', meeting_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating parent meeting: {e}")
        flash('Failed to create parent meeting form.', 'error')
        return redirect(url_for('behaviour_bp.add_parent_meeting'))


@behaviour_bp.route('/forms/parent-meeting/<int:meeting_id>', methods=['GET'])
@login_required
def view_parent_meeting(meeting_id):
    _ensure_parent_meetings_table()
    pm = db.session.query(ParentMeeting).filter_by(id=meeting_id).first()
    if not pm:
        flash('Parent meeting form not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='parent_meeting', entity_id=meeting_id).order_by(Signature.created_at.asc()).all()
    required_signers = _load_required_signers(pm.required_signature_roles)
    signature_counts = _signature_role_counts(signatures)
    return render_template('parent_meeting_detail.html', pm=pm, signatures=signatures, required_signers=required_signers, signature_counts=signature_counts)


# ---------------- Parent Acknowledgment Forms ----------------
@behaviour_bp.route('/forms/parent-ack/add', methods=['GET', 'POST'])
@login_required
def add_parent_ack():
    _ensure_parent_ack_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        homerooms, grades = _get_homerooms_grades()
        return render_template('add_parent_ack.html', esis=esis, student=student, homerooms=homerooms, grades=grades)

    # POST
    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade_session = (request.form.get('grade_session') or '').strip()
        date_str = (request.form.get('date') or '').strip()

        if not all([esis, student_name, date_str]):
            flash('ESIS, Student Name, and Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_parent_ack', esis=esis))
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            flash('Invalid date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('behaviour_bp.add_parent_ack', esis=esis))

        # Auto-detect grade/session if missing
        if not grade_session and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                grade_session = s.homeroom

        row = ParentAcknowledgment(
            esis=esis,
            student_name=student_name,
            grade_session=grade_session,
            date=date_obj,
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_parent_ack', ack_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating parent acknowledgment: {e}")
        flash('Failed to create parent acknowledgment form.', 'error')
        return redirect(url_for('behaviour_bp.add_parent_ack'))


@behaviour_bp.route('/forms/parent-ack/<int:ack_id>', methods=['GET'])
@login_required
def view_parent_ack(ack_id):
    _ensure_parent_ack_table()
    pa = db.session.query(ParentAcknowledgment).filter_by(id=ack_id).first()
    if not pa:
        flash('Parent acknowledgment not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='parent_ack', entity_id=ack_id).order_by(Signature.created_at.asc()).all()
    return render_template('parent_ack_detail.html', pa=pa, signatures=signatures)


# ---------------- Student Statement Forms ----------------
@behaviour_bp.route('/forms/student-statement/add', methods=['GET', 'POST'])
@login_required
def add_student_statement():
    _ensure_student_statements_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        locations = _preset_locations()
        homerooms, grades = _get_homerooms_grades()
        return render_template(
            'add_student_statement.html',
            esis=esis,
            student=student,
            locations=locations,
            homerooms=homerooms,
            grades=grades,
            max_upload_size=MAX_STUDENT_STATEMENT_FILE_SIZE
        )

    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        class_session = (request.form.get('class_session') or '').strip()
        date_str = (request.form.get('date') or '').strip()
        time_val = (request.form.get('time') or '').strip()
        location = (request.form.get('location') or '').strip()
        other_details = (request.form.get('other_details') or '').strip()
        completed_by = (request.form.get('completed_by') or '').strip()
        completed_by_role = (request.form.get('completed_by_role') or '').strip()
        reviewed_by = (request.form.get('reviewed_by') or '').strip()
        statement_file = request.files.get('statement_file')

        if not all([esis, student_name, date_str]):
            flash('ESIS, Student Name, and Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_student_statement', esis=esis))
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            flash('Invalid date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('behaviour_bp.add_student_statement', esis=esis))

        try:
            file_path, file_name, file_mime, file_size = _save_student_statement_file(statement_file)
        except ValueError as exc:
            flash(str(exc), 'error')
            return redirect(url_for('behaviour_bp.add_student_statement', esis=esis))

        # Auto-detect class/session if missing
        if not class_session and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                class_session = s.homeroom

        row = StudentStatement(
            esis=esis,
            student_name=student_name,
            class_session=class_session,
            date=date_obj,
            time=time_val,
            location=location,
            statement='',
            other_details=other_details,
            completed_by=completed_by,
            completed_by_role=completed_by_role,
            reviewed_by=reviewed_by,
            file_path=file_path,
            file_name=file_name,
            file_mime=file_mime,
            file_size=file_size,
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_student_statement', st_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating student statement: {e}")
        if 'file_path' in locals():
            try:
                (Path(current_app.root_path) / file_path).unlink(missing_ok=True)
            except Exception:
                pass
        flash('Failed to create student statement.', 'error')
        return redirect(url_for('behaviour_bp.add_student_statement'))


@behaviour_bp.route('/forms/student-statement/<int:st_id>', methods=['GET'])
@login_required
def view_student_statement(st_id):
    _ensure_student_statements_table()
    st = db.session.query(StudentStatement).filter_by(id=st_id).first()
    if not st:
        flash('Student statement not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='student_statement', entity_id=st_id).order_by(Signature.created_at.asc()).all()
    return render_template('student_statement_detail.html', st=st, signatures=signatures)


@behaviour_bp.route('/forms/student-statement/<int:st_id>/download', methods=['GET'])
@login_required
def download_student_statement(st_id):
    _ensure_student_statements_table()
    st = db.session.query(StudentStatement).filter_by(id=st_id).first()
    if not st or not st.file_path:
        flash('File not available for this statement.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    abs_path = Path(current_app.root_path) / st.file_path
    if not abs_path.exists():
        flash('Uploaded file is missing.', 'error')
        return redirect(url_for('behaviour_bp.view_student_statement', st_id=st_id))
    return send_file(
        abs_path,
        download_name=st.file_name or abs_path.name,
        mimetype=st.file_mime or 'application/octet-stream',
        as_attachment=True
    )


# ---------------- Staff Statement Forms ----------------
@behaviour_bp.route('/forms/staff-statement/add', methods=['GET', 'POST'])
@login_required
def add_staff_statement():
    _ensure_staff_statements_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        locations = _preset_locations()
        homerooms, grades = _get_homerooms_grades()
        return render_template(
            'add_staff_statement.html',
            esis=esis,
            student=student,
            locations=locations,
            homerooms=homerooms,
            grades=grades,
            signature_presets=STAFF_STATEMENT_SIGNATURE_PRESETS
        )

    try:
        esis = (request.form.get('esis') or '').strip()
        staff_name = (request.form.get('staff_name') or '').strip()
        position = (request.form.get('position') or '').strip()
        date_incident_str = (request.form.get('date_of_incident') or '').strip()
        time_incident = (request.form.get('time_of_incident') or '').strip()
        location_incident = (request.form.get('location_of_incident') or '').strip()
        date_statement_str = (request.form.get('date_of_statement') or '').strip()
        details = (request.form.get('details') or '').strip()
        individuals = (request.form.get('individuals_involved') or '').strip()
        actions_taken = (request.form.get('actions_taken') or '').strip()
        witnesses = (request.form.get('witnesses') or '').strip()
        additional_comments = (request.form.get('additional_comments') or '').strip()
        slt_name = (request.form.get('slt_name') or '').strip()
        slt_position = (request.form.get('slt_position') or '').strip()
        slt_date_str = (request.form.get('slt_date_review') or '').strip()
        slt_actions = (request.form.get('slt_actions') or '').strip()
        signatures_payload = (request.form.get('required_signatures_payload') or '').strip()

        if not all([esis, staff_name]):
            flash('ESIS and Staff Name are required.', 'error')
            return redirect(url_for('behaviour_bp.add_staff_statement', esis=esis))
        date_incident = None
        date_statement = None
        slt_date = None
        try:
            if date_incident_str:
                date_incident = datetime.strptime(date_incident_str, '%Y-%m-%d').date()
            if date_statement_str:
                date_statement = datetime.strptime(date_statement_str, '%Y-%m-%d').date()
            if slt_date_str:
                slt_date = datetime.strptime(slt_date_str, '%Y-%m-%d').date()
        except Exception:
            flash('Invalid date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('behaviour_bp.add_staff_statement', esis=esis))

        try:
            signer_entries = _parse_required_signatures_payload(signatures_payload, STAFF_STATEMENT_SIGNATURE_PRESETS)
        except ValueError as exc:
            flash(str(exc), 'error')
            return redirect(url_for('behaviour_bp.add_staff_statement', esis=esis))
        if not signer_entries:
            flash('Capture all required signatures before submitting.', 'error')
            return redirect(url_for('behaviour_bp.add_staff_statement', esis=esis))
        required_snapshot = [
            {'key': entry['key'], 'label': entry['label'], 'signer_role': entry['signer_role']}
            for entry in signer_entries
        ]

        row = StaffStatement(
            esis=esis,
            staff_name=staff_name,
            position=position,
            date_of_incident=date_incident,
            time_of_incident=time_incident,
            location_of_incident=location_incident,
            date_of_statement=date_statement,
            details=details,
            individuals_involved=individuals,
            actions_taken=actions_taken,
            witnesses=witnesses,
            additional_comments=additional_comments,
            slt_name=slt_name,
            slt_position=slt_position,
            slt_date_review=slt_date,
            slt_actions=slt_actions,
            required_signature_roles=json.dumps(required_snapshot),
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.flush()
        _create_initial_signatures('staff_statement', row.id, signer_entries)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_staff_statement', sf_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating staff statement: {e}")
        flash('Failed to create staff statement.', 'error')
        return redirect(url_for('behaviour_bp.add_staff_statement'))


@behaviour_bp.route('/forms/staff-statement/<int:sf_id>', methods=['GET'])
@login_required
def view_staff_statement(sf_id):
    _ensure_staff_statements_table()
    sf = db.session.query(StaffStatement).filter_by(id=sf_id).first()
    if not sf:
        flash('Staff statement not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='staff_statement', entity_id=sf_id).order_by(Signature.created_at.asc()).all()
    student = db.session.query(Students).filter_by(esis=sf.esis).first()
    required_signers = _load_required_signers(sf.required_signature_roles)
    signature_counts = _signature_role_counts(signatures)
    return render_template('staff_statement_detail.html', sf=sf, signatures=signatures, student=student, required_signers=required_signers, signature_counts=signature_counts)


# ---------------- Safeguarding Concern Forms ----------------
@behaviour_bp.route('/forms/safeguarding/add', methods=['GET', 'POST'])
@login_required
def add_safeguarding():
    _ensure_safeguarding_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        # derive reporting person
        reporting_name = (session.get('teacher_name') or '')
        reporting_role = 'Admin' if session.get('is_admin') or session.get('role') == 'admin' else 'Teacher'
        today = datetime.utcnow().date().strftime('%Y-%m-%d')
        now = datetime.utcnow().strftime('%H:%M')
        locations = _preset_locations()
        homerooms, grades = _get_homerooms_grades()
        return render_template('add_safeguarding.html', esis=esis, student=student, reporting_name=reporting_name, reporting_role=reporting_role, today=today, now=now, locations=locations, homerooms=homerooms, grades=grades)

    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade_session = (request.form.get('grade_session') or '').strip()
        reporting_name = (request.form.get('reporting_name') or '').strip()
        reporting_role = (request.form.get('reporting_role') or '').strip()
        report_date_str = (request.form.get('report_date') or '').strip()
        report_time = (request.form.get('report_time') or '').strip()
        incident_date_str = (request.form.get('incident_date') or '').strip()
        incident_time = (request.form.get('incident_time') or '').strip()
        incident_location = (request.form.get('incident_location') or '').strip()
        description = (request.form.get('description') or '').strip()
        concern_types = ', '.join(request.form.getlist('concern_types'))
        student_disclosure = (request.form.get('student_disclosure') or '').strip()
        disclosure_details = (request.form.get('disclosure_details') or '').strip()
        immediate_actions = (request.form.get('immediate_actions') or '').strip()
        referred_to = (request.form.get('referred_to') or '').strip()
        referral_time = (request.form.get('referral_time') or '').strip()
        referral_date_str = (request.form.get('referral_date') or '').strip()
        follow_up_actions = (request.form.get('follow_up_actions') or '').strip()
        additional_notes = (request.form.get('additional_notes') or '').strip()

        if not all([esis, student_name, reporting_name, reporting_role, report_date_str]):
            flash('Required fields are missing.', 'error')
            return redirect(url_for('behaviour_bp.add_safeguarding', esis=esis))

        report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
        incident_date = datetime.strptime(incident_date_str, '%Y-%m-%d').date() if incident_date_str else None
        referral_date = datetime.strptime(referral_date_str, '%Y-%m-%d').date() if referral_date_str else None

        # Auto-detect grade/session if missing
        if not grade_session and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                grade_session = s.homeroom

        row = SafeguardingConcern(
            esis=esis,
            student_name=student_name,
            grade_session=grade_session,
            reporting_name=reporting_name,
            reporting_role=reporting_role,
            report_date=report_date,
            report_time=report_time,
            incident_date=incident_date,
            incident_time=incident_time,
            incident_location=incident_location,
            description=description,
            concern_types=concern_types,
            student_disclosure=student_disclosure,
            disclosure_details=disclosure_details,
            immediate_actions=immediate_actions,
            referred_to=referred_to,
            referral_time=referral_time,
            referral_date=referral_date,
            follow_up_actions=follow_up_actions,
            additional_notes=additional_notes,
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_safeguarding', sg_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating safeguarding form: {e}")
        flash('Failed to create safeguarding form.', 'error')
        return redirect(url_for('behaviour_bp.add_safeguarding'))


@behaviour_bp.route('/forms/safeguarding/<int:sg_id>', methods=['GET'])
@login_required
def view_safeguarding(sg_id):
    _ensure_safeguarding_table()
    sg = db.session.query(SafeguardingConcern).filter_by(id=sg_id).first()
    if not sg:
        flash('Safeguarding form not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='safeguarding', entity_id=sg_id).order_by(Signature.created_at.asc()).all()
    return render_template('safeguarding_detail.html', sg=sg, signatures=signatures)


# ---------------- Phone Violation Contract ----------------
@behaviour_bp.route('/forms/phone-violation/add', methods=['GET', 'POST'])
@login_required
def add_phone_violation():
    _ensure_phone_violation_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        today = datetime.utcnow().date().strftime('%Y-%m-%d')
        homerooms, grades = _get_homerooms_grades()
        return render_template('add_phone_violation.html', esis=esis, student=student, today=today, homerooms=homerooms, grades=grades)

    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade_session = (request.form.get('grade_session') or '').strip()
        date_str = (request.form.get('date') or '').strip()
        if not all([esis, student_name, date_str]):
            flash('ESIS, Student Name, and Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_phone_violation', esis=esis))
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        # Auto-detect grade/session if missing
        if not grade_session and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                grade_session = s.homeroom

        row = PhoneViolationContract(
            esis=esis,
            student_name=student_name,
            grade_session=grade_session,
            date=date_obj,
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_phone_violation', pv_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating phone violation: {e}")
        flash('Failed to create phone violation form.', 'error')
        return redirect(url_for('behaviour_bp.add_phone_violation'))


@behaviour_bp.route('/forms/phone-violation/<int:pv_id>', methods=['GET'])
@login_required
def view_phone_violation(pv_id):
    _ensure_phone_violation_table()
    pv = db.session.query(PhoneViolationContract).filter_by(id=pv_id).first()
    if not pv:
        flash('Phone violation form not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='phone_violation', entity_id=pv_id).order_by(Signature.created_at.asc()).all()
    return render_template('phone_violation_detail.html', pv=pv, signatures=signatures)


# ---------------- Counseling Session Tracker ----------------
@behaviour_bp.route('/forms/counseling-session/add', methods=['GET', 'POST'])
@login_required
def add_counseling_session():
    _ensure_counseling_sessions_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        homerooms, grades = _get_homerooms_grades()
        today = datetime.utcnow().date().strftime('%Y-%m-%d')
        default_duration = 15
        return render_template(
            'add_counseling_session.html',
            esis=esis,
            student=student,
            today=today,
            default_duration=default_duration,
            homerooms=homerooms,
            grades=grades
        )

    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        homeroom = (request.form.get('grade_session') or '').strip()
        session_date_str = (request.form.get('session_date') or '').strip()
        duration_str = (request.form.get('duration_minutes') or '').strip()
        counselors = (request.form.get('counselors') or '').strip()

        if not all([esis, student_name, session_date_str]):
            flash('ESIS, Student Name, and Session Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_counseling_session', esis=esis))

        try:
            session_date = datetime.strptime(session_date_str, '%Y-%m-%d').date()
        except Exception:
            flash('Invalid session date. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('behaviour_bp.add_counseling_session', esis=esis))

        if not homeroom and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                homeroom = s.homeroom

        try:
            duration_minutes = int(duration_str) if duration_str else None
        except ValueError:
            flash('Duration must be a number of minutes.', 'error')
            return redirect(url_for('behaviour_bp.add_counseling_session', esis=esis))

        checkbox_fields = [
            'focus_study_skills', 'focus_time_management', 'focus_academic_goal_setting',
            'focus_self_esteem', 'focus_decision_making', 'focus_mindfulness_relaxation',
            'focus_stress_management', 'focus_coping_anxiety', 'focus_conflict_resolution',
            'focus_immediate_crisis_support', 'focus_grief_loss', 'focus_support_trauma',
            'focus_managing_anger', 'focus_improving_communication', 'focus_positive_habits',
            'group_building_friendships', 'group_developing_empathy', 'group_grief_loss_support',
            'group_anxiety_depression_support', 'group_learning_disabilities_support',
            'group_stress_management', 'group_leadership_training', 'group_team_building',
            'group_community_service', 'group_mediation_skills', 'group_role_play_conflict',
            'group_communication_strategies'
        ]
        checkbox_values = {field: bool(request.form.get(field)) for field in checkbox_fields}

        session_row = CounselingSession(
            esis=esis,
            student_name=student_name,
            homeroom=homeroom,
            session_date=session_date,
            duration_minutes=duration_minutes,
            counselors=counselors,
            summary_of_progress=(request.form.get('summary_of_progress') or '').strip(),
            progress_toward_goals=(request.form.get('progress_toward_goals') or '').strip(),
            follow_up_challenges=(request.form.get('follow_up_challenges') or '').strip(),
            follow_up_support=(request.form.get('follow_up_support') or '').strip(),
            future_sessions_planned=(request.form.get('future_sessions_planned') or '').strip(),
            additional_support_needed=(request.form.get('additional_support_needed') or '').strip(),
            parent_guardian_communication=(request.form.get('parent_guardian_communication') or '').strip(),
            counselor_observations=(request.form.get('counselor_observations') or '').strip(),
            created_by_teacher_id=session.get('teacher_id')
        )
        for field, value in checkbox_values.items():
            setattr(session_row, field, value)

        db.session.add(session_row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_counseling_session', session_id=session_row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating counseling session: {e}")
        flash('Failed to create counseling session form.', 'error')
        return redirect(url_for('behaviour_bp.add_counseling_session', esis=(request.form.get('esis') or '').strip()))


@behaviour_bp.route('/forms/counseling-session/<int:session_id>', methods=['GET'])
@login_required
def view_counseling_session(session_id):
    _ensure_counseling_sessions_table()
    session_row = db.session.query(CounselingSession).filter_by(id=session_id).first()
    if not session_row:
        flash('Counseling session not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(
        entity_type='counseling_session',
        entity_id=session_id
    ).order_by(Signature.created_at.asc()).all()
    return render_template('counseling_session_detail.html', session=session_row, signatures=signatures)


# ---------------- Behavior Contract ----------------
@behaviour_bp.route('/forms/behavior-contract/add', methods=['GET', 'POST'])
@login_required
def add_behavior_contract():
    _ensure_behavior_contracts_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        today = datetime.utcnow().date().strftime('%Y-%m-%d')
        now = datetime.utcnow().strftime('%H:%M')
        # Try to parse grade from homeroom like G10-A
        grade = ''
        if student and student.homeroom:
            m = re.match(r'G(\d+)', student.homeroom)
            if m:
                grade = m.group(1)
        homerooms, grades = _get_homerooms_grades()
        return render_template('add_behavior_contract.html', esis=esis, student=student, today=today, now=now, grade=grade, homerooms=homerooms, grades=grades)

    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade = (request.form.get('grade') or '').strip()
        date_str = (request.form.get('date') or '').strip()
        time_val = (request.form.get('time') or '').strip()
        cons_warning = request.form.get('cons_warning') is not None
        cons_parent_meeting = request.form.get('cons_parent_meeting') is not None
        cons_detention = request.form.get('cons_detention') is not None
        cons_referral = request.form.get('cons_referral') is not None
        cons_further_action = request.form.get('cons_further_action') is not None
        cons_further_action_text = (request.form.get('cons_further_action_text') or '').strip()

        if not all([esis, student_name, date_str]):
            flash('ESIS, Student Name, and Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_behavior_contract', esis=esis))
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        # Auto-detect grade if missing
        if not grade and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                m = re.match(r'G(\d+)', s.homeroom or '')
                if m:
                    grade = m.group(1)

        row = BehaviorContract(
            esis=esis,
            student_name=student_name,
            grade=grade,
            date=date_obj,
            time=time_val,
            cons_warning=cons_warning,
            cons_parent_meeting=cons_parent_meeting,
            cons_detention=cons_detention,
            cons_referral=cons_referral,
            cons_further_action=cons_further_action,
            cons_further_action_text=cons_further_action_text,
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_behavior_contract', bc_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating behavior contract: {e}")
        flash('Failed to create behavior contract.', 'error')
        return redirect(url_for('behaviour_bp.add_behavior_contract'))


@behaviour_bp.route('/forms/behavior-contract/<int:bc_id>', methods=['GET'])
@login_required
def view_behavior_contract(bc_id):
    _ensure_behavior_contracts_table()
    bc = db.session.query(BehaviorContract).filter_by(id=bc_id).first()
    if not bc:
        flash('Behavior contract not found.', 'error')
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    _ensure_signatures_table()
    signatures = db.session.query(Signature).filter_by(entity_type='behavior_contract', entity_id=bc_id).order_by(Signature.created_at.asc()).all()
    return render_template('behavior_contract_detail.html', bc=bc, signatures=signatures)


# ---------------- Student Conduct Pledge ----------------
@behaviour_bp.route('/forms/student-conduct-pledge/add', methods=['GET', 'POST'])
@login_required
def add_student_conduct_pledge():
    _ensure_student_conduct_pledge_table()
    if request.method == 'GET':
        esis = (request.args.get('esis') or '').strip()
        student = db.session.query(Students).filter_by(esis=esis).first() if esis else None
        today = datetime.utcnow().date().strftime('%Y-%m-%d')
        grade = ''
        if student and student.homeroom:
            m = re.match(r'G(\d+)', student.homeroom)
            if m:
                grade = m.group(1)
        homerooms, grades = _get_homerooms_grades()
        return render_template('add_student_conduct_pledge.html', esis=esis, student=student, today=today, grade=grade, homerooms=homerooms, grades=grades)

    # POST
    try:
        esis = (request.form.get('esis') or '').strip()
        student_name = (request.form.get('student_name') or '').strip()
        grade = (request.form.get('grade') or '').strip()
        date_str = (request.form.get('date') or '').strip()

        if not all([esis, student_name, date_str]):
            flash('ESIS, Student Name, and Date are required.', 'error')
            return redirect(url_for('behaviour_bp.add_student_conduct_pledge', esis=esis))

        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()

        if not grade and esis:
            s = db.session.query(Students).filter_by(esis=esis).first()
            if s and s.homeroom:
                m = re.match(r'G(\d+)', s.homeroom or '')
                if m:
                    grade = m.group(1)

        row = StudentConductPledge(
            esis=esis,
            student_name=student_name,
            grade=grade,
            date=date_obj,
            created_by_teacher_id=session.get('teacher_id')
        )
        db.session.add(row)
        db.session.commit()
        return redirect(url_for('behaviour_bp.view_student_conduct_pledge', pledge_id=row.id))
    except Exception as e:
        current_app.logger.error(f"Error creating student conduct pledge: {e}")
        flash('Failed to create student conduct pledge.', 'error')
        return redirect(url_for('behaviour_bp.add_student_conduct_pledge'))

@behaviour_bp.route('/forms/student-conduct-pledge/<int:pledge_id>', methods=['GET'])
@login_required
def view_student_conduct_pledge(pledge_id):
    _ensure_student_conduct_pledge_table()
    pledge = db.session.query(StudentConductPledge).filter_by(id=pledge_id).first_or_404()
    signatures = db.session.query(Signature).filter_by(entity_type='student_conduct_pledge', entity_id=pledge_id).order_by(Signature.created_at.asc()).all()
    return render_template('student_conduct_pledge_detail.html', pledge=pledge, signatures=signatures)
# ---------------- Student Incidents Page ----------------
@behaviour_bp.route('/behaviour/student/<string:esis>', methods=['GET'])
@login_required
def student_incidents_page(esis):
    _ensure_parent_meetings_table()
    _ensure_parent_ack_table()
    _ensure_student_statements_table()
    _ensure_staff_statements_table()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Base info
    student = db.session.query(Students).filter_by(esis=esis).first()
    student_name = student.name if student else None
    homeroom = student.homeroom if student else None

    query = db.session.query(Incident, Teacher).join(Teacher, Incident.teacher_id == Teacher.id, isouter=True).filter(Incident.esis == esis)
    if start_date:
        query = query.filter(Incident.date_of_incident >= datetime.strptime(start_date, '%Y-%m-%d'))
    if end_date:
        end = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query = query.filter(Incident.date_of_incident <= end)

    rows = query.order_by(Incident.date_of_incident.desc()).all()
    total = len(rows)
    minor = sum(1 for inc, _ in rows if inc.incident_grade in ['C1', 'C2'])
    major = sum(1 for inc, _ in rows if inc.incident_grade in ['C3', 'C4'])

    # Time chart data
    from collections import Counter
    by_date = Counter(inc.date_of_incident.strftime('%Y-%m-%d') for inc, _ in rows)
    sorted_dates = sorted(by_date.keys())
    time_chart = {
        'labels': sorted_dates,
        'counts': [by_date[d] for d in sorted_dates]
    }

    incidents = [
        {
            'id': inc.id,
            'date': inc.date_of_incident.strftime('%Y-%m-%d %H:%M'),
            'grade': inc.incident_grade,
            'place': inc.place_of_incident,
            'teacher': teacher.name if teacher else 'Unknown',
            'desc': inc.incident_description
        } for inc, teacher in rows
    ]

    # Fallback name/homeroom if student record missing
    if not student_name and rows:
        student_name = rows[0][0].name
        homeroom = rows[0][0].homeroom

    # Additional forms for this student
    _ensure_parent_meetings_table()
    _ensure_parent_ack_table()
    _ensure_student_statements_table()
    _ensure_staff_statements_table()
    pm_rows = db.session.query(ParentMeeting).filter_by(esis=esis).order_by(ParentMeeting.date.desc(), ParentMeeting.created_at.desc()).all()
    ack_rows = db.session.query(ParentAcknowledgment).filter_by(esis=esis).order_by(ParentAcknowledgment.date.desc(), ParentAcknowledgment.created_at.desc()).all()

    parent_meetings = [
        {
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d'),
            'parent_name': r.parent_name or '',
            'attended_by': r.attended_by or ''
        } for r in pm_rows
    ]
    parent_acks = [
        {
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d')
        } for r in ack_rows
    ]

    # Student and Staff statements
    st_rows = db.session.query(StudentStatement).filter_by(esis=esis).order_by(StudentStatement.date.desc(), StudentStatement.created_at.desc()).all()
    sf_rows = db.session.query(StaffStatement).filter_by(esis=esis).order_by(StaffStatement.created_at.desc()).all()

    student_statements = [
        {
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d'),
            'location': r.location or ''
        } for r in st_rows
    ]
    staff_statements = [
        {
            'id': r.id,
            'date': (r.date_of_statement or r.date_of_incident).strftime('%Y-%m-%d') if (r.date_of_statement or r.date_of_incident) else '',
            'staff_name': r.staff_name or '',
            'position': r.position or ''
        } for r in sf_rows
    ]

    # Safeguarding and Phone violations
    _ensure_safeguarding_table()
    _ensure_phone_violation_table()
    sg_rows = db.session.query(SafeguardingConcern).filter_by(esis=esis).order_by(SafeguardingConcern.report_date.desc(), SafeguardingConcern.created_at.desc()).all()
    pv_rows = db.session.query(PhoneViolationContract).filter_by(esis=esis).order_by(PhoneViolationContract.date.desc(), PhoneViolationContract.created_at.desc()).all()

    safeguarding_list = [
        {
            'id': r.id,
            'report_date': r.report_date.strftime('%Y-%m-%d'),
            'reporting_name': r.reporting_name,
            'reporting_role': r.reporting_role
        } for r in sg_rows
    ]
    phone_violations = [
        {
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d')
        } for r in pv_rows
    ]
    # Behavior contracts
    _ensure_behavior_contracts_table()
    bc_rows = db.session.query(BehaviorContract).filter_by(esis=esis).order_by(BehaviorContract.date.desc(), BehaviorContract.created_at.desc()).all()
    behavior_contracts = [
        {
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d'),
            'grade': r.grade or ''
        } for r in bc_rows
    ]
    
    # Counseling sessions
    _ensure_counseling_sessions_table()
    cs_rows = db.session.query(CounselingSession).filter_by(esis=esis).order_by(CounselingSession.session_date.desc(), CounselingSession.created_at.desc()).all()
    counseling_sessions = [
        {
            'id': r.id,
            'date': r.session_date.strftime('%Y-%m-%d'),
            'counselors': r.counselors or ''
        } for r in cs_rows
    ]

    # Student Conduct Pledges
    _ensure_student_conduct_pledge_table()
    pledge_rows = db.session.query(StudentConductPledge).filter_by(esis=esis).order_by(StudentConductPledge.date.desc(), StudentConductPledge.created_at.desc()).all()
    student_conduct_pledges = [
        {
            'id': r.id,
            'date': r.date.strftime('%Y-%m-%d')
        } for r in pledge_rows
    ]

    # Dynamic form submissions
    try:
        ensure_dynamic_form_tables()
        dynamic_submissions = db.session.query(DynamicFormSubmission).filter_by(student_esis=esis).order_by(DynamicFormSubmission.created_at.desc()).all()
    except Exception as err:
        current_app.logger.warning(f"Dynamic form history unavailable: {err}")
        dynamic_submissions = []

    return render_template(
        'student_incidents.html',
        esis=esis,
        student_name=student_name or '',
        homeroom=homeroom or '',
        summary={'total': total, 'minor': minor, 'major': major},
        time_chart=time_chart,
        incidents=incidents,
        parent_meetings=parent_meetings,
        parent_acknowledgments=parent_acks,
        student_statements=student_statements,
        staff_statements=staff_statements,
        safeguarding_list=safeguarding_list,
        phone_violations=phone_violations,
        behavior_contracts=behavior_contracts,
        counseling_sessions=counseling_sessions,
        student_conduct_pledges=student_conduct_pledges,
        dynamic_submissions=dynamic_submissions
    )

def _schedule_incident_alert(incident_id: int) -> None:
    app = current_app._get_current_object()

    def task():
        with app.app_context():
            _send_incident_alert(incident_id)

    threading.Thread(target=task, daemon=True).start()


def _schedule_incident_notification(incident_id: int) -> None:
    app = current_app._get_current_object()

    def task():
        with app.app_context():
            _send_incident_notification(incident_id)

    threading.Thread(target=task, daemon=True).start()


def _format_plan_sections(plan_text: str):
    sections = []
    for raw_line in plan_text.replace('\r', '').split('\n'):
        line = raw_line.strip()
        if not line or ':' not in line:
            continue
        title, body = line.split(':', 1)
        items = [item.strip() for item in body.split(';') if item.strip()]
        sections.append((title.strip(), items))
    return sections



def _render_behaviour_alert_html(incident: Incident, total_incidents: int, plan_text: str) -> str:
    primary = '#0f172a'
    accent = '#2563eb'
    background = '#f8fafc'
    student_name = html.escape(incident.name or 'Unknown')
    esis = html.escape(incident.esis or 'N/A')
    homeroom = html.escape(incident.homeroom or 'Unknown')
    category = html.escape(incident.incident_grade or 'N/A')
    action_taken = html.escape(incident.action_taken or 'N/A')
    description = html.escape(incident.incident_description or 'N/A')
    place = html.escape(incident.place_of_incident or 'N/A')
    incident_time = incident.date_of_incident.strftime('%Y-%m-%d %H:%M') if incident.date_of_incident else 'N/A'
    incident_time = html.escape(incident_time)
    sections = _format_plan_sections(plan_text)
    plan_blocks = []
    for title, items in sections:
        safe_title = html.escape(title)
        if len(items) > 1:
            bullets = ''.join(f'<li style="margin-bottom:4px;">{html.escape(item)}</li>' for item in items)
            body_html = f'<ul style="margin:8px 0 0;padding-left:18px;color:{primary};">{bullets}</ul>'
        else:
            content = html.escape(items[0]) if items else ''
            body_html = f'<p style="margin:8px 0 0;color:{primary};">{content}</p>'
        plan_blocks.append(
            f'<div style="margin-bottom:16px;"><div style="font-size:12px;letter-spacing:0.08em;font-weight:600;text-transform:uppercase;color:{accent};">{safe_title}</div>{body_html}</div>'
        )
    if not plan_blocks:
        plan_blocks.append(f'<p style="margin:0;color:{primary};">{html.escape(plan_text)}</p>')
    plan_html = ''.join(plan_blocks)
    return (
        f'''
<div style="background:{background};border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;font-family:'Segoe UI',Arial,sans-serif;color:{primary};">
  <div style="background:linear-gradient(135deg,#1e3a8a,#2563eb);padding:20px;color:#ffffff;">
    <div style="font-size:13px;letter-spacing:0.08em;text-transform:uppercase;opacity:0.85;">Behaviour Alert</div>
    <div style="font-size:20px;font-weight:600;margin-top:6px;">{student_name}</div>
    <div style="opacity:0.8;margin-top:4px;">ESIS {esis} &bull; {homeroom}</div>
    <div style="display:flex;gap:12px;margin-top:16px;flex-wrap:wrap;">
      <div style="background:rgba(255,255,255,0.2);border-radius:12px;padding:12px;min-width:140px;color:#ffffff;">
        <div style="font-size:12px;opacity:0.85;">Recorded Incidents</div>
        <div style="font-size:22px;font-weight:600;">{total_incidents}</div>
      </div>
    </div>
  </div>

  <div style="padding:20px;">
    <div style="background:#fff;border:1px solid #dbeafe;border-radius:12px;padding:16px;">
      <div style="font-size:12px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:{accent};">Most Recent Incident</div>
      <div style="margin-top:10px;color:{primary};">
        <div style="margin-bottom:4px;"><strong>Date:</strong> {incident_time}</div>
        <div style="margin-bottom:4px;"><strong>Location:</strong> {place}</div>
        <div style="margin-bottom:4px;"><strong>Category:</strong> {category}</div>
        <div style="margin-bottom:4px;"><strong>Action Taken:</strong> {action_taken}</div>
        <div style="margin-bottom:0;"><strong>Description:</strong> {description}</div>
      </div>
    </div>
    <div style="margin-top:20px;">
      <div style="font-size:16px;font-weight:600;color:{primary};margin-bottom:12px;">Action Plan</div>
      {plan_html}
    </div>
  </div>
</div>
'''
    )



def _incident_notification_subject(incident: Incident) -> str:
    grade = (incident.incident_grade or '').upper() or 'C1'
    severity = 'Major Incident' if grade in ('C3', 'C4') else 'Incident'
    student = incident.name or 'Student'
    return f"[Behaviour] {severity} {grade} - {student}"


def _render_incident_notification_html(incident: Incident, incident_url: str) -> str:
    grade = (incident.incident_grade or '').upper() or 'C1'
    is_major = grade in ('C3', 'C4')
    theme = {
        'background': '#fef2f2' if is_major else '#fffbeb',
        'border': '#fca5a5' if is_major else '#fcd34d',
        'accent': '#b91c1c' if is_major else '#b45309',
        'headline': '#7f1d1d' if is_major else '#92400e',
        'button_bg': '#dc2626' if is_major else '#f59e0b',
        'text': '#1f2937',
    }

    student_name = html.escape(incident.name or 'Unknown')
    esis = html.escape(incident.esis or 'N/A')
    homeroom = html.escape(incident.homeroom or 'N/A')
    teacher = html.escape((incident.teacher.name if incident.teacher else 'Unknown') or 'Unknown')
    location = html.escape(incident.place_of_incident or 'Unknown')
    description = html.escape(incident.incident_description or 'No description provided.')
    action_taken = html.escape(incident.action_taken or 'Not specified.')
    timestamp = incident.date_of_incident.strftime('%Y-%m-%d %H:%M') if incident.date_of_incident else 'N/A'
    timestamp = html.escape(timestamp)
    safe_url = html.escape(incident_url)

    return f"""
<div style="font-family:'Segoe UI',Arial,sans-serif;background:{theme['background']};border:1px solid {theme['border']};border-radius:16px;padding:24px;max-width:640px;margin:0 auto;color:{theme['text']};">
  <div style="display:flex;flex-direction:column;gap:16px;">
    <div>
      <div style="font-size:12px;letter-spacing:0.12em;text-transform:uppercase;color:{theme['accent']};font-weight:600;">{'Major Incident' if is_major else 'Incident Notification'}</div>
      <div style="font-size:22px;font-weight:700;color:{theme['headline']};margin-top:4px;">{grade} reported for {student_name}</div>
      <div style="margin-top:6px;font-size:14px;color:{theme['text']};opacity:0.85;">Recorded on {timestamp} &bull; {location}</div>
    </div>
    <div style="background:white;border-radius:12px;padding:20px;border:1px solid rgba(0,0,0,0.05);">
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;font-size:14px;">
        <div><div style="font-weight:600;color:{theme['accent']};">Student</div><div>{student_name}</div></div>
        <div><div style="font-weight:600;color:{theme['accent']};">ESIS</div><div>{esis}</div></div>
        <div><div style="font-weight:600;color:{theme['accent']};">Homeroom</div><div>{homeroom}</div></div>
        <div><div style="font-weight:600;color:{theme['accent']};">Recorded By</div><div>{teacher}</div></div>
      </div>
      <div style="margin-top:18px;">
        <div style="font-weight:600;color:{theme['accent']};margin-bottom:6px;">Incident Summary</div>
        <div style="margin-bottom:6px;"><strong>Action Taken:</strong> {action_taken}</div>
        <div style="line-height:1.6;">{description}</div>
      </div>
    </div>
    <div style="text-align:center;">
      <a href="{safe_url}" style="display:inline-block;padding:12px 24px;background:{theme['button_bg']};color:white;font-weight:600;border-radius:999px;text-decoration:none;" target="_blank" rel="noopener noreferrer">View Incident in Behaviour App</a>
    </div>
    <div style="font-size:12px;color:{theme['text']};opacity:0.7;text-align:center;">This notification was generated automatically from the Behaviour system.</div>
  </div>
</div>
"""


def _deliver_behaviour_alert(incident: Incident, total_incidents: int, *, force: bool = False):
    if not incident:
        return {'sent': False, 'reason': 'no_incident'}

    if not force and total_incidents != INCIDENT_ALERT_THRESHOLD:
        return {'sent': False, 'reason': 'threshold_mismatch'}

    grade = _extract_grade_from_homeroom(incident.homeroom)
    if not grade:
        return {'sent': False, 'reason': 'no_grade'}

    _ensure_grade_notification_table()
    setting = db.session.get(GradeNotificationSetting, grade)
    if not setting:
        return {'sent': False, 'reason': 'no_setting', 'grade': grade}

    recipients = []
    if setting.lead_email:
        recipients.append(setting.lead_email.strip())
    recipients.extend(_parse_recipient_list(setting.recipient_emails or ''))
    recipients = [addr for addr in dict.fromkeys([a for a in recipients if a])]
    if not recipients:
        return {'sent': False, 'reason': 'no_recipients', 'grade': grade}

    try:
        profile = _prepare_action_plan_profile(incident.esis)
        plan_text = generate_action_plan(profile)
    except Exception as err:
        current_app.logger.error('Failed to generate action plan for email: %s', err)
        plan_text = 'Unable to auto-generate an action plan. Please review the behaviour record for more details.'

    email_html = _render_behaviour_alert_html(incident, total_incidents, plan_text)
    student_label = incident.name or 'Student'
    subject = f"[Behaviour Alert] {student_label} | {total_incidents} incidents recorded"

    try:
        send_mail(recipients, subject, email_html, profile="behaviour")
        current_app.logger.info(
            'Behaviour alert sent for %s to %s', incident.esis, ', '.join(recipients)
        )
        return {
            'sent': True,
            'student': f"{incident.name} ({incident.esis})",
            'recipients': recipients,
            'grade': grade,
            'total': total_incidents,
        }
    except Exception as err:
        current_app.logger.error('Failed to send behaviour alert email: %s', err)
        return {
            'sent': False,
            'reason': 'send_error',
            'error': str(err),
            'student': f"{incident.name} ({incident.esis})",
            'recipients': recipients,
            'grade': grade,
            'total': total_incidents,
        }



def _send_incident_alert(incident_id: int) -> None:
    try:
        incident = db.session.get(Incident, incident_id)
    except Exception as err:
        current_app.logger.error('Failed to load incident for alert: %s', err)
        return
    if not incident:
        return

    total_incidents = db.session.query(Incident).filter(Incident.esis == incident.esis).count()
    outcome = _deliver_behaviour_alert(incident, total_incidents, force=False)
    if outcome.get('sent'):
        return

    reason = outcome.get('reason')
    if reason and reason != 'threshold_mismatch':
        current_app.logger.info('Skipping behaviour alert for incident %s: %s', incident_id, reason)




def _send_incident_notification(incident_id: int) -> None:
    try:
        incident = db.session.get(Incident, incident_id)
    except Exception as err:
        current_app.logger.error('Failed to load incident for notification: %s', err)
        return
    if not incident:
        return

    _ensure_incident_notification_table()
    try:
        setting = (
            db.session.query(IncidentNotificationSetting)
            .order_by(IncidentNotificationSetting.id.asc())
            .first()
        )
    except Exception as err:
        current_app.logger.error('Failed to read incident notification setting: %s', err)
        return

    if not setting or not setting.enabled:
        return

    recipient = (setting.recipient_email or '').strip()
    if not recipient:
        return

    try:
        with current_app.test_request_context():
            incident_link = url_for('behaviour_bp.view_incident_report', incident_id=incident.id, _external=True)
    except Exception:
        incident_link = f"/report/incident/{incident.id}"

    email_html = _render_incident_notification_html(incident, incident_link)
    subject = _incident_notification_subject(incident)

    try:
        send_mail(recipient, subject, email_html, profile="behaviour")
        current_app.logger.info('Incident notification sent for %s to %s', incident.esis, recipient)
    except Exception as err:
        current_app.logger.error('Failed to send incident notification email: %s', err)


def run_manual_incident_alerts():
    summary = {
        'candidates': 0,
        'sent': [],
        'skipped': {
            'no_grade': [],
            'no_setting': [],
            'no_recipients': [],
            'threshold_mismatch': [],
            'no_incident': [],
        },
        'failed': [],
    }

    try:
        rows = (
            db.session.query(Incident.esis, func.count(Incident.id).label('total'))
            .group_by(Incident.esis)
            .having(func.count(Incident.id) >= INCIDENT_ALERT_THRESHOLD)
            .all()
        )
    except Exception as err:
        current_app.logger.error('Failed to load incident counts for reminders: %s', err)
        return summary

    summary['candidates'] = len(rows)
    for esis, total in rows:
        try:
            latest_incident = (
                db.session.query(Incident)
                .filter(Incident.esis == esis)
                .order_by(Incident.date_of_incident.desc(), Incident.created_at.desc())
                .first()
            )
        except Exception as err:
            current_app.logger.error('Failed to query latest incident for %s: %s', esis, err)
            summary['skipped'].setdefault('query_error', []).append(esis)
            continue

        if not latest_incident:
            summary['skipped']['no_incident'].append(esis)
            continue

        outcome = _deliver_behaviour_alert(latest_incident, total, force=True)
        descriptor = f"{latest_incident.name} ({esis})"

        if outcome.get('sent'):
            summary['sent'].append({
                'student': descriptor,
                'grade': outcome.get('grade'),
                'total': total,
                'recipients': outcome.get('recipients', []),
            })
            continue

        reason = outcome.get('reason')
        if reason == 'send_error':
            summary['failed'].append({
                'student': descriptor,
                'error': outcome.get('error', 'Unable to send email'),
                'recipients': outcome.get('recipients', []),
            })
        else:
            summary['skipped'].setdefault(reason or 'unknown', []).append(descriptor)

    return summary


@behaviour_bp.route('/admin/grade-notifications/send-reminders', methods=['POST'])
@login_required
def send_grade_notification_reminders():
    if not admin_required():
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))

    summary = run_manual_incident_alerts()
    candidates = summary.get('candidates', 0)
    sent_count = len(summary.get('sent', []))
    skipped_total = sum(len(items) for items in summary.get('skipped', {}).values())
    failed_count = len(summary.get('failed', []))

    if candidates == 0:
        flash('No students currently meet the three-incident threshold.', 'info')
    else:
        def _format_students(items):
            if not items:
                return ''
            if len(items) > 5:
                return ', '.join(items[:5]) + f", +{len(items) - 5} more"
            return ', '.join(items)

        parts = [
            f"Processed {candidates} student{'s' if candidates != 1 else ''}.",
            f"Sent {sent_count} alert{'s' if sent_count != 1 else ''}."
        ]
        if skipped_total:
            parts.append(f"Skipped {skipped_total}.")
        if failed_count:
            parts.append(f"{failed_count} send error{'s' if failed_count != 1 else ''}.")
        category = 'success' if sent_count else ('error' if failed_count else 'warning' if skipped_total else 'info')
        flash(' '.join(parts), category)

        skipped = summary.get('skipped', {})
        if skipped.get('no_setting'):
            flash('Missing grade email settings for: ' + _format_students(skipped['no_setting']), 'warning')
        if skipped.get('no_recipients'):
            flash('No recipients configured for: ' + _format_students(skipped['no_recipients']), 'warning')
        if skipped.get('no_grade'):
            flash('Could not determine grade for: ' + _format_students(skipped['no_grade']), 'warning')
        if skipped.get('no_incident'):
            flash('No incidents found for: ' + _format_students(skipped['no_incident']), 'warning')
        if skipped.get('threshold_mismatch'):
            flash('Threshold mismatch for: ' + _format_students(skipped['threshold_mismatch']), 'info')
        if skipped.get('query_error'):
            flash('Query errors for ESIS: ' + _format_students(skipped['query_error']), 'error')
        if skipped.get('unknown'):
            flash('Skipped (unknown reason) for: ' + _format_students(skipped['unknown']), 'warning')
        if summary.get('failed'):
            flash('Email send errors for: ' + _format_students([item['student'] for item in summary['failed']]), 'error')

    redirect_target = request.referrer or url_for('admin_bp.dashboard')
    return redirect(redirect_target)

# ---------------- Incidents API ----------------
# ---------------- Export Endpoints ----------------
# ---------------- Parent Meeting Forms ----------------
# ---------------- Parent Acknowledgment Forms ----------------
# ---------------- Student Statement Forms ----------------
# ---------------- Staff Statement Forms ----------------
# ---------------- Safeguarding Concern Forms ----------------
# ---------------- Phone Violation Contract ----------------
# ---------------- Counseling Session Tracker ----------------
# ---------------- Behavior Contract ----------------
# ---------------- Student Conduct Pledge ----------------
# ---------------- Student Incidents Page ----------------
@behaviour_bp.route('/api/students/<string:esis>/action-plan', methods=['POST'])
@login_required
def generate_student_action_plan(esis):
    if not (session.get('is_admin') or session.get('role') == 'admin'):
        return jsonify({'success': False, 'error': 'Admin access required.'}), 403
    try:
        profile = _prepare_action_plan_profile(esis)
    except ValueError as err:
        return jsonify({'success': False, 'error': str(err)}), 404
    except Exception as err:  # pragma: no cover - defensive
        current_app.logger.error('Failed to prepare action plan profile: %s', err)
        return jsonify({'success': False, 'error': 'Unable to prepare student data.'}), 500

    try:
        plan_text = generate_action_plan(profile)
    except Exception as err:  # pragma: no cover - external dependency
        current_app.logger.error('Gemini action plan generation failed: %s', err)
        return jsonify({'success': False, 'error': 'Action plan generation failed. Please try again later.'}), 502

    return jsonify({'success': True, 'plan': plan_text})


