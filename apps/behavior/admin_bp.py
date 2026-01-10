# admin_bp.py

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify, current_app, abort
from werkzeug.security import generate_password_hash
from behaviour import (
    Teacher,
    Students,
    Incident,
    IncidentNotificationSetting,
    ExcuseNotificationSetting,
    OneDriveExportSetting,
    MsEmailMonitorSetting,
    SickLeaveWindowAttempt,
    SickLeaveGradeRecipient,
    AcademicTerm,
)
from behaviour_bp import (
    _preset_locations,
    _get_homerooms_grades,
    _parse_datetime_local,
    _load_academic_terms,
    ACADEMIC_TERM_SPECS,
)
try:
    from behaviour import TeacherRole
except Exception:
    TeacherRole = None
try:
    from behaviour import Suspension, Signature
except Exception:
    Suspension = None
    Signature = None
from extensions import db
from sqlalchemy import func
from datetime import datetime, timedelta
import csv
import io
import re
import requests
from uuid import uuid4
from functools import wraps
from typing import Optional
from dynamic_forms_service import ensure_dynamic_form_tables
import ms_auth_cache
from ms_email import send_mail

admin_bp = Blueprint('admin_bp', __name__, url_prefix='/admin')

# --- Hardcoded Admin Credentials ---
ADMIN_CREDENTIALS = {
    "mustafa": "mustafa",
    "danny": "danny",
}

GRADE_LEAD_GRADES = ("6", "7", "10", "11", "12")
ALLOWED_ROLES = (
    ("teacher", "admin", "pa", "slt", "administrator")
    + tuple(f"grade_lead_{grade}" for grade in GRADE_LEAD_GRADES)
)
ROLE_LABELS = {
    "teacher": "Teacher",
    "admin": "Admin",
    "pa": "PA (Excuse Viewer)",
    "slt": "SLT",
    "administrator": "Administrator",
    **{f"grade_lead_{grade}": f"Grade Lead (Grade {grade})" for grade in GRADE_LEAD_GRADES},
}
GRADE_FALLBACK_ALL = "ALL"
MS_EMAIL_PROFILES = {
    "behaviour": {
        "label": "Behaviour Notifications",
        "description": "Used for incident emails, behaviour alerts, and Microsoft monitor warnings.",
    },
    "absence": {
        "label": "Absence & Excuse Notifications",
        "description": "Used for teacher excuse confirmations, admin alerts, and sick-leave grade emails.",
    },
}


def _normalise_role(value: str) -> str:
    role = (value or "teacher").strip().lower()
    if role not in ALLOWED_ROLES:
        return "teacher"
    return role


def _normalise_grade_value(value: str) -> str:
    return (value or '').strip().upper()


def admin_login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_admin'):
            return redirect(url_for('admin_bp.login'))
        return f(*args, **kwargs)
    return decorated_function

def _ensure_dynamic_tables_exist():
    ensure_dynamic_form_tables()


def _ensure_incident_notification_table():
    try:
        IncidentNotificationSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_excuse_notification_table():
    try:
        ExcuseNotificationSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_onedrive_export_setting_table():
    try:
        OneDriveExportSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_ms_monitor_table():
    try:
        MsEmailMonitorSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_sick_leave_grade_table():
    try:
        SickLeaveGradeRecipient.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _get_ms_monitor_setting():
    _ensure_ms_monitor_table()
    setting = (
        db.session.query(MsEmailMonitorSetting)
        .order_by(MsEmailMonitorSetting.id.asc())
        .first()
    )
    if not setting:
        setting = MsEmailMonitorSetting()
    return setting


def _parse_email_list(raw: str):
    tokens = [token.strip() for token in re.split(r'[\s,;]+', raw or '') if token and token.strip()]
    return [token for token in tokens if EMAIL_PATTERN.match(token)]


def _parse_term_date_field(value: Optional[str], label: str):
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError(f"{label} must be in YYYY-MM-DD format.")

def _resolve_ms_profile(profile: str) -> str:
    slug = (profile or "").strip().lower()
    if slug not in MS_EMAIL_PROFILES:
        abort(404)
    return slug


def _send_ms_monitor_alert(setting: MsEmailMonitorSetting, status_payload: dict, error_text: Optional[str] = None, *, profile_label: str = "Behaviour Notifications"):
    if not setting.enabled or not setting.recipient_email:
        return

    status_ok = bool(status_payload.get('accounts')) and not error_text
    now = datetime.utcnow()
    setting.last_status_ok = status_ok
    setting.last_checked_at = now

    if status_ok:
        db.session.add(setting)
        db.session.commit()
        return

    if setting.last_alert_sent_at is None:
        should_alert = True
    else:
        should_alert = (now - setting.last_alert_sent_at) > timedelta(hours=6)

    if not should_alert:
        db.session.add(setting)
        db.session.commit()
        return

    subject = f"Behaviour App: {profile_label} email integration requires attention"
    reason = error_text or f"No Microsoft account is connected to the {profile_label} email integration."
    html_body = f"""
    <div style="font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding:16px; color:#0f172a;">
        <h2 style="margin:0 0 12px; color:#0f172a;">Microsoft Email Integration Alert</h2>
        <p style="margin:0 0 12px; color:#475569;">
            The Behaviour application detected that the Microsoft Graph email integration is not currently connected.
        </p>
        <p style="margin:0 0 12px; color:#dc2626; font-weight:600;">{reason}</p>
        <p style="margin:0 0 16px; color:#475569;">
            Visit the "Start Sign-In" button inside the Microsoft Email card to reauthenticate.
        </p>
        <p style="font-size:12px; color:#94a3b8;">This alert will repeat every 6 hours while the integration remains disconnected.</p>
    </div>
    """
    try:
        send_mail(setting.recipient_email, subject, html_body, profile="behaviour")
        setting.last_alert_sent_at = now
    except Exception as exc:
        current_app.logger.warning('Unable to send ms_email monitor alert: %s', exc)
    finally:
        db.session.add(setting)
        db.session.commit()

@admin_bp.route('/')
@admin_login_required
def dashboard():
    # Ensure roles table exists so role queries don't fail
    if TeacherRole is not None:
        try:
            # Create on default engine; bind points to same DB
            TeacherRole.__table__.create(bind=db.engine, checkfirst=True)
        except Exception:
            pass
    
    # Ensure dynamic form tables exist
    try:
        _ensure_dynamic_tables_exist()
    except Exception as e:
        flash(f'Error creating dynamic form tables: {e}', 'danger')

    notification_setting = None
    excuse_notification_setting = None
    onedrive_export_setting = None
    ms_monitor_setting = _get_ms_monitor_setting()
    ms_email_integrations = {}
    try:
        _ensure_incident_notification_table()
        notification_setting = (
            db.session.query(IncidentNotificationSetting)
            .order_by(IncidentNotificationSetting.id.asc())
            .first()
        )
    except Exception as e:
        flash(f'Error loading incident notification setting: {e}', 'warning')
    try:
        _ensure_excuse_notification_table()
        excuse_notification_setting = (
            db.session.query(ExcuseNotificationSetting)
            .order_by(ExcuseNotificationSetting.id.asc())
            .first()
        )
    except Exception as e:
        flash(f'Error loading excuse notification setting: {e}', 'warning')
    try:
        _ensure_onedrive_export_setting_table()
        onedrive_export_setting = (
            db.session.query(OneDriveExportSetting)
            .order_by(OneDriveExportSetting.id.asc())
            .first()
        )
    except Exception as exc:
        current_app.logger.warning('Unable to load OneDrive export setting: %s', exc)
    for slug, meta in MS_EMAIL_PROFILES.items():
        profile_ctx = {
            'status': {},
            'error': None,
            'requires_login': False,
            'token_error': None,
            'token_ready': False,
            'meta': meta,
        }
        try:
            profile_ctx['status'] = ms_auth_cache.status(profile=slug)
            token_ready, token_error = ms_auth_cache.token_ready(profile=slug)
            profile_ctx['token_ready'] = token_ready
            profile_ctx['token_error'] = token_error
            profile_ctx['requires_login'] = not token_ready
            if profile_ctx['requires_login']:
                _send_ms_monitor_alert(
                    ms_monitor_setting,
                    profile_ctx['status'],
                    token_error or f"{meta['label']} requires Microsoft sign-in.",
                    profile_label=meta['label'],
                )
        except Exception as exc:
            profile_ctx['error'] = str(exc)
            profile_ctx['requires_login'] = True
            _send_ms_monitor_alert(
                ms_monitor_setting,
                profile_ctx.get('status') or {},
                profile_ctx['error'],
                profile_label=meta['label'],
            )
        ms_email_integrations[slug] = profile_ctx

    try:
        _, detected_grades = _get_homerooms_grades()
        grade_options = sorted({_normalise_grade_value(g) for g in detected_grades if g})
    except Exception as exc:
        current_app.logger.warning('Unable to load grade list: %s', exc)
        grade_options = []
    if GRADE_FALLBACK_ALL not in grade_options:
        grade_options.insert(0, GRADE_FALLBACK_ALL)

    teachers = db.session.query(Teacher).order_by(Teacher.name).all()
    roles_map = {}
    if TeacherRole is not None:
        try:
            role_rows = db.session.query(TeacherRole).all()
            roles_map = {r.teacher_id: r.role for r in role_rows}
        except Exception:
            roles_map = {}
    try:
        SickLeaveWindowAttempt.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass
    sick_window_attempts = (
        db.session.query(SickLeaveWindowAttempt)
        .order_by(SickLeaveWindowAttempt.attempted_at.desc())
        .limit(20)
        .all()
    )
    try:
        _ensure_sick_leave_grade_table()
        sick_leave_grade_settings = (
            db.session.query(SickLeaveGradeRecipient)
            .order_by(SickLeaveGradeRecipient.grade.asc())
            .all()
        )
    except Exception as exc:
        current_app.logger.warning('Unable to load sick leave grade recipients: %s', exc)
        sick_leave_grade_settings = []
    academic_terms = []
    try:
        terms = _load_academic_terms()
        academic_terms = [
            {
                'slug': term.slug,
                'name': term.name,
                'start_date': term.start_date.strftime('%Y-%m-%d') if term.start_date else '',
                'end_date': term.end_date.strftime('%Y-%m-%d') if term.end_date else '',
            }
            for term in terms
        ]
    except Exception as exc:
        current_app.logger.warning('Unable to load academic terms: %s', exc)
    return render_template(
        'admin_dashboard.html',
        teachers=teachers,
        roles_map=roles_map,
        role_labels=ROLE_LABELS,
        allowed_roles=ALLOWED_ROLES,
        grade_options=grade_options,
        incident_notification_setting=notification_setting,
        excuse_notification_setting=excuse_notification_setting,
        onedrive_export_setting=onedrive_export_setting,
        ms_monitor_setting=ms_monitor_setting,
        ms_email_integrations=ms_email_integrations,
        sick_window_attempts=sick_window_attempts,
        sick_leave_grade_settings=sick_leave_grade_settings,
        academic_terms=academic_terms,
    )


@admin_bp.route('/terms/update', methods=['POST'])
@admin_login_required
def update_term_dates():
    slug = (request.form.get('slug') or '').strip()
    if not slug:
        flash('Select a term to update.', 'error')
        return redirect(url_for('admin_bp.dashboard') + '#insights')
    spec = next((entry for entry in ACADEMIC_TERM_SPECS if entry['slug'] == slug), None)
    if not spec:
        flash('Unknown academic term.', 'error')
        return redirect(url_for('admin_bp.dashboard') + '#insights')

    try:
        start_date = _parse_term_date_field(request.form.get('start_date'), 'Start date')
        end_date = _parse_term_date_field(request.form.get('end_date'), 'End date')
    except ValueError as exc:
        flash(str(exc), 'error')
        return redirect(url_for('admin_bp.dashboard') + '#insights')

    if start_date and end_date and end_date < start_date:
        flash('End date cannot be before start date.', 'error')
        return redirect(url_for('admin_bp.dashboard') + '#insights')

    term = db.session.get(AcademicTerm, slug)
    if not term:
        term = AcademicTerm(slug=slug, name=spec['name'])
    term.name = spec['name']
    term.start_date = start_date
    term.end_date = end_date
    db.session.add(term)
    db.session.commit()
    flash(f"{spec['name']} dates saved.", 'success')
    return redirect(url_for('admin_bp.dashboard') + '#insights')


@admin_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username and ADMIN_CREDENTIALS.get(username) == password:
            session['is_admin'] = True
            session['admin_name'] = username
            return redirect(url_for('admin_bp.dashboard'))
        else:
            flash('Invalid username or password.', 'danger')
    return render_template('admin_login.html')

@admin_bp.route('/logout')
def logout():
    session.pop('is_admin', None)
    session.pop('admin_name', None)
    flash('You have been logged out.', 'success')
    return redirect(url_for('admin_bp.login'))


@admin_bp.route('/incident-notification-email', methods=['POST'])
@admin_login_required
def update_incident_notification_email():
    email = (request.form.get('recipient_email') or '').strip()
    enabled = request.form.get('enabled') == 'on'

    _ensure_incident_notification_table()
    setting = (
        db.session.query(IncidentNotificationSetting)
        .order_by(IncidentNotificationSetting.id.asc())
        .first()
    )
    if not setting:
        setting = IncidentNotificationSetting()

    if enabled and not email:
        flash('Please provide an email address to enable incident notifications.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))

    setting.recipient_email = email or None
    setting.enabled = bool(email) and enabled
    db.session.add(setting)
    db.session.commit()

    if setting.enabled:
        flash(f'Incident notifications will be sent to {setting.recipient_email}.', 'success')
    else:
        flash('Incident notifications have been disabled.', 'info')

    return redirect(url_for('admin_bp.dashboard'))


@admin_bp.route('/ms-email/<profile>/status', methods=['GET'])
@admin_login_required
def ms_email_status(profile):
    slug = _resolve_ms_profile(profile)
    try:
        status_payload = ms_auth_cache.status(profile=slug)
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500
    try:
        flows = ms_auth_cache.get_device_flows(profile=slug)
    except Exception:
        flows = []
    token_ready, token_error = ms_auth_cache.token_ready(profile=slug)
    return jsonify({
        'ok': True,
        'status': status_payload,
        'flows': flows,
        'token_ready': token_ready,
        'token_error': token_error,
    })


@admin_bp.route('/ms-email/<profile>/start', methods=['POST'])
@admin_login_required
def ms_email_start(profile):
    slug = _resolve_ms_profile(profile)
    try:
        flow = ms_auth_cache.start_device_flow(profile=slug)
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 400
    return jsonify({'ok': True, 'flow': flow})


@admin_bp.route('/ms-email/monitor', methods=['POST'])
@admin_login_required
def update_ms_email_monitor():
    recipient = (request.form.get('monitor_email') or '').strip()
    enabled = request.form.get('enabled') == 'on'

    setting = _get_ms_monitor_setting()
    setting.recipient_email = recipient or None
    setting.enabled = enabled and bool(setting.recipient_email)
    if not setting.enabled:
        setting.last_alert_sent_at = None
        setting.last_status_ok = None
    db.session.add(setting)
    db.session.commit()

    if setting.enabled:
        flash('Email integration alerts will be sent when Microsoft auth needs attention.', 'success')
    else:
        flash('Email integration alerts have been disabled.', 'info')
    return redirect(url_for('admin_bp.dashboard'))


@admin_bp.route('/excuse-notification-email', methods=['POST'])
@admin_login_required
def update_excuse_notification_email():
    raw_emails = (request.form.get('recipient_emails') or '').strip()
    enabled = request.form.get('enabled') == 'on'

    _ensure_excuse_notification_table()
    recipients = _parse_email_list(raw_emails)

    if enabled and not recipients:
        flash('Please enter at least one email before enabling excuse notifications.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))

    setting = (
        db.session.query(ExcuseNotificationSetting)
        .order_by(ExcuseNotificationSetting.id.asc())
        .first()
    )
    if not setting:
        setting = ExcuseNotificationSetting()

    setting.recipient_emails = raw_emails or None
    setting.enabled = enabled and bool(recipients)
    db.session.add(setting)
    db.session.commit()

    if setting.enabled:
        flash('Excuse notifications will be emailed to the selected recipients.', 'success')
    else:
        flash('Excuse notifications have been disabled.', 'info')

    return redirect(url_for('admin_bp.dashboard'))


@admin_bp.route('/onedrive-export/settings', methods=['POST'])
@admin_login_required
def update_onedrive_export_settings():
    recipients_raw = (request.form.get('recipient_emails') or '').strip()
    enabled = request.form.get('enabled') == 'on'
    ms_profile = (request.form.get('ms_profile') or 'absence').strip() or 'absence'

    _ensure_onedrive_export_setting_table()
    setting = (
        db.session.query(OneDriveExportSetting)
        .order_by(OneDriveExportSetting.id.asc())
        .first()
    )
    if not setting:
        setting = OneDriveExportSetting()

    recipients = _parse_email_list(recipients_raw)
    if enabled and not recipients:
        flash('Add at least one email address to enable OneDrive export alerts.', 'warning')
        return redirect(url_for('admin_bp.dashboard') + '#onedrive-export')

    setting.recipient_emails = recipients_raw or None
    setting.enabled = bool(recipients) and enabled
    setting.ms_profile = ms_profile
    db.session.add(setting)
    db.session.commit()

    if setting.enabled:
        flash('OneDrive archive alerts will be emailed to your selected addresses.', 'success')
    else:
        flash('OneDrive archive alerts have been disabled.', 'info')

    return redirect(url_for('admin_bp.dashboard') + '#onedrive-export')


@admin_bp.route('/sick-leave/grade-recipients', methods=['POST'])
@admin_login_required
def update_sick_leave_grade_recipients():
    grade = (request.form.get('grade') or '').strip()
    recipients_raw = (request.form.get('recipient_emails') or '').strip()

    if not grade:
        flash('Grade is required to save sick leave emails.', 'warning')
        return redirect(url_for('admin_bp.dashboard') + '#sick-grade-emails')

    grade_key = _normalise_grade_value(grade)
    _ensure_sick_leave_grade_table()
    setting = db.session.get(SickLeaveGradeRecipient, grade_key)

    if not recipients_raw:
        if setting:
            db.session.delete(setting)
            db.session.commit()
            flash(f'Sick leave emails cleared for Grade {grade_key}.', 'success')
        else:
            flash('No emails to clear for that grade.', 'info')
        return redirect(url_for('admin_bp.dashboard') + '#sick-grade-emails')

    if not setting:
        setting = SickLeaveGradeRecipient(grade=grade_key)
    setting.recipient_emails = recipients_raw
    db.session.add(setting)
    db.session.commit()
    flash(f'Sick leave approval emails saved for Grade {grade_key}.', 'success')
    return redirect(url_for('admin_bp.dashboard') + '#sick-grade-emails')

@admin_bp.route('/upload/teachers', methods=['POST'])
@admin_login_required
def upload_teachers():
    file = request.files.get('file')
    if not file or file.filename == '':
        flash('No file selected.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))

    try:
        existing_emails = {
            (email or '').strip().lower()
            for (email,) in db.session.query(Teacher.email).all()
            if email
        }
        csv_emails = set()
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.reader(stream)
        next(csv_reader) # Skip header row
        teachers_to_add = []
        skipped_rows = []
        for idx, row in enumerate(csv_reader, start=2):
            if not row or all(not cell.strip() for cell in row):
                continue
            if len(row) < 3:
                skipped_rows.append(f'Row {idx}: expected name,email,password[,subject][,grade].')
                continue
            name = (row[0] or '').strip()
            email = (row[1] or '').strip().lower()
            password = (row[2] or '').strip()
            subject = (row[3] or '').strip() if len(row) > 3 else ''
            grade = _normalise_grade_value(row[4] if len(row) > 4 else '')
            if not name or not email or not password:
                skipped_rows.append(f'Row {idx}: name, email, and password are required.')
                continue
            if email in existing_emails:
                skipped_rows.append(f'Row {idx}: {email} already exists, skipped.')
                continue
            if email in csv_emails:
                skipped_rows.append(f'Row {idx}: duplicate email {email} found in file, skipped.')
                continue
            try:
                hashed_password = generate_password_hash(password)
            except Exception:
                hashed_password = password
            teachers_to_add.append(Teacher(
                name=name,
                email=email,
                password=hashed_password,
                subject=subject or None,
                grade=grade or None
            ))
            csv_emails.add(email)

        if not teachers_to_add:
            flash('No valid teacher rows found in CSV.', 'warning')
        else:
            db.session.bulk_save_objects(teachers_to_add)
            db.session.commit()
            flash(f'Successfully added {len(teachers_to_add)} teacher(s)!', 'success')
        if skipped_rows:
            flash('Some rows were skipped:<br>' + '<br>'.join(skipped_rows), 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'An error occurred: {e}', 'danger')
        
    return redirect(url_for('admin_bp.dashboard'))

@admin_bp.route('/upload/students', methods=['POST'])
@admin_login_required
def upload_students():
    file = request.files.get('file')
    if not file or file.filename == '':
        flash('No file selected.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.reader(stream)
        next(csv_reader) # Skip header row
        
        added = 0
        updated = 0
        skipped_rows = []
        for idx, row in enumerate(csv_reader, start=2):
            # Ignore empty lines
            if not row or all(not cell.strip() for cell in row):
                continue

            if len(row) < 3:
                skipped_rows.append(f'Row {idx}: missing columns (expected esis,name,homeroom)')
                continue

            esis, name, homeroom = (cell.strip() for cell in row[:3])

            if not esis or not name:
                skipped_rows.append(f'Row {idx}: ESIS and name are required.')
                continue

            student = db.session.query(Students).filter_by(esis=esis).first()
            if student:
                student.name = name
                student.homeroom = homeroom or None
                updated += 1
            else:
                db.session.add(Students(esis=esis, name=name, homeroom=homeroom or None))
                added += 1
        
        db.session.commit()

        status_bits = []
        if added:
            status_bits.append(f'added {added}')
        if updated:
            status_bits.append(f'updated {updated}')
        if not status_bits:
            status_bits.append('processed 0 records')
        flash(f'Successfully {", ".join(status_bits)} from CSV.', 'success')

        if skipped_rows:
            flash('Some rows were skipped:<br>' + '<br>'.join(skipped_rows), 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'An error occurred: {e}', 'danger')
        
    return redirect(url_for('admin_bp.dashboard'))

@admin_bp.route('/students/upsert', methods=['POST'])
@admin_login_required
def upsert_student_single():
    esis = (request.form.get('esis') or '').strip()
    name = (request.form.get('name') or '').strip()
    homeroom = (request.form.get('homeroom') or '').strip() or None

    if not esis or not name:
        flash('ESIS ID and student name are required.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))

    try:
        student = db.session.query(Students).filter_by(esis=esis).first()
        if student:
            student.name = name
            student.homeroom = homeroom
            message = f'Student {esis} updated successfully.'
        else:
            student = Students(esis=esis, name=name, homeroom=homeroom)
            db.session.add(student)
            message = f'Student {esis} added successfully.'
        db.session.commit()
        flash(message, 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Unable to save student: {e}', 'danger')

    return redirect(url_for('admin_bp.dashboard'))

@admin_bp.route('/students/lookup', methods=['GET'])
@admin_login_required
def lookup_student():
    esis = (request.args.get('esis') or '').strip()
    if not esis:
        return jsonify({'found': False, 'error': 'Missing ESIS ID.'}), 400

    student = db.session.query(Students).filter_by(esis=esis).first()
    if not student:
        return jsonify({'found': False})

    return jsonify({
        'found': True,
        'student': {
            'id': student.id,
            'esis': student.esis,
            'name': student.name,
            'homeroom': student.homeroom or ''
        }
    })


@admin_bp.route('/init/roles', methods=['POST'])
@admin_login_required
def init_roles_table():
    if TeacherRole is None:
        flash('TeacherRole model not available.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))
    try:
        # Create the table on the teachers bind if it does not exist
        TeacherRole.__table__.create(bind=db.engine, checkfirst=True)
        flash('Roles table is ready.', 'success')
    except Exception as e:
        flash(f'Error preparing roles table: {e}', 'danger')
    return redirect(url_for('admin_bp.dashboard'))

# ----- Manage teachers -----
@admin_bp.route('/teachers/add', methods=['POST'])
@admin_login_required
def add_teacher():
    name = (request.form.get('name') or '').strip()
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''
    role = _normalise_role(request.form.get('role') or 'teacher')
    subject = (request.form.get('subject') or '').strip()
    grade = _normalise_grade_value(request.form.get('grade'))
    if not name or not email or not password:
        flash('Name, email, and password are required.', 'warning')
        return redirect(url_for('admin_bp.dashboard'))
    try:
        pwd = generate_password_hash(password)
        t = Teacher(name=name, email=email, password=pwd, subject=subject or None, grade=grade or None)
        db.session.add(t)
        db.session.commit()
        if TeacherRole is not None:
            try:
                TeacherRole.__table__.create(bind=db.engine, checkfirst=True)
                db.session.add(TeacherRole(teacher_id=t.id, role=role))
                db.session.commit()
            except Exception:
                db.session.rollback()
        flash('Teacher added.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error adding teacher: {e}', 'danger')
    return redirect(url_for('admin_bp.dashboard'))


@admin_bp.route('/teachers/<int:teacher_id>/update', methods=['POST'])
@admin_login_required
def update_teacher(teacher_id):
    name = (request.form.get('name') or '').strip()
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password')  # optional
    role = _normalise_role(request.form.get('role') or '')
    subject = (request.form.get('subject') or '').strip()
    grade = _normalise_grade_value(request.form.get('grade'))
    try:
        t = db.session.get(Teacher, teacher_id)
        if not t:
            flash('Teacher not found.', 'warning')
            return redirect(url_for('admin_bp.dashboard'))
        if name:
            t.name = name
        if email:
            t.email = email
        t.subject = subject or None
        t.grade = grade or None
        if password:
            try:
                t.password = generate_password_hash(password)
            except Exception:
                t.password = password
        db.session.commit()

        if TeacherRole is not None:
            try:
                TeacherRole.__table__.create(bind=db.engine, checkfirst=True)
                role_row = db.session.query(TeacherRole).filter_by(teacher_id=teacher_id).first()
                if role_row:
                    role_row.role = role
                else:
                    db.session.add(TeacherRole(teacher_id=teacher_id, role=role))
                db.session.commit()
            except Exception:
                db.session.rollback()
        flash('Teacher updated.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error updating teacher: {e}', 'danger')
    return redirect(url_for('admin_bp.dashboard'))


@admin_bp.route('/teachers/<int:teacher_id>/delete', methods=['POST'])
@admin_login_required
def delete_teacher(teacher_id):
    try:
        t = db.session.get(Teacher, teacher_id)
        if not t:
            flash('Teacher not found.', 'warning')
            return redirect(url_for('admin_bp.dashboard'))
        # Prevent deleting if incidents exist to avoid orphaned records
        count = db.session.query(Incident.id).filter_by(teacher_id=teacher_id).count()
        if count > 0:
            flash(f'Cannot delete teacher with {count} incident(s). Reassign or remove incidents first.', 'warning')
            return redirect(url_for('admin_bp.dashboard'))
        db.session.delete(t)
        db.session.commit()
        if TeacherRole is not None:
            try:
                role_row = db.session.query(TeacherRole).filter_by(teacher_id=teacher_id).first()
                if role_row:
                    db.session.delete(role_row)
                    db.session.commit()
            except Exception:
                db.session.rollback()
        flash('Teacher deleted.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting teacher: {e}', 'danger')
    return redirect(url_for('admin_bp.dashboard'))


# ---- API: Students aggregated by incident count ----
@admin_bp.route('/api/student-incidents', methods=['GET'])
@admin_login_required
def api_student_incidents():
    try:
        min_count = request.args.get('min_count', type=int)
        max_count = request.args.get('max_count', type=int)
        search = (request.args.get('search') or '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)

        name_expr = func.coalesce(func.max(Students.name), func.max(Incident.name))
        homeroom_expr = func.coalesce(func.max(Students.homeroom), func.max(Incident.homeroom))

        q = db.session.query(
            Incident.esis.label('esis'),
            name_expr.label('name'),
            homeroom_expr.label('homeroom'),
            func.count(Incident.id).label('count'),
            func.max(Incident.date_of_incident).label('last_date')
        ).outerjoin(Students, Students.esis == Incident.esis)

        if search:
            like = f"%{search}%"
            q = q.filter(
                (Incident.esis.ilike(like)) |
                (Students.name.ilike(like)) |
                (Incident.name.ilike(like))
            )

        q = q.group_by(Incident.esis)

        if min_count is not None:
            q = q.having(func.count(Incident.id) >= min_count)
        if max_count is not None:
            q = q.having(func.count(Incident.id) <= max_count)

        # Default order: highest count first
        q = q.order_by(func.count(Incident.id).desc(), func.max(Incident.date_of_incident).desc())

        total = q.count()
        rows = q.limit(per_page).offset((page - 1) * per_page).all()

        data = []
        for esis, name, homeroom, count_val, last_dt in rows:
            data.append({
                'esis': esis,
                'name': name or '',
                'homeroom': homeroom or '',
                'count': int(count_val or 0),
                'last_date': last_dt.strftime('%Y-%m-%d %H:%M') if last_dt else ''
            })

        return {
            'data': data,
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page
        }
    except Exception as e:
        return {'error': str(e)}, 500


# ---- Super Admin: Delete Incident ----
@admin_bp.route('/incidents/<int:incident_id>/delete', methods=['POST'])
@admin_login_required
def delete_incident_admin(incident_id):
    try:
        inc = db.session.get(Incident, incident_id)
        if not inc:
            flash('Incident not found.', 'warning')
            return redirect(url_for('admin_bp.dashboard'))
        # Remove related signatures if model available
        if Signature is not None:
            try:
                db.session.query(Signature).filter_by(entity_type='incident', entity_id=incident_id).delete(synchronize_session=False)
            except Exception:
                db.session.rollback()
        db.session.delete(inc)
        db.session.commit()
        flash('Incident deleted.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting incident: {e}', 'danger')
    # Redirect back to dashboard list
    return redirect(url_for('behaviour_bp.behaviour_dashboard'))


# ---- Super Admin: Delete Suspension ----
@admin_bp.route('/suspensions/<int:suspension_id>/delete', methods=['POST'])
@admin_login_required
def delete_suspension_admin(suspension_id):
    if Suspension is None:
        flash('Suspension model not available.', 'danger')
        return redirect(url_for('behaviour_bp.behaviour_dashboard') + '#tab=suspensions')
    try:
        s = db.session.get(Suspension, suspension_id)
        if not s:
            flash('Suspension not found.', 'warning')
            return redirect(url_for('behaviour_bp.behaviour_dashboard') + '#tab=suspensions')
        if Signature is not None:
            try:
                db.session.query(Signature).filter_by(entity_type='suspension', entity_id=suspension_id).delete(synchronize_session=False)
            except Exception:
                db.session.rollback()
        db.session.delete(s)
        db.session.commit()
        flash('Suspension deleted.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting suspension: {e}', 'danger')
    return redirect(url_for('behaviour_bp.behaviour_dashboard') + '#tab=suspensions')

@admin_bp.route('/incidents/<int:incident_id>/edit', methods=['GET', 'POST'])
@admin_login_required
def edit_incident(incident_id):
    incident = db.session.query(Incident).get_or_404(incident_id)
    
    if request.method == 'POST':
        incident.esis = request.form.get('esis')
        incident.name = request.form.get('name')
        incident.homeroom = request.form.get('homeroom')
        incident.date_of_incident = _parse_datetime_local(request.form.get('date_of_incident'))
        incident.place_of_incident = request.form.get('place_of_incident')
        incident.incident_description = request.form.get('incident_description')
        incident.incident_grade = request.form.get('incident_grade')
        incident.action_taken = request.form.get('action_taken')
        
        db.session.commit()
        flash('Incident updated successfully!', 'success')
        return redirect(url_for('behaviour_bp.view_incident_report', incident_id=incident.id))

    # GET request
    locations = _preset_locations()
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
        "Contact the PLO to inform the parent of the student’s challenging behavior.",
        "Parents meeting has been held.",
        "Grade level lead reports the case to the head of section.",
        "The head of section reports the case to the behavior coordinator.",
        "Creating an intervention plan by the behavior coordinator."
    ]
    homerooms, grades = _get_homerooms_grades()

    return render_template(
        'add_behaviour.html',
        incident=incident,
        locations=locations,
        incident_grades=incident_grades,
        incidents_by_grade=incidents_by_grade,
        grade_by_incident=grade_by_incident,
        actions_taken=actions_taken,
        grades=grades,
        teacher_name=incident.teacher.name
    )
EMAIL_PATTERN = re.compile(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
