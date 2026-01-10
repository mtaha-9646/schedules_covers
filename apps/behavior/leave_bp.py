from datetime import datetime, date, time, timedelta
from pathlib import Path
from uuid import uuid4
import re
import os
import requests
from zoneinfo import ZoneInfo

from sqlalchemy import inspect, text, or_, case
from sqlalchemy.orm import joinedload

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
    current_app,
    send_file,
)
from markupsafe import escape
from werkzeug.utils import secure_filename
from onedrive_exports import (
    export_excuse_to_onedrive,
    get_onedrive_setting,
    remove_exported_attachment,
)

from auth import login_required
from behaviour import (
    Teacher,
    TeacherExcuse,
    TeacherExcuseMessage,
    ExcuseNotificationSetting,
    SickLeaveWindowAttempt,
    SickLeaveGradeRecipient,
)
from extensions import db
from ms_email import send_mail

leave_bp = Blueprint('leave_bp', __name__, url_prefix='/absence')

UAE_TZ = ZoneInfo('Asia/Dubai')
SICK_WINDOW_START = time(5, 30)
SICK_WINDOW_END = time(8, 0)

LEAVE_TYPE_SICK = 'sickleave'
LEAVE_TYPE_CONFERENCE = 'conference_offsite'
LEAVE_TYPE_TRAINING = 'training_offsite'
LEAVE_TYPE_EARLY = 'early_leave_request'

VALID_LEAVE_TYPES = (
    LEAVE_TYPE_SICK,
    LEAVE_TYPE_CONFERENCE,
    LEAVE_TYPE_TRAINING,
    LEAVE_TYPE_EARLY,
)
VALID_STATUSES = ('pending', 'approved', 'rejected', 'invalid')
LEAVE_TYPE_LABELS = {
    LEAVE_TYPE_SICK: 'Sick Leave',
    LEAVE_TYPE_CONFERENCE: 'Conference Outside School',
    LEAVE_TYPE_TRAINING: 'Training Outside School',
    LEAVE_TYPE_EARLY: 'Early Leave Request',
}
TIMED_LEAVE_TYPES = {LEAVE_TYPE_CONFERENCE, LEAVE_TYPE_TRAINING}
LEGACY_ABSENCE_TYPES = {'excuse', 'personal'}
ABSENCE_EMAIL_PROFILE = 'absence'
SICK_NOTE_UPLOAD_SUBDIR = Path('uploads') / 'sickleave'
SICK_NOTE_ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'heic', 'doc', 'docx'}
SICK_NOTE_MAX_SIZE_MB = 10
ATTACHMENT_DUE_DAYS = 5
ATTACHMENT_REMINDER_LIMIT = 5
ATTACHMENT_REMINDER_INTERVAL_HOURS = 24
ATTACHMENT_STATUS_LABELS = {
    TeacherExcuse.ATTACHMENT_STATUS_NOT_REQUIRED: 'Not required',
    TeacherExcuse.ATTACHMENT_STATUS_MISSING: 'Missing',
    TeacherExcuse.ATTACHMENT_STATUS_SUBMITTED: 'Awaiting review',
    TeacherExcuse.ATTACHMENT_STATUS_APPROVED: 'Approved',
    TeacherExcuse.ATTACHMENT_STATUS_DECLINED: 'Closed',
}
DEFAULT_LEAVE_APPROVAL_WEBHOOK_URL = "https://coveralreef.pythonanywhere.com/external/leave-approvals"


def _ensure_table():
    """Create the teacher_excuses table when the feature is accessed for the first time."""
    try:
        TeacherExcuse.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass
    _ensure_optional_columns()
    _ensure_sick_window_table()
    _ensure_grade_recipient_table()
    _ensure_message_table()


def _ensure_message_table():
    try:
        TeacherExcuseMessage.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_optional_columns():
    """Add newly introduced columns when running on existing databases."""
    try:
        inspector = inspect(db.engine)
        columns = {col['name'] for col in inspector.get_columns('teacher_excuses')}
        migrations = []
        if 'end_date' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN end_date DATE")
        if 'start_time' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN start_time TIME")
        if 'end_time' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN end_time TIME")
        if 'attachment_required' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_required BOOLEAN NOT NULL DEFAULT 0")
        if 'attachment_status' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_status VARCHAR(20) NOT NULL DEFAULT 'not_required'")
        if 'attachment_path' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_path VARCHAR(500)")
        if 'attachment_original_name' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_original_name VARCHAR(255)")
        if 'attachment_uploaded_at' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_uploaded_at DATETIME")
        if 'attachment_due_at' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_due_at DATETIME")
        if 'attachment_reminder_count' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_reminder_count INTEGER NOT NULL DEFAULT 0")
        if 'attachment_last_reminder_at' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_last_reminder_at DATETIME")
        if 'attachment_export_path' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_export_path VARCHAR(500)")
        if 'attachment_exported_at' not in columns:
            migrations.append("ALTER TABLE teacher_excuses ADD COLUMN attachment_exported_at DATETIME")
        for statement in migrations:
            with db.engine.begin() as connection:
                connection.execute(text(statement))
    except Exception as exc:
        current_app.logger.warning('Unable to validate teacher_excuses columns: %s', exc)

def _ensure_sick_window_table():
    try:
        SickLeaveWindowAttempt.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_grade_recipient_table():
    try:
        SickLeaveGradeRecipient.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _ensure_notification_setting_table():
    try:
        ExcuseNotificationSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def _parse_recipients(raw: str):
    tokens = re.split(r'[\s,;]+', raw or '')
    return [token.strip() for token in tokens if token and token.strip()]


def _load_excuse_messages(excuses):
    ids = [excuse.id for excuse in excuses or []]
    if not ids:
        return {}
    rows = (
        db.session.query(TeacherExcuseMessage)
        .filter(TeacherExcuseMessage.excuse_id.in_(ids))
        .order_by(TeacherExcuseMessage.created_at.asc())
        .all()
    )
    result = {}
    for row in rows:
        result.setdefault(row.excuse_id, []).append(row)
    return result


def _leave_type_label(value: str) -> str:
    key = (value or '').lower()
    return LEAVE_TYPE_LABELS.get(key, (value or 'Leave').replace('_', ' ').title())


def _requires_time_window(leave_type: str) -> bool:
    return (leave_type or '').lower() in TIMED_LEAVE_TYPES


def _format_time_label(value: time | None) -> str:
    return value.strftime('%H:%M') if value else '--'


def _serialize_time_value(value: time | None) -> str | None:
    return value.strftime('%H:%M') if value else None


def _iso_or_none(value: date | datetime | None) -> str | None:
    return value.isoformat() if value else None


_LEAVE_TYPE_NORMALIZATION = {
    'sickleave': 'sick',
    'conference_offsite': 'conference',
    'training_offsite': 'training',
    'early_leave_request': 'early',
}


def _normalize_leave_type(value: str | None) -> str:
    key = (value or '').lower()
    if not key:
        return 'unknown'
    return _LEAVE_TYPE_NORMALIZATION.get(key, key)


def _format_multiline(text: str) -> str:
    safe = escape(text or '')
    return safe.replace('\n', '<br />')


def _parse_time_value(raw: str):
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, '%H:%M').time()
    except ValueError:
        return None


def _attachment_root() -> Path:
    root = Path(current_app.root_path) / SICK_NOTE_UPLOAD_SUBDIR
    root.mkdir(parents=True, exist_ok=True)
    return root


def _store_sick_note(file_obj):
    if not file_obj or not file_obj.filename:
        raise ValueError('Select a file to upload.')
    filename = secure_filename(file_obj.filename)
    if not filename:
        raise ValueError('Select a valid file to upload.')
    extension = Path(filename).suffix.lstrip('.').lower()
    if extension not in SICK_NOTE_ALLOWED_EXTENSIONS:
        raise ValueError(f'Allowed file types: {", ".join(sorted(SICK_NOTE_ALLOWED_EXTENSIONS))}.')
    file_obj.stream.seek(0, os.SEEK_END)
    size_bytes = file_obj.stream.tell()
    file_obj.stream.seek(0)
    max_bytes = SICK_NOTE_MAX_SIZE_MB * 1024 * 1024
    if size_bytes > max_bytes:
        raise ValueError(f'Files must be {SICK_NOTE_MAX_SIZE_MB} MB or smaller.')
    root = _attachment_root()
    unique_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid4().hex}{Path(filename).suffix.lower()}"
    destination = root / unique_name
    file_obj.save(destination)
    relative = destination.relative_to(Path(current_app.root_path))
    return relative.as_posix(), Path(filename).name


def _delete_existing_attachment(excuse: TeacherExcuse):
    if not excuse or not excuse.attachment_path:
        return
    file_path = Path(current_app.root_path) / excuse.attachment_path
    try:
        file_path.unlink(missing_ok=True)
    except Exception as exc:
        current_app.logger.warning('Unable to remove attachment %s: %s', file_path, exc)
    excuse.attachment_path = None
    excuse.attachment_original_name = None
    excuse.attachment_uploaded_at = None
    excuse.attachment_status = TeacherExcuse.ATTACHMENT_STATUS_MISSING


def _resolve_attachment_file(excuse: TeacherExcuse) -> Path | None:
    if not excuse or not excuse.attachment_path:
        return None
    candidate = Path(current_app.root_path) / excuse.attachment_path
    try:
        candidate.relative_to(Path(current_app.root_path))
    except ValueError:
        # Path traversal detected; ignore.
        return None
    return candidate


def _attachment_deadline(excuse: TeacherExcuse):
    if not excuse or not excuse.attachment_required:
        return None
    if excuse.attachment_due_at:
        return excuse.attachment_due_at
    base = excuse.created_at or datetime.utcnow()
    return base + timedelta(days=ATTACHMENT_DUE_DAYS)


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


def _send_excuse_notification(excuse: TeacherExcuse):
    """Send an email to subscribed admins when a new excuse is submitted."""
    try:
        _ensure_notification_setting_table()
        setting = (
            db.session.query(ExcuseNotificationSetting)
            .order_by(ExcuseNotificationSetting.id.asc())
            .first()
        )
    except Exception as exc:
        current_app.logger.warning('Unable to load excuse notification setting: %s', exc)
        return

    if not setting or not setting.enabled:
        return

    recipients = [addr for addr in dict.fromkeys(_parse_recipients(setting.recipient_emails or '')) if addr]
    if not recipients:
        return

    teacher_name = excuse.teacher.name if excuse.teacher else 'Unknown Teacher'
    submitted_at = excuse.created_at.strftime('%d %b %Y %H:%M')
    detail_url = url_for('leave_bp.manage_requests', _external=True)
    status_badge = excuse.status.title()
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    leave_type_label = getattr(excuse, 'type_label', _leave_type_label(excuse.leave_type))

    html_body = f"""
    <div style="font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding:16px; color:#0f172a;">
        <h2 style="margin:0 0 12px; color:#0f172a;">Teacher Absence Excuse Submitted</h2>
        <p style="margin:0 0 16px; color:#475569;">
            A new sick leave / excuse has been logged inside the Behaviour app. Review the details below.
        </p>
        <table cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse; margin-bottom:16px;">
            <tr>
                <td style="padding:8px 0; width:160px; font-weight:600; color:#0f172a;">Teacher</td>
                <td style="padding:8px 0; color:#0f172a;">{teacher_name}</td>
            </tr>
            <tr>
                <td style="padding:8px 0; font-weight:600; color:#0f172a;">Dates</td>
                <td style="padding:8px 0; color:#0f172a;">{leave_dates}</td>
            </tr>
            <tr>
                <td style="padding:8px 0; font-weight:600; color:#0f172a;">Type</td>
                <td style="padding:8px 0; color:#0f172a;">{leave_type_label}</td>
            </tr>
            <tr>
                <td style="padding:8px 0; font-weight:600; color:#0f172a;">Status</td>
                <td style="padding:8px 0; color:#0f172a;">{status_badge}</td>
            </tr>
            <tr>
                <td style="padding:8px 0; font-weight:600; color:#0f172a;">Submitted</td>
                <td style="padding:8px 0; color:#0f172a;">{submitted_at}</td>
            </tr>
        </table>
        <div style="padding:16px; border-radius:12px; background:#eef2ff; color:#312e81; margin-bottom:16px;">
            <strong>Reason:</strong><br/>
            {excuse.reason}
        </div>
        <a href="{detail_url}" style="display:inline-block; padding:12px 24px; border-radius:999px; background:#2563eb; color:#ffffff; text-decoration:none; font-weight:600;">
            Review Request
        </a>
        <p style="margin-top:20px; font-size:12px; color:#94a3b8;">
            Sent automatically from the Behaviour absence block.
        </p>
    </div>
    """
    date_summary = leave_dates.replace(' ', ' ')
    subject = f"Teacher excuse submitted - {teacher_name} ({date_summary})"

    try:
        send_mail(recipients, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Failed to send excuse notification: %s', exc)


def _record_sick_window_attempt(teacher_id: int, leave_date, reason: str):
    if not teacher_id or not leave_date:
        return
    try:
        _ensure_sick_window_table()
        attempt = SickLeaveWindowAttempt(
            teacher_id=teacher_id,
            leave_date=leave_date,
            reason_preview=(reason or '')[:500],
        )
        db.session.add(attempt)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning('Unable to record sick leave window attempt: %s', exc)


def _normalise_grade_value(value: str) -> str:
    return (value or '').strip().upper()


def _send_grade_sick_leave_alert(excuse: TeacherExcuse):
    if excuse.status != 'approved' or (excuse.leave_type or '').lower() != LEAVE_TYPE_SICK:
        return

    teacher = excuse.teacher
    raw_grade = _normalise_grade_value(getattr(teacher, 'grade', None) if teacher else '')
    grade_label = raw_grade or 'ALL'

    try:
        _ensure_grade_recipient_table()
        setting = None
        if raw_grade:
            setting = db.session.get(SickLeaveGradeRecipient, raw_grade)
            if setting is None:
                setting = (
                    db.session.query(SickLeaveGradeRecipient)
                    .filter(SickLeaveGradeRecipient.grade.ilike(raw_grade))
                    .first()
                )
        if not setting:
            setting = db.session.get(SickLeaveGradeRecipient, 'ALL')
            if setting is None:
                setting = (
                    db.session.query(SickLeaveGradeRecipient)
                    .filter(SickLeaveGradeRecipient.grade.ilike('all'))
                    .first()
                )
            if setting and not raw_grade:
                grade_label = 'ALL'
    except Exception as exc:
        current_app.logger.warning('Unable to load sick leave grade setting: %s', exc)
        return

    if not setting:
        return
    recipients = _parse_recipients(setting.recipient_emails or '')
    if not recipients:
        return

    teacher_name = teacher.name if teacher else 'Unknown Teacher'
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    reason = excuse.reason or '--'
    admin_comment = excuse.admin_comment or ''
    review_url = url_for('leave_bp.manage_requests', _external=True)
    subject = f"Sick Leave Approved - {grade_label} - {teacher_name}"
    html_body = f"""
    <div style="font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px;color:#0f172a;">
        <p style="margin:0 0 12px;color:#475569;">Team {grade_label}, a sick leave request has been approved.</p>
        <table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;margin-bottom:16px;">
            <tr>
                <td style="padding:8px 0;width:150px;font-weight:600;">Teacher</td>
                <td style="padding:8px 0;">{teacher_name}</td>
            </tr>
            <tr>
                <td style="padding:8px 0;font-weight:600;">Dates</td>
                <td style="padding:8px 0;">{leave_dates}</td>
            </tr>
            <tr>
                <td style="padding:8px 0;font-weight:600;">Reason</td>
                <td style="padding:8px 0;">{reason}</td>
            </tr>
            <tr>
                <td style="padding:8px 0;font-weight:600;">Admin Notes</td>
                <td style="padding:8px 0;">{admin_comment or '--'}</td>
            </tr>
        </table>
        <a href="{review_url}" style="display:inline-block;padding:10px 20px;border-radius:999px;background:#2563eb;color:#fff;font-weight:600;text-decoration:none;">View Requests</a>
    </div>
    """
    try:
        send_mail(recipients, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to send grade sick leave alert: %s', exc)


def _send_drive_contact_alert(subject: str, html_body: str):
    setting = None
    try:
        setting = get_onedrive_setting()
    except Exception as exc:
        current_app.logger.warning('Unable to load OneDrive contacts: %s', exc)
    if not setting or not setting.enabled:
        return
    recipients = setting.email_list()
    if not recipients:
        return
    try:
        send_mail(recipients, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to notify drive personnel: %s', exc)


def _notify_drive_attachment_event(excuse: TeacherExcuse, export_info: dict | None, action_label: str):
    folder_item = export_info.get('folder') if export_info else None
    folder_name = (export_info or {}).get('folder_name')
    folder_url = folder_item.web_url if folder_item else ''
    teacher_name = excuse.teacher.name if excuse.teacher else 'Unknown Teacher'
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    action_copy = (action_label or 'updated').replace('_', ' ')
    archive_window = folder_name or 'current sick leave window'
    subject = f"OneDrive Sick Leave archive update - {teacher_name}"
    if folder_url:
        link_block = (
            f'<div style="display:flex;gap:12px;flex-wrap:wrap;margin:0 0 16px;">'
            f'<a href="{folder_url}" '
            'style="padding:10px 18px;border-radius:999px;background:#0ea5e9;color:#fff;text-decoration:none;font-weight:600;" '
            'target="_blank" rel="noopener">Open folder</a>'
            '</div>'
        )
    else:
        link_block = '<p style="margin:0 0 12px;color:#94a3b8;">Attachment stored locally; OneDrive link pending.</p>'

    html_body = f"""
    <div style="font-family:'Inter','Segoe UI',sans-serif;color:#0f172a;padding:16px;">
      <h2 style="margin:0 0 12px;">OneDrive Sick Leave Archive updated</h2>
      <p style="margin:0 0 12px;color:#475569;">
        {teacher_name} {action_copy} a sick leave attachment for <strong>{leave_dates}</strong>.
        The file has been stored inside the OneDrive Sick Leave Archive ({archive_window}).
      </p>
      <ul style="margin:0 0 16px;padding-left:20px;color:#475569;">
        <li>Request ID: {excuse.id}</li>
        <li>Folder window: {archive_window}</li>
      </ul>
      {link_block}
      <p style="margin:0;color:#94a3b8;font-size:12px;">You are receiving this alert from the OneDrive Sick Leave Archive setting.</p>
    </div>
    """
    _send_drive_contact_alert(subject, html_body)


def _notify_excuse_attachment_event(excuse: TeacherExcuse, action_label: str, export_info: dict | None = None):
    _ensure_notification_setting_table()
    try:
        setting = (
            db.session.query(ExcuseNotificationSetting)
            .order_by(ExcuseNotificationSetting.id.asc())
            .first()
        )
    except Exception as exc:
        current_app.logger.warning('Unable to load excuse notification settings: %s', exc)
        return
    if not setting or not setting.enabled:
        return
    recipients = [addr for addr in _parse_recipients(setting.recipient_emails or '') if addr]
    if not recipients:
        return
    teacher_name = excuse.teacher.name if excuse.teacher else 'Unknown Teacher'
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    manage_url = url_for('leave_bp.manage_requests', _external=True)
    raw_action = (action_label or 'uploaded').replace('_', ' ')
    subject = f"Review sick leave attachment - {teacher_name}"
    action_heading = raw_action.title()
    html_body = f"""
    <div style="font-family:'Inter','Segoe UI',sans-serif;color:#0f172a;padding:16px;">
      <h2 style="margin:0 0 12px;">Attachment {action_heading}</h2>
      <p style="margin:0 0 12px;color:#475569;">
        {teacher_name} {raw_action} a sick leave attachment for <strong>{leave_dates}</strong>.
      </p>
      <a href="{manage_url}" style="display:inline-block;margin:0 0 12px;padding:10px 18px;border-radius:999px;background:#2563eb;color:#fff;text-decoration:none;font-weight:600;">Review request</a>
      <p style="margin:0;color:#94a3b8;font-size:12px;">Request ID #{excuse.id}</p>
    </div>
    """
    try:
        send_mail(recipients, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to send attachment alert to admin list: %s', exc)


def _send_excuse_message_notification(excuse: TeacherExcuse, message: TeacherExcuseMessage):
    if not excuse or not message:
        return
    teacher = excuse.teacher
    teacher_name = teacher.name if teacher else 'Teacher'
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    formatted_body = _format_multiline(message.body)
    anchor = f"#request-{excuse.id}"
    if message.sender_type == 'teacher':
        _ensure_notification_setting_table()
        try:
            setting = (
                db.session.query(ExcuseNotificationSetting)
                .order_by(ExcuseNotificationSetting.id.asc())
                .first()
            )
        except Exception as exc:
            current_app.logger.warning('Unable to load excuse notification settings: %s', exc)
            return
        if not setting or not setting.enabled:
            return
        recipients = [addr for addr in _parse_recipients(setting.recipient_emails or '') if addr]
        if not recipients:
            return
        subject = f"Teacher replied - {teacher_name} ({leave_dates})"
        manage_url = url_for('leave_bp.manage_requests', _external=True) + anchor
        html_body = f"""
        <div style="font-family:'Inter','Segoe UI',sans-serif;color:#0f172a;padding:16px;">
          <p style="margin:0 0 12px;">{teacher_name} added a message to their excuse on <strong>{leave_dates}</strong>.</p>
          <div style="border-left:4px solid #c7d2fe;background:#eef2ff;padding:12px 16px;margin-bottom:12px;">
            {formatted_body}
          </div>
          <a href="{manage_url}" style="display:inline-block;padding:10px 20px;border-radius:999px;background:#2563eb;color:#fff;font-weight:600;text-decoration:none;">Open ticket</a>
        </div>
        """
        try:
            send_mail(recipients, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
        except Exception as exc:
            current_app.logger.warning('Unable to notify admins about excuse message: %s', exc)
    else:
        if not teacher or not teacher.email:
            return
        subject = f"Admin replied to your excuse ({leave_dates})"
        list_url = url_for('leave_bp.list_requests', _external=True) + anchor
        html_body = f"""
        <div style="font-family:'Inter','Segoe UI',sans-serif;color:#0f172a;padding:16px;">
          <p style="margin:0 0 12px;">Leadership replied to your sick leave / excuse for <strong>{leave_dates}</strong>.</p>
          <div style="border-left:4px solid #bae6fd;background:#e0f2fe;padding:12px 16px;margin-bottom:12px;">
            {formatted_body}
          </div>
          <a href="{list_url}" style="display:inline-block;padding:10px 20px;border-radius:999px;background:#0ea5e9;color:#fff;font-weight:600;text-decoration:none;">Open conversation</a>
        </div>
        """
        try:
            send_mail(teacher.email, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
        except Exception as exc:
            current_app.logger.warning('Unable to notify teacher about excuse message: %s', exc)


def _send_teacher_submission_receipt(excuse: TeacherExcuse):
    teacher = excuse.teacher
    if not teacher or not teacher.email:
        return

    teacher_name = teacher.name or 'Teacher'
    leave_type_label = getattr(excuse, 'type_label', _leave_type_label(excuse.leave_type))
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    reason_html = _format_multiline(excuse.reason or '--')
    time_window = excuse.time_range_label if hasattr(excuse, 'time_range_label') else None
    time_block = ''
    if _requires_time_window(excuse.leave_type) and time_window:
        time_block = f"""
      <p style="margin:0 0 12px;color:#475569;">
        Session time: <strong>{time_window}</strong>
      </p>
        """
    attachment_block = ''
    if (excuse.leave_type or '').lower() == LEAVE_TYPE_SICK:
        upload_url = url_for('leave_bp.list_requests', _external=True)
        deadline = _attachment_deadline(excuse)
        deadline_label = deadline.strftime('%d %b %Y %H:%M UTC') if deadline else '5 days from submission'
        attachment_block = f"""
      <div style="border-left:4px solid #facc15;padding:12px 16px;background:#fffbeb;margin:0 0 16px;">
        <p style="margin:0 0 6px;font-size:13px;font-weight:600;color:#92400e;">Reminder: sick leave document</p>
        <p style="margin:0;color:#92400e;">
          Upload your medical note within 5 days (by <strong>{deadline_label}</strong>) so we can approve this request.
          Use the button below anytime if you still need to attach it.
        </p>
        <p style="margin:12px 0 0;">
          <a href="{upload_url}" style="display:inline-block;padding:8px 16px;border-radius:999px;background:#92400e;color:#fff;font-weight:600;text-decoration:none;">
            Manage sick leave attachments
          </a>
        </p>
      </div>
        """

    html_body = f"""
    <div style="font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px;color:#0f172a;">
      <h2 style="margin:0 0 12px;">We received your {leave_type_label.lower()} request</h2>
      <p style="margin:0 0 12px;color:#475569;">Hi {escape(teacher_name)}, thanks for letting us know you'll be away.</p>
      <p style="margin:0 0 12px;color:#475569;">
        We recorded your request for <strong>{leave_dates}</strong> with the reason below. Our admin team will review it shortly.
      </p>
      {time_block}
      {attachment_block}
      <div style="border-left:4px solid #e2e8f0;padding:12px 16px;background:#f8fafc;margin:0 0 16px;">
        <p style="margin:0 0 4px;font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Reason provided</p>
        <p style="margin:0;color:#0f172a;">{reason_html}</p>
      </div>
      <p style="margin:0;color:#475569;">We'll email you again once a decision is made. No further action is needed right now.</p>
    </div>
    """

    subject = f"Absence request received - {leave_type_label}"
    try:
        send_mail(teacher.email, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to send teacher submission receipt: %s', exc)


def _send_teacher_status_update(excuse: TeacherExcuse):
    teacher = excuse.teacher
    if not teacher or not teacher.email:
        return

    status_label = excuse.status.title()
    leave_type_label = getattr(excuse, 'type_label', _leave_type_label(excuse.leave_type))
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    reason_html = _format_multiline(excuse.reason or '--')
    admin_comment = excuse.admin_comment or ''
    admin_html = _format_multiline(admin_comment) if admin_comment else ''
    time_window = excuse.time_range_label if hasattr(excuse, 'time_range_label') else None

    theme_color = '#059669' if excuse.status == 'approved' else '#dc2626'
    theme_bg = '#ecfdf5' if excuse.status == 'approved' else '#fef2f2'
    subject = f"{leave_type_label} request {status_label.lower()} ({leave_dates})"
    admin_block = ''
    if admin_comment:
        admin_block = f"""
        <div style="border-left:4px solid #6366f1;padding:12px 16px;background:#eef2ff;margin:0 0 16px;">
          <p style="margin:0 0 4px;font-size:12px;color:#4c1d95;text-transform:uppercase;letter-spacing:0.08em;">Admin notes</p>
          <p style="margin:0;color:#312e81;">{admin_html}</p>
        </div>
        """

    html_body = f"""
    <div style="font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px;color:#0f172a;">
      <div style="background:{theme_bg};border-radius:12px;padding:16px;margin-bottom:16px;">
        <p style="margin:0;font-size:13px;color:#475569;">Request status</p>
        <p style="margin:4px 0 0;font-weight:600;color:{theme_color};font-size:20px;">{status_label}</p>
      </div>
      <p style="margin:0 0 12px;color:#475569;">
        Your {leave_type_label.lower()} request for <strong>{leave_dates}</strong> has been reviewed.
      </p>
      {f'<p style="margin:0 0 12px;color:#475569;">Session time: <strong>{time_window}</strong></p>' if (_requires_time_window(excuse.leave_type) and time_window) else ''}
      <div style="border-left:4px solid #e2e8f0;padding:12px 16px;background:#f8fafc;margin:0 0 16px;">
        <p style="margin:0 0 4px;font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Reason on file</p>
        <p style="margin:0;color:#0f172a;">{reason_html}</p>
      </div>
      {admin_block}
      <p style="margin:0;color:#475569;">Contact your line manager if you need to provide further details.</p>
    </div>
    """

    try:
        send_mail(teacher.email, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to send teacher status update: %s', exc)


def _build_leave_approval_webhook_payload(excuse: TeacherExcuse) -> dict:
    teacher = excuse.teacher
    normalized_leave_type = _normalize_leave_type(excuse.leave_type)
    leave_start = _iso_or_none(excuse.leave_date)
    leave_end = _iso_or_none(excuse.end_date or excuse.leave_date)
    submitted_at = _iso_or_none(excuse.created_at)
    email = teacher.email if teacher else None

    teacher_payload = None
    if teacher:
        teacher_payload = {
            "id": teacher.id,
            "name": teacher.name,
            "email": teacher.email,
        }

    return {
        "request_id": f"req-{excuse.id}",
        "excuse_id": excuse.id,
        "email": email,
        "teacher_name": teacher.name if teacher else None,
        "teacher": teacher_payload,
        "leave_type": normalized_leave_type,
        "leave_start": leave_start,
        "leave_end": leave_end,
        "submitted_at": submitted_at,
        "status": excuse.status,
        "reason": excuse.reason,
        "admin_comment": excuse.admin_comment,
        "generated_at": datetime.utcnow().isoformat() + 'Z',
    }


def _send_leave_approval_webhook(excuse: TeacherExcuse) -> None:
    webhook_url = current_app.config.get('LEAVE_APPROVAL_WEBHOOK_URL') or DEFAULT_LEAVE_APPROVAL_WEBHOOK_URL
    if not webhook_url:
        return

    headers = {'Content-Type': 'application/json'}
    secret = current_app.config.get('LEAVE_APPROVAL_WEBHOOK_SECRET')
    if secret:
        headers['X-Leave-Webhook-Secret'] = secret

    timeout = current_app.config.get('LEAVE_APPROVAL_WEBHOOK_TIMEOUT', 5)
    try:
        timeout_value = float(timeout)
    except (TypeError, ValueError):
        timeout_value = 5.0

    payload = _build_leave_approval_webhook_payload(excuse)
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=timeout_value,
        )
        response.raise_for_status()
        current_app.logger.info('Leave approval webhook sent for request %s', excuse.id)
    except requests.RequestException as exc:
        current_app.logger.warning('Leave approval webhook failed for request %s: %s', excuse.id, exc)


def _archive_sick_leave_to_onedrive(excuse: TeacherExcuse):
    if not excuse or not excuse.attachment_path:
        return None
    try:
        info = export_excuse_to_onedrive(excuse, email_profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to archive sick leave to OneDrive: %s', exc)
        return None
    if info and info.get('remote_path'):
        excuse.attachment_export_path = info['remote_path']
        excuse.attachment_exported_at = datetime.utcnow()
        try:
            db.session.commit()
        except Exception as exc:
            current_app.logger.warning('Unable to update export metadata: %s', exc)
    return info


def _send_attachment_reminder_email(excuse: TeacherExcuse, deadline: datetime | None):
    teacher = excuse.teacher
    if not teacher or not teacher.email:
        return False
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    deadline_label = deadline.strftime('%d %b %Y %H:%M UTC') if deadline else 'the deadline'
    upload_url = url_for('leave_bp.list_requests', _external=True)
    html_body = f"""
    <div style="font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px;color:#0f172a;">
      <h2 style="margin:0 0 12px;color:#0f172a;">Reminder: Upload your sick leave document</h2>
      <p style="margin:0 0 12px;color:#475569;">
        We still need the sick leave document for your request on <strong>{leave_dates}</strong>.
        Please upload the attachment before <strong>{deadline_label}</strong> so we can close the ticket.
      </p>
      <p style="margin:0 0 16px;color:#475569;">Use the button below to upload the document instantly.</p>
      <a href="{upload_url}" style="display:inline-block;padding:10px 20px;border-radius:999px;background:#2563eb;color:#fff;font-weight:600;text-decoration:none;">
        Upload document
      </a>
    </div>
    """
    subject = 'Reminder: Submit your sick leave document'
    try:
        send_mail(teacher.email, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
        return True
    except Exception as exc:
        current_app.logger.warning('Unable to send sick leave reminder: %s', exc)
        return False


def _send_attachment_invalidated_email(excuse: TeacherExcuse):
    teacher = excuse.teacher
    if not teacher or not teacher.email:
        return
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    html_body = f"""
    <div style="font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px;color:#0f172a;">
      <h2 style="margin:0 0 12px;color:#b91c1c;">Sick leave request closed</h2>
      <p style="margin:0 0 12px;color:#475569;">
        The ticket for your sick leave request on <strong>{leave_dates}</strong> has been marked invalid because a medical document was not submitted within the 5-day window.
      </p>
      <p style="margin:0;color:#475569;">Contact your line manager if you have any questions.</p>
    </div>
    """
    subject = 'Sick leave request closed - document missing'
    try:
        send_mail(teacher.email, subject, html_body, profile=ABSENCE_EMAIL_PROFILE)
    except Exception as exc:
        current_app.logger.warning('Unable to send sick leave invalidation email: %s', exc)


def _auto_invalidate_missing_attachment(excuse: TeacherExcuse, note: str):
    if not excuse:
        return False
    updated = False
    normalized_note = note.strip()
    if excuse.status != 'invalid':
        excuse.status = 'invalid'
        updated = True
    excuse.attachment_status = TeacherExcuse.ATTACHMENT_STATUS_DECLINED
    if normalized_note:
        if excuse.admin_comment:
            if normalized_note not in excuse.admin_comment:
                excuse.admin_comment = f"{excuse.admin_comment}\n{normalized_note}"
        else:
            excuse.admin_comment = normalized_note
    excuse.reviewed_by = 'System'
    excuse.reviewed_at = datetime.utcnow()
    return updated


def process_sick_leave_attachment_reminders(current_time: datetime | None = None):
    """Send reminder emails and invalidate overdue tickets. Returns a summary dict."""
    _ensure_table()
    now = current_time or datetime.utcnow()
    reminders_sent = 0
    invalidated = 0
    open_tickets = (
        db.session.query(TeacherExcuse)
        .options(joinedload(TeacherExcuse.teacher))
        .filter(TeacherExcuse.leave_type == LEAVE_TYPE_SICK)
        .filter(TeacherExcuse.attachment_required.is_(True))
        .filter(TeacherExcuse.status == 'pending')
        .filter(TeacherExcuse.attachment_status == TeacherExcuse.ATTACHMENT_STATUS_MISSING)
        .all()
    )
    for excuse in open_tickets:
        deadline = _attachment_deadline(excuse)
        if deadline and now >= deadline:
            changed = _auto_invalidate_missing_attachment(
                excuse,
                'Automatically marked invalid after 5 days without a sick leave document.',
            )
            if changed:
                invalidated += 1
                _send_attachment_invalidated_email(excuse)
            continue
        if excuse.attachment_reminder_count >= ATTACHMENT_REMINDER_LIMIT:
            continue
        last_ping = excuse.attachment_last_reminder_at or excuse.created_at
        if not last_ping:
            last_ping = now - timedelta(hours=ATTACHMENT_REMINDER_INTERVAL_HOURS + 1)
        delta = now - last_ping
        if delta.total_seconds() >= ATTACHMENT_REMINDER_INTERVAL_HOURS * 3600:
            if _send_attachment_reminder_email(excuse, deadline):
                excuse.attachment_reminder_count += 1
                excuse.attachment_last_reminder_at = now
                reminders_sent += 1
    db.session.commit()
    return {'reminders_sent': reminders_sent, 'invalidated': invalidated}

def _has_pa_excuse_access() -> bool:
    """Return True when the logged in user is the PA viewer role."""
    return (session.get('role') or '').lower() == 'pa'


def _is_excuse_super_admin() -> bool:
    """Only the platform super admin (admin console account) can manage excuses."""
    if session.get('admin_name'):
        return True
    if session.get('is_super_admin'):
        return True
    if session.get('is_admin') and not session.get('teacher_id'):
        return True
    return False


def _require_admin(*, allow_pa: bool = False):
    """Restrict absence management views to the super admin (and optional PA read-only access).

    The PA role is optionally allowed when a view explicitly opts into read-only access.
    """
    if _is_excuse_super_admin():
        return None
    if allow_pa and _has_pa_excuse_access():
        return None
    flash('Super admin access required to manage excuses.', 'danger')
    return redirect(url_for('admin_bp.login'))


@leave_bp.route('/requests')
@login_required
def list_requests():
    _ensure_table()
    teacher_id = session.get('teacher_id')
    if not teacher_id:
        flash('Please sign in as a teacher to submit excuses.', 'warning')
        return redirect(url_for('auth_bp.login'))

    requests = (
        db.session.query(TeacherExcuse)
        .filter_by(teacher_id=teacher_id)
        .order_by(TeacherExcuse.leave_date.desc(), TeacherExcuse.created_at.desc())
        .all()
    )
    conversation_map = _load_excuse_messages(requests)
    return render_template(
        'absence/teacher_requests.html',
        requests=requests,
        valid_leave_types=VALID_LEAVE_TYPES,
        type_labels=LEAVE_TYPE_LABELS,
        timed_leave_types=sorted(TIMED_LEAVE_TYPES),
        attachment_status_labels=ATTACHMENT_STATUS_LABELS,
        attachment_reminder_limit=ATTACHMENT_REMINDER_LIMIT,
        now_utc=datetime.utcnow(),
        conversation_map=conversation_map,
    )


@leave_bp.route('/requests/new', methods=['GET', 'POST'])
@login_required
def new_request():
    _ensure_table()
    teacher_id = session.get('teacher_id')
    if not teacher_id:
        flash('Only teachers can submit excuses.', 'warning')
        return redirect(url_for('leave_bp.list_requests'))

    if request.method == 'POST':
        leave_date_raw = request.form.get('leave_date', '').strip()
        leave_end_date_raw = request.form.get('leave_end_date', '').strip()
        leave_type = (request.form.get('leave_type') or LEAVE_TYPE_SICK).strip().lower()
        reason = (request.form.get('reason') or '').strip()
        session_start_raw = (request.form.get('session_start') or '').strip()
        session_end_raw = (request.form.get('session_end') or '').strip()
        early_leave_time_raw = (request.form.get('early_leave_time') or '').strip()
        sick_file = request.files.get('sick_attachment')

        errors = []
        try:
            leave_date = datetime.strptime(leave_date_raw, '%Y-%m-%d').date()
        except ValueError:
            leave_date = None
            errors.append('Please provide a valid start date.')

        end_date = leave_date
        if leave_type == LEAVE_TYPE_SICK and leave_end_date_raw:
            try:
                parsed_end = datetime.strptime(leave_end_date_raw, '%Y-%m-%d').date()
                end_date = parsed_end
            except ValueError:
                errors.append('Please provide a valid end date for your sick leave.')

        start_time_value = None
        end_time_value = None
        requires_time = _requires_time_window(leave_type)
        if requires_time:
            start_time_value = _parse_time_value(session_start_raw)
            end_time_value = _parse_time_value(session_end_raw)
            if not start_time_value or not end_time_value:
                errors.append('Please enter both start and end times for conference or training requests.')
            elif end_time_value <= start_time_value:
                errors.append('End time must be after the start time for conference or training requests.')

        if leave_type not in VALID_LEAVE_TYPES:
            errors.append('Select a valid leave type.')
        if not reason:
            errors.append('Reason is required.')

        if leave_type == LEAVE_TYPE_SICK and leave_date and end_date and end_date < leave_date:
            errors.append('Sick leave end date cannot be before the start date.')

        now_uae = datetime.now(UAE_TZ)
        today_uae = now_uae.date()
        if leave_date and leave_date < today_uae:
            errors.append('Excuses can only be submitted for today or upcoming days.')
        if leave_type == LEAVE_TYPE_SICK and leave_date == today_uae:
            if SICK_WINDOW_START <= now_uae.time() < SICK_WINDOW_END:
                _record_sick_window_attempt(teacher_id, leave_date, reason)
                errors.append('You cannot submit sick leave requests after 5:30 AM UAE time. Please try again after 8:00 AM.')

        if leave_type == LEAVE_TYPE_EARLY and leave_date:
            if leave_date <= today_uae:
                errors.append('Early leave requests must be scheduled for upcoming days.')
            end_date = leave_date
            early_time_value = _parse_time_value(early_leave_time_raw)
            if not early_time_value:
                errors.append('Please provide the time you plan to leave.')
            else:
                start_time_value = early_time_value
                end_time_value = None

        if errors:
            for err in errors:
                flash(err, 'danger')
            return redirect(url_for('leave_bp.new_request'))

        attachment_required = leave_type == LEAVE_TYPE_SICK
        attachment_status = TeacherExcuse.ATTACHMENT_STATUS_NOT_REQUIRED
        attachment_path = None
        attachment_original_name = None
        attachment_uploaded_at = None
        attachment_due_at = None
        if attachment_required:
            attachment_status = TeacherExcuse.ATTACHMENT_STATUS_MISSING
            attachment_due_at = datetime.utcnow() + timedelta(days=ATTACHMENT_DUE_DAYS)
            if sick_file and sick_file.filename:
                try:
                    saved_path, original_name = _store_sick_note(sick_file)
                    attachment_path = saved_path
                    attachment_original_name = original_name
                    attachment_uploaded_at = datetime.utcnow()
                    attachment_status = TeacherExcuse.ATTACHMENT_STATUS_SUBMITTED
                except ValueError as exc:
                    flash(str(exc), 'danger')
                    return redirect(url_for('leave_bp.new_request'))

        existing = (
            db.session.query(TeacherExcuse)
            .filter(
                TeacherExcuse.teacher_id == teacher_id,
                TeacherExcuse.leave_date == leave_date,
                TeacherExcuse.status == 'pending',
            )
            .first()
        )
        if existing:
            flash('You already submitted an excuse for this date. Please wait for review.', 'warning')
            return redirect(url_for('leave_bp.list_requests'))

        excuse = TeacherExcuse(
            teacher_id=teacher_id,
            leave_date=leave_date,
            end_date=end_date,
            leave_type=leave_type,
            reason=reason,
            start_time=start_time_value,
            end_time=end_time_value,
            attachment_required=attachment_required,
            attachment_status=attachment_status,
            attachment_path=attachment_path,
            attachment_original_name=attachment_original_name,
            attachment_uploaded_at=attachment_uploaded_at,
            attachment_due_at=attachment_due_at,
            attachment_export_path=None,
            attachment_exported_at=None,
        )
        db.session.add(excuse)
        db.session.commit()
        db.session.refresh(excuse)
        _send_excuse_notification(excuse)
        _send_teacher_submission_receipt(excuse)
        if excuse.attachment_path:
            drive_info = _archive_sick_leave_to_onedrive(excuse)
            _notify_drive_attachment_event(excuse, drive_info, 'uploaded')
            _notify_excuse_attachment_event(excuse, 'uploaded', drive_info)
        _send_leave_approval_webhook(excuse)
        flash('Your excuse has been submitted for review.', 'success')
        return redirect(url_for('leave_bp.list_requests'))

    return render_template(
        'absence/request_form.html',
        valid_leave_types=VALID_LEAVE_TYPES,
        type_labels=LEAVE_TYPE_LABELS,
        timed_leave_types=sorted(TIMED_LEAVE_TYPES),
    )


@leave_bp.route('/requests/<int:request_id>/attachment', methods=['POST'])
@login_required
def upload_sick_leave_attachment(request_id: int):
    _ensure_table()
    teacher_id = session.get('teacher_id')
    if not teacher_id:
        flash('Only teachers can manage sick leave documents.', 'warning')
        return redirect(url_for('leave_bp.list_requests'))

    excuse = (
        db.session.query(TeacherExcuse)
        .filter(TeacherExcuse.id == request_id, TeacherExcuse.teacher_id == teacher_id)
        .first()
    )
    if not excuse or (excuse.leave_type or '').lower() != LEAVE_TYPE_SICK:
        flash('Sick leave attachment not found.', 'warning')
        return redirect(url_for('leave_bp.list_requests'))
    file_obj = request.files.get('attachment')
    if not file_obj or not file_obj.filename:
        flash('Select a file to upload.', 'danger')
        return redirect(url_for('leave_bp.list_requests'))
    try:
        saved_path, original_name = _store_sick_note(file_obj)
    except ValueError as exc:
        flash(str(exc), 'danger')
        return redirect(url_for('leave_bp.list_requests'))

    had_previous = bool(excuse.attachment_path or excuse.attachment_export_path)
    if excuse.attachment_export_path:
        remove_exported_attachment(excuse, ms_profile_override=ABSENCE_EMAIL_PROFILE)
    _delete_existing_attachment(excuse)
    excuse.attachment_required = True
    excuse.attachment_path = saved_path
    excuse.attachment_original_name = original_name
    excuse.attachment_uploaded_at = datetime.utcnow()
    excuse.attachment_status = TeacherExcuse.ATTACHMENT_STATUS_SUBMITTED
    if not excuse.attachment_due_at:
        excuse.attachment_due_at = datetime.utcnow() + timedelta(days=ATTACHMENT_DUE_DAYS)
    db.session.commit()
    drive_info = _archive_sick_leave_to_onedrive(excuse)
    action = 'replaced' if had_previous else 'uploaded'
    _notify_drive_attachment_event(excuse, drive_info, action)
    _notify_excuse_attachment_event(excuse, action, drive_info)
    flash('Medical document uploaded successfully.', 'success')
    return redirect(url_for('leave_bp.list_requests'))


@leave_bp.route('/requests/<int:request_id>/no-document', methods=['POST'])
@login_required
def acknowledge_no_sick_leave_document(request_id: int):
    _ensure_table()
    teacher_id = session.get('teacher_id')
    if not teacher_id:
        flash('Sign in to manage your excuses.', 'warning')
        return redirect(url_for('auth_bp.login'))
    excuse = (
        db.session.query(TeacherExcuse)
        .filter(TeacherExcuse.id == request_id, TeacherExcuse.teacher_id == teacher_id)
        .first()
    )
    if not excuse or (excuse.leave_type or '').lower() != LEAVE_TYPE_SICK:
        flash('Sick leave request not found.', 'warning')
        return redirect(url_for('leave_bp.list_requests'))
    if excuse.status != 'pending':
        flash('This request has already been processed.', 'warning')
        return redirect(url_for('leave_bp.list_requests'))

    if excuse.attachment_export_path:
        remove_exported_attachment(excuse, ms_profile_override=ABSENCE_EMAIL_PROFILE)
    _delete_existing_attachment(excuse)
    excuse.attachment_path = None
    excuse.attachment_original_name = None
    excuse.attachment_uploaded_at = None
    excuse.attachment_status = TeacherExcuse.ATTACHMENT_STATUS_DECLINED
    excuse.attachment_required = True
    excuse.status = 'invalid'
    excuse.admin_comment = 'Teacher confirmed that no sick leave document will be submitted.'
    excuse.reviewed_by = session.get('teacher_name') or 'Teacher'
    excuse.reviewed_at = datetime.utcnow()
    db.session.commit()
    flash('Request has been marked invalid as no sick leave document will be provided.', 'info')
    return redirect(url_for('leave_bp.list_requests'))


@leave_bp.route('/requests/<int:request_id>/attachment/download')
@login_required
def download_sick_leave_attachment(request_id: int):
    _ensure_table()
    excuse = db.session.query(TeacherExcuse).get(request_id)
    redirect_endpoint = 'leave_bp.manage_requests' if (_is_excuse_super_admin() or _has_pa_excuse_access()) else 'leave_bp.list_requests'
    if excuse is None or not excuse.attachment_path:
        flash('Attachment not available.', 'warning')
        return redirect(url_for(redirect_endpoint))
    teacher_id = session.get('teacher_id')
    allowed = _is_excuse_super_admin() or _has_pa_excuse_access() or (teacher_id and teacher_id == excuse.teacher_id)
    if not allowed:
        flash('You do not have access to this attachment.', 'danger')
        return redirect(url_for(redirect_endpoint))
    file_path = _resolve_attachment_file(excuse)
    if not file_path or not file_path.exists():
        flash('Attachment could not be located on the server.', 'danger')
        return redirect(url_for(redirect_endpoint))
    download_name = excuse.attachment_original_name or file_path.name
    return send_file(file_path, as_attachment=False, download_name=download_name)


@leave_bp.route('/requests/<int:request_id>/messages', methods=['POST'])
@login_required
def post_excuse_message(request_id: int):
    _ensure_table()
    _ensure_message_table()
    excuse = db.session.get(TeacherExcuse, request_id)
    if not excuse:
        flash('Request not found.', 'warning')
        return redirect(url_for('leave_bp.list_requests'))

    teacher_id = session.get('teacher_id')
    is_admin = _is_excuse_super_admin()
    if not is_admin and (not teacher_id or teacher_id != excuse.teacher_id):
        flash('You do not have access to this request.', 'danger')
        return redirect(url_for('leave_bp.list_requests'))

    if (excuse.status or '').lower() in {'approved', 'rejected', 'invalid'}:
        flash('This request is closed. New messages are disabled.', 'warning')
        target = url_for('leave_bp.manage_requests') if is_admin else url_for('leave_bp.list_requests')
        return redirect(f"{target}#request-{excuse.id}")

    body = (request.form.get('message') or '').strip()
    if not body:
        flash('Enter a message before sending.', 'warning')
        target = url_for('leave_bp.manage_requests') if is_admin else url_for('leave_bp.list_requests')
        return redirect(f"{target}#request-{excuse.id}")

    message = TeacherExcuseMessage(
        excuse_id=excuse.id,
        sender_type='admin' if is_admin else 'teacher',
        sender_teacher_id=teacher_id if not is_admin else None,
        sender_teacher_name=(session.get('teacher_name') or (excuse.teacher.name if excuse.teacher else None)) if not is_admin else None,
        sender_admin_name=(session.get('admin_name') or session.get('teacher_name') or 'Admin') if is_admin else None,
        sender_email=(excuse.teacher.email if not is_admin else None),
        body=body,
    )
    db.session.add(message)
    try:
        db.session.commit()
    except Exception as exc:
        current_app.logger.warning('Unable to save excuse message: %s', exc)
        db.session.rollback()
        flash('Unable to send your message. Please try again.', 'danger')
        target = url_for('leave_bp.manage_requests') if is_admin else url_for('leave_bp.list_requests')
        return redirect(f"{target}#request-{excuse.id}")

    try:
        _send_excuse_message_notification(excuse, message)
    except Exception as exc:
        current_app.logger.warning('Excuse message notification failed: %s', exc)

    flash('Message sent.', 'success')
    target = url_for('leave_bp.manage_requests') if is_admin else url_for('leave_bp.list_requests')
    return redirect(f"{target}#request-{excuse.id}")


@leave_bp.route('/manage')
@login_required
def manage_requests():
    _ensure_table()
    guard = _require_admin(allow_pa=True)
    if guard is not None:
        return guard

    filter_type = (request.args.get('leave_type') or '').strip().lower()
    filter_active = filter_type if filter_type in VALID_LEAVE_TYPES else ''
    filter_options = [('all', 'All Requests')] + [
        (key, label) for key, label in LEAVE_TYPE_LABELS.items()
    ]

    status_priority = case(
        (TeacherExcuse.status == 'pending', 0),
        (TeacherExcuse.status == 'approved', 1),
        (TeacherExcuse.status == 'rejected', 2),
        (TeacherExcuse.status == 'invalid', 3),
        else_=4,
    )
    q = db.session.query(TeacherExcuse)
    if filter_active:
        q = q.filter(TeacherExcuse.leave_type == filter_active)
    q = q.order_by(
        status_priority,
        TeacherExcuse.created_at.desc(),
        TeacherExcuse.leave_date.desc(),
    )
    requests = q.all()
    conversation_map = _load_excuse_messages(requests)

    total_requests = len(requests)
    absence_types = {LEAVE_TYPE_SICK} | set(LEGACY_ABSENCE_TYPES)
    groups_config = [
        ('sickleave', 'Sick Leave & Absence', 'Full-day or multi-day absences, including legacy general excuses.', absence_types),
        ('offsite', 'Off-Site (Conference / Training)', 'Teachers attending events outside school with defined times.', TIMED_LEAVE_TYPES),
        ('early', 'Early Leave Requests', 'Teachers requesting to leave the campus early.', {LEAVE_TYPE_EARLY}),
    ]
    grouped_requests = []
    captured_ids = set()
    for key, title, description, type_set in groups_config:
        normalized_types = {value.lower() for value in type_set}
        if filter_active and filter_active not in normalized_types:
            continue
        items = [req for req in requests if (req.leave_type or '').lower() in normalized_types]
        captured_ids.update(req.id for req in items)
        grouped_requests.append({
            'key': key,
            'title': title,
            'description': description,
            'rows': items,
        })
    remaining = [req for req in requests if req.id not in captured_ids]
    if remaining:
        grouped_requests.append({
            'key': 'legacy',
            'title': 'Legacy Requests',
            'description': 'Older request types that do not fit the new categories.',
            'rows': remaining,
        })
    return render_template(
        'absence/manage_requests.html',
        total_requests=total_requests,
        grouped_requests=grouped_requests,
        requests=requests,
        statuses=VALID_STATUSES,
        type_labels=LEAVE_TYPE_LABELS,
        timed_leave_types=sorted(TIMED_LEAVE_TYPES),
        active_filter=filter_active,
        filter_options=filter_options,
        attachment_status_labels=ATTACHMENT_STATUS_LABELS,
        attachment_reminder_limit=ATTACHMENT_REMINDER_LIMIT,
        now_utc=datetime.utcnow(),
        conversation_map=conversation_map,
        can_moderate=_is_excuse_super_admin(),
        is_pa_viewer=_has_pa_excuse_access(),
    )


@leave_bp.route('/manage/<int:request_id>/delete', methods=['POST'])
@login_required
def delete_request(request_id: int):
    _ensure_table()
    guard = _require_admin()
    if guard is not None:
        return guard

    excuse = db.session.get(TeacherExcuse, request_id)
    if not excuse:
        flash('Excuse not found.', 'warning')
        return redirect(url_for('leave_bp.manage_requests'))

    if excuse.attachment_export_path:
        remove_exported_attachment(excuse, ms_profile_override=ABSENCE_EMAIL_PROFILE)
    _delete_existing_attachment(excuse)
    db.session.delete(excuse)
    db.session.commit()
    flash('Request deleted.', 'success')
    return redirect(url_for('leave_bp.manage_requests'))


@leave_bp.route('/whereabouts')
@login_required
def teacher_whereabouts():
    _ensure_table()
    guard = _require_admin()
    if guard is not None:
        return guard

    filter_type = (request.args.get('leave_type') or '').strip().lower()
    filter_active = filter_type if filter_type in VALID_LEAVE_TYPES else ''
    filter_options = [('all', 'All Requests')] + [
        (key, label) for key, label in LEAVE_TYPE_LABELS.items()
    ]

    now_uae = datetime.now(UAE_TZ)
    today = now_uae.date()
    candidates = (
        db.session.query(TeacherExcuse)
        .options(joinedload(TeacherExcuse.teacher))
        .filter(TeacherExcuse.status == 'approved')
        .filter(TeacherExcuse.leave_date <= today)
        .filter(or_(TeacherExcuse.end_date.is_(None), TeacherExcuse.end_date >= today))
        .all()
    )
    active_excuses = {}
    for excuse in candidates:
        if not excuse.teacher_id or not excuse.teacher:
            continue
        if not _is_excuse_active(excuse, now_uae):
            continue
        if filter_active and (excuse.leave_type or '').lower() != filter_active:
            continue
        active_excuses[excuse.teacher_id] = excuse

    teachers = db.session.query(Teacher).order_by(Teacher.name).all()
    excused_rows = []
    present_rows = []
    for teacher in teachers:
        active = active_excuses.get(teacher.id)
        if active:
            excused_rows.append({
                'teacher': teacher,
                'excuse': active,
                'type_label': getattr(active, 'type_label', _leave_type_label(active.leave_type)),
                'date_label': getattr(active, 'date_range_label', active.leave_date.strftime('%d %b %Y')),
                'time_label': getattr(active, 'time_range_label', None),
                'reason': active.reason or '--',
            })
        else:
            present_rows.append(teacher)

    return render_template(
        'absence/whereabouts.html',
        now_label=now_uae.strftime('%d %b %Y %H:%M'),
        excused_teachers=excused_rows,
        present_teachers=present_rows,
        active_filter=filter_active,
        filter_options=filter_options,
    )


@leave_bp.route('/manage/<int:request_id>/status', methods=['POST'])
@login_required
def update_request_status(request_id: int):
    _ensure_table()
    guard = _require_admin()
    if guard is not None:
        return guard

    excuse = db.session.query(TeacherExcuse).get(request_id)
    if excuse is None:
        flash('Excuse not found.', 'warning')
        return redirect(url_for('leave_bp.manage_requests'))

    status = (request.form.get('status') or 'pending').strip().lower()
    admin_comment = (request.form.get('admin_comment') or '').strip() or None
    if status not in VALID_STATUSES:
        flash('Invalid status selected.', 'danger')
        return redirect(url_for('leave_bp.manage_requests'))

    requires_attachment = (excuse.leave_type or '').lower() == LEAVE_TYPE_SICK and excuse.attachment_required
    if status == 'approved' and requires_attachment and not excuse.attachment_path and not _is_excuse_super_admin():
        flash('Upload a sick leave document before approving this request.', 'danger')
        return redirect(url_for('leave_bp.manage_requests'))

    previous_status = excuse.status
    excuse.status = status
    excuse.admin_comment = admin_comment
    excuse.reviewed_by = session.get('admin_name') or session.get('teacher_name')
    excuse.reviewed_at = datetime.utcnow()
    if requires_attachment:
        if status == 'approved':
            excuse.attachment_status = TeacherExcuse.ATTACHMENT_STATUS_APPROVED
        elif status in ('rejected', 'invalid'):
            excuse.attachment_status = TeacherExcuse.ATTACHMENT_STATUS_DECLINED
        else:
            excuse.attachment_status = (
                TeacherExcuse.ATTACHMENT_STATUS_SUBMITTED if excuse.attachment_path
                else TeacherExcuse.ATTACHMENT_STATUS_MISSING
            )
    db.session.commit()

    status_changed = previous_status != status
    try:
        db.session.refresh(excuse)
    except Exception:
        pass

    if status_changed and (excuse.leave_type or '').lower() == LEAVE_TYPE_SICK and status == 'approved':
        _send_grade_sick_leave_alert(excuse)
    if status_changed and status != 'pending':
        _send_teacher_status_update(excuse)
    flash('Excuse updated successfully.', 'success')
    return redirect(url_for('leave_bp.manage_requests'))
