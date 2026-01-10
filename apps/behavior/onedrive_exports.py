from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from flask import current_app
from zoneinfo import ZoneInfo

from behaviour import OneDriveExportSetting, TeacherExcuse
from extensions import db
from ms_auth_cache import get_token_silent
from ms_email import send_mail
from onedrive_client import OneDriveClient, GraphAPIError

EXPORT_TZ = ZoneInfo('Asia/Dubai')
DEFAULT_MS_PROFILE = 'absence'


def _ensure_setting_table():
    try:
        OneDriveExportSetting.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass


def get_onedrive_setting() -> Optional[OneDriveExportSetting]:
    _ensure_setting_table()
    try:
        return (
            db.session.query(OneDriveExportSetting)
            .order_by(OneDriveExportSetting.id.asc())
            .first()
        )
    except Exception as exc:
        current_app.logger.warning('Unable to load OneDrive export setting: %s', exc)
        return None


def compute_current_window(reference: Optional[datetime] = None) -> tuple[date, date]:
    reference = reference or datetime.now(EXPORT_TZ)
    return compute_window_for_date(reference.date())


def compute_window_for_date(target_date: date) -> tuple[date, date]:
    if target_date.day >= 15:
        start = date(target_date.year, target_date.month, 15)
        next_month = target_date.month + 1
        next_year = target_date.year
    else:
        prev_month = target_date.month - 1
        prev_year = target_date.year
        if prev_month <= 0:
            prev_month = 12
            prev_year -= 1
        start = date(prev_year, prev_month, 15)
        next_month = start.month + 1
        next_year = start.year
    if next_month > 12:
        next_month = 1
        next_year += 1
    end = date(next_year, next_month, 16)
    return start, end


def _safe_teacher_name(excuse: TeacherExcuse) -> str:
    teacher_name = excuse.teacher.name if excuse.teacher else 'Teacher'
    sanitized = re.sub(r'[^A-Za-z0-9]+', '_', teacher_name).strip('_')
    return sanitized or 'Teacher'


def format_export_filename(excuse: TeacherExcuse, local_path: Path) -> str:
    base_date = excuse.leave_date.isoformat() if excuse.leave_date else datetime.utcnow().date().isoformat()
    original_name = excuse.attachment_original_name or local_path.name
    extension = Path(original_name).suffix
    return f"{_safe_teacher_name(excuse)}-{base_date}-REQ{excuse.id}{extension}"


def _resolve_attachment_path(excuse: TeacherExcuse) -> Optional[Path]:
    if not excuse.attachment_path:
        return None
    local_path = Path(current_app.root_path) / excuse.attachment_path
    if not local_path.exists():
        current_app.logger.warning('Attachment missing on disk for excuse %s', excuse.id)
        return None
    return local_path


def export_excuse_to_onedrive(
    excuse: TeacherExcuse,
    *,
    email_profile: str = 'behaviour',
    notify: bool = True,
    ms_profile_override: Optional[str] = None,
):
    if not excuse or (excuse.leave_type or '').lower() != 'sickleave':
        return None
    local_path = _resolve_attachment_path(excuse)
    if not local_path:
        return None

    setting = get_onedrive_setting()
    recipient_list = setting.email_list() if setting else []
    profile = ms_profile_override or (setting.ms_profile if setting and setting.ms_profile else DEFAULT_MS_PROFILE)

    try:
        token = get_token_silent(profile=profile)
    except Exception as exc:
        current_app.logger.warning('OneDrive export skipped (auth error): %s', exc)
        return None

    client = OneDriveClient(token)
    leave_date = excuse.leave_date or datetime.utcnow().date()
    start, end = compute_window_for_date(leave_date)
    folder_name = f"{start.isoformat()}_to_{end.isoformat()}"
    filename = format_export_filename(excuse, local_path)
    remote_path = f"{folder_name}/{filename}"

    if excuse.attachment_export_path and excuse.attachment_export_path != remote_path:
        try:
            client.delete_item_by_path(excuse.attachment_export_path)
        except Exception as exc:
            current_app.logger.warning('Unable to remove previous OneDrive file for excuse %s: %s', excuse.id, exc)

    try:
        file_item, folder_item = client.upload_file(
            local_path,
            folder_name,
            filename,
        )
    except GraphAPIError as exc:
        current_app.logger.warning('OneDrive export failed (graph): %s', exc)
        return None
    except Exception as exc:
        current_app.logger.warning('OneDrive export failed: %s', exc)
        return None

    if recipient_list and (folder_item or file_item):
        try:
            target_id = folder_item.id if folder_item else file_item.id
            client.share_item_with_recipients(target_id, recipient_list, roles=["read"], send_invitation=False)
        except Exception as exc:
            current_app.logger.warning('Unable to grant OneDrive access for excuse %s: %s', excuse.id, exc)

    export_info = {
        'folder_name': folder_name,
        'file': file_item,
        'folder': folder_item,
        'remote_path': remote_path,
    }
    if notify:
        _send_export_notification(setting, excuse, export_info, email_profile)
    return export_info


def remove_exported_attachment(excuse: TeacherExcuse, ms_profile_override: Optional[str] = None) -> bool:
    if not excuse or not excuse.attachment_export_path:
        return False
    setting = get_onedrive_setting()
    profile = ms_profile_override or (setting.ms_profile if setting and setting.ms_profile else DEFAULT_MS_PROFILE)
    try:
        token = get_token_silent(profile=profile)
    except Exception as exc:
        current_app.logger.warning('OneDrive removal skipped (auth error): %s', exc)
        return False
    client = OneDriveClient(token)
    try:
        client.delete_item_by_path(excuse.attachment_export_path)
    except Exception as exc:
        current_app.logger.warning('Unable to remove OneDrive file for excuse %s: %s', excuse.id, exc)
        return False
    excuse.attachment_export_path = None
    excuse.attachment_exported_at = None
    return True


def _send_export_notification(setting, excuse, export_info, email_profile: str):
    if not setting or not setting.enabled:
        return
    recipients = setting.email_list()
    if not recipients:
        return
    folder_item = export_info.get('folder')
    folder_url = folder_item.web_url if folder_item else ''

    teacher_name = excuse.teacher.name if excuse.teacher else 'Unknown Teacher'
    leave_dates = getattr(excuse, 'date_range_label', excuse.leave_date.strftime('%d %b %Y'))
    subject = f"Sick leave archived - {teacher_name} ({leave_dates})"
    html_body = f"""
    <div style="font-family:'Inter','Segoe UI',sans-serif;color:#0f172a;padding:16px;">
      <h2 style="margin:0 0 12px;">Sick leave archived</h2>
      <p style="margin:0 0 16px;color:#475569;">
        A new sick leave request for <strong>{leave_dates}</strong> from <strong>{teacher_name}</strong> has been uploaded to OneDrive.
      </p>
      <ul style="margin:0 0 16px;padding-left:20px;color:#475569;">
        <li>Request ID: {excuse.id}</li>
        <li>Folder window: {export_info.get('folder_name')}</li>
        <li>Status: {excuse.status.title()}</li>
      </ul>
      {'<div style=\"display:flex;gap:12px;flex-wrap:wrap;\"><a href=\"' + folder_url + '\" style=\"padding:10px 18px;border-radius:999px;background:#0ea5e9;color:#fff;text-decoration:none;font-weight:600;\">Open folder</a></div>' if folder_url else ''}
      <p style="margin:16px 0 0;color:#94a3b8;font-size:12px;">This message was generated automatically by the Behaviour app.</p>
    </div>
    """
    try:
        send_mail(
            recipients,
            subject,
            html_body,
            profile=email_profile,
        )
    except Exception as exc:
        current_app.logger.warning('Unable to send OneDrive export email: %s', exc)
