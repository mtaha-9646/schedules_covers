from datetime import datetime
import re
from typing import Optional
from extensions import db

EMAIL_SPLIT_RE = re.compile(r'[\s,;]+')
EMAIL_VALID_RE = re.compile(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')

def _clean_email_tokens(raw: Optional[str]) -> list[str]:
    tokens = [token.strip() for token in EMAIL_SPLIT_RE.split(raw or '') if token and token.strip()]
    return [token for token in tokens if EMAIL_VALID_RE.match(token)]

class Teacher(db.Model):
    __bind_key__ = 'teachers_bind'
    __tablename__ = 'teachers'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    subject = db.Column(db.String(120), nullable=True)
    grade = db.Column(db.String(20), nullable=True)

    def __repr__(self):
        return f"<Teacher {self.name}>"

class TeacherExcuse(db.Model):
    ATTACHMENT_STATUS_NOT_REQUIRED = 'not_required'
    ATTACHMENT_STATUS_MISSING = 'missing'
    ATTACHMENT_STATUS_SUBMITTED = 'submitted'
    ATTACHMENT_STATUS_APPROVED = 'approved'
    ATTACHMENT_STATUS_DECLINED = 'declined'

    __tablename__ = 'teacher_excuses'
    __bind_key__ = 'teachers_bind'

    id = db.Column(db.Integer, primary_key=True)
    teacher_id = db.Column(db.Integer, db.ForeignKey('teachers.id'), nullable=False, index=True)
    leave_date = db.Column(db.Date, nullable=False, index=True)
    end_date = db.Column(db.Date, nullable=True)
    start_time = db.Column(db.Time, nullable=True)
    end_time = db.Column(db.Time, nullable=True)
    leave_type = db.Column(db.String(40), nullable=False, default='sickleave')
    reason = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(20), nullable=False, default='pending')
    admin_comment = db.Column(db.Text, nullable=True)
    reviewed_by = db.Column(db.String(150), nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    attachment_required = db.Column(db.Boolean, nullable=False, default=False)
    attachment_status = db.Column(db.String(20), nullable=False, default=ATTACHMENT_STATUS_NOT_REQUIRED)
    attachment_path = db.Column(db.String(500), nullable=True)
    attachment_original_name = db.Column(db.String(255), nullable=True)
    attachment_uploaded_at = db.Column(db.DateTime, nullable=True)
    attachment_due_at = db.Column(db.DateTime, nullable=True)
    attachment_reminder_count = db.Column(db.Integer, nullable=False, default=0)
    attachment_last_reminder_at = db.Column(db.DateTime, nullable=True)
    attachment_export_path = db.Column(db.String(500), nullable=True)
    attachment_exported_at = db.Column(db.DateTime, nullable=True)

    teacher = db.relationship(
        'Teacher',
        foreign_keys=[teacher_id],
        primaryjoin="TeacherExcuse.teacher_id == Teacher.id",
        backref=db.backref('absence_requests', lazy='dynamic')
    )
    messages = db.relationship(
        'TeacherExcuseMessage',
        backref='excuse',
        lazy='dynamic',
        cascade='all, delete-orphan'
    )

    def __repr__(self):
        return f"<TeacherExcuse teacher_id={self.teacher_id} date={self.leave_date} status={self.status}>"

    def to_dict(self):
        return {
            'id': self.id,
            'teacher_id': self.teacher_id,
            'teacher_name': self.teacher.name if self.teacher else 'Unknown',
            'leave_date': self.leave_date.isoformat(),
            'leave_type': self.leave_type,
            'start_time': self.start_time.strftime('%H:%M') if self.start_time else None,
            'end_time': self.end_time.strftime('%H:%M') if self.end_time else None,
            'time_range_label': self.time_range_label,
            'reason': self.reason,
            'status': self.status,
            'admin_comment': self.admin_comment,
            'reviewed_by': self.reviewed_by,
            'reviewed_at': self.reviewed_at.isoformat() if self.reviewed_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'attachment_required': self.attachment_required,
            'attachment_status': self.attachment_status,
            'attachment_original_name': self.attachment_original_name,
            'attachment_present': bool(self.attachment_path),
            'attachment_due_at': self.attachment_due_at.isoformat() if self.attachment_due_at else None,
            'attachment_uploaded_at': self.attachment_uploaded_at.isoformat() if self.attachment_uploaded_at else None,
            'attachment_reminder_count': self.attachment_reminder_count,
            'attachment_export_path': self.attachment_export_path,
            'attachment_exported_at': self.attachment_exported_at.isoformat() if self.attachment_exported_at else None,
        }

    @property
    def type_label(self):
        mapping = {
            'sickleave': 'Sick Leave',
            'conference_offsite': 'Conference Outside School',
            'training_offsite': 'Training Outside School',
            'early_leave_request': 'Early Leave Request',
        }
        key = (self.leave_type or '').lower()
        return mapping.get(key, (self.leave_type or 'Leave').replace('_', ' ').title())

    @property
    def normalized_end_date(self):
        return self.end_date or self.leave_date

    @property
    def spans_multiple_days(self):
        return bool(self.normalized_end_date and self.normalized_end_date != self.leave_date)

    @property
    def date_range_label(self):
        fmt = '%d %b %Y'
        start = self.leave_date.strftime(fmt)
        end = self.normalized_end_date.strftime(fmt) if self.normalized_end_date else None
        if end and end != start:
            return f"{start} - {end}"
        return start

    @property
    def time_range_label(self):
        if not self.start_time and not self.end_time:
            return None
        fmt = '%H:%M'
        start = self.start_time.strftime(fmt) if self.start_time else '--'
        end = self.end_time.strftime(fmt) if self.end_time else '--'
        if end == start or not self.end_time:
            return start
        return f"{start} - {end}"

class TeacherExcuseMessage(db.Model):
    __tablename__ = 'teacher_excuse_messages'
    __bind_key__ = 'teachers_bind'

    id = db.Column(db.Integer, primary_key=True)
    excuse_id = db.Column(db.Integer, db.ForeignKey('teacher_excuses.id'), nullable=False, index=True)
    sender_type = db.Column(db.String(20), nullable=False)  # 'teacher' or 'admin'
    sender_teacher_id = db.Column(db.Integer, nullable=True)
    sender_teacher_name = db.Column(db.String(150), nullable=True)
    sender_admin_name = db.Column(db.String(150), nullable=True)
    sender_email = db.Column(db.String(255), nullable=True)
    body = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"<TeacherExcuseMessage excuse={self.excuse_id} sender={self.sender_type}>"

class SickLeaveWindowAttempt(db.Model):
    __tablename__ = 'sick_window_attempts'
    __bind_key__ = 'teachers_bind'

    id = db.Column(db.Integer, primary_key=True)
    teacher_id = db.Column(db.Integer, db.ForeignKey('teachers.id'), nullable=False, index=True)
    leave_date = db.Column(db.Date, nullable=True)
    attempted_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    reason_preview = db.Column(db.Text, nullable=True)

    teacher = db.relationship(
        'Teacher',
        foreign_keys=[teacher_id],
        primaryjoin="SickLeaveWindowAttempt.teacher_id == Teacher.id",
        backref=db.backref('sick_window_attempts', lazy='dynamic')
    )

class ExcuseNotificationSetting(db.Model):
    __tablename__ = 'excuse_notification_settings'

    id = db.Column(db.Integer, primary_key=True)
    recipient_emails = db.Column(db.Text, nullable=True)
    enabled = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def email_list(self):
        return _clean_email_tokens(self.recipient_emails)

class SickLeaveGradeRecipient(db.Model):
    __tablename__ = 'sick_leave_grade_recipients'

    grade = db.Column(db.String(10), primary_key=True)
    recipient_emails = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def email_list(self):
        return _clean_email_tokens(self.recipient_emails)
