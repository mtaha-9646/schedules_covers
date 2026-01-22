from __future__ import annotations

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from db import Base


class ScheduleEntry(Base):
    __tablename__ = "schedule_entries"

    id = Column(Integer, primary_key=True)
    teacher = Column(String, index=True, nullable=False)
    day = Column(String, nullable=False)
    day_code = Column(String, index=True, nullable=False)
    period = Column(String, nullable=False)
    period_raw = Column(String)
    period_group = Column(String, index=True)
    period_rank = Column(Integer)
    details = Column(Text)
    details_display = Column(Text)
    grade_detected = Column(Integer)
    email = Column(String)
    subject = Column(String)
    course_count = Column(Integer)


class TeacherManifest(Base):
    __tablename__ = "teacher_manifest"

    id = Column(Integer, primary_key=True)
    slug = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    email = Column(String)


class AbsenceRecord(Base):
    __tablename__ = "absence_records"

    id = Column(Integer, primary_key=True)
    request_id = Column(String, unique=True, index=True, nullable=False)
    teacher = Column(String, nullable=False)
    teacher_email = Column(String, index=True, nullable=False)
    teacher_slug = Column(String)
    leave_type = Column(String)
    leave_start = Column(Date, index=True, nullable=False)
    leave_end = Column(Date, nullable=False)
    status = Column(String)
    reason = Column(Text)
    submitted_at = Column(DateTime)
    recorded_at = Column(DateTime)
    subject = Column(String)
    level_label = Column(String)
    payload = Column(Text)
    forwarded_at = Column(DateTime)
    forward_status = Column(String)
    forward_response = Column(Text)


class CoverAssignment(Base):
    __tablename__ = "cover_assignments"

    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False)
    request_id = Column(String, index=True)
    slot_key = Column(String, nullable=False)
    absent_teacher = Column(String, nullable=False)
    absent_email = Column(String, nullable=False)
    cover_teacher = Column(String, nullable=False)
    cover_email = Column(String, nullable=False)
    cover_slug = Column(String)
    subject = Column(String)
    class_subject = Column(String)
    class_grade = Column(String)
    class_details = Column(Text)
    period_label = Column(String)
    period_raw = Column(String)
    class_time = Column(String)
    cover_subject = Column(String)
    status = Column(String)
    leave_type = Column(String)
    leave_start = Column(Date)
    leave_end = Column(Date)
    submitted_at = Column(DateTime)
    cover_free_periods = Column(Integer)
    cover_scheduled = Column(Integer)
    cover_max_periods = Column(Integer)
    cover_assigned_at = Column(DateTime)
    day_label = Column(String)

    __table_args__ = (
        UniqueConstraint("date", "request_id", "slot_key", name="uq_assignment_slot"),
    )


class ExcludedTeacher(Base):
    __tablename__ = "excluded_teachers"

    id = Column(Integer, primary_key=True)
    slug = Column(String, unique=True, nullable=False)


class AssignmentSetting(Base):
    __tablename__ = "assignment_settings"

    key = Column(String, primary_key=True)
    value = Column(Integer, nullable=False)


class DutyAssignment(Base):
    __tablename__ = "duty_assignments"

    id = Column(Integer, primary_key=True)
    assignment_date = Column(Date, index=True, nullable=False)
    grade = Column(String, index=True)
    slot_type = Column(String, nullable=False)
    period_label = Column(String)
    pod = Column(String)
    label = Column(String)
    break_location = Column(String)
    teacher_name = Column(String)
    teacher_email = Column(String, index=True)
    created_at = Column(DateTime)


class PodDutyAssignment(Base):
    __tablename__ = "pod_duty_assignments"

    id = Column(Integer, primary_key=True)
    assignment_date = Column(Date, index=True, nullable=False)
    day_code = Column(String, index=True)
    period_label = Column(String, index=True)
    pod_label = Column(String, index=True)
    teacher_name = Column(String)
    teacher_email = Column(String)
    teacher_slug = Column(String, index=True)
    created_at = Column(DateTime)


class PodDutyNotification(Base):
    __tablename__ = "pod_duty_notifications"

    id = Column(Integer, primary_key=True)
    assignment_date = Column(Date, index=True)
    period_label = Column(String, index=True)
    pod_label = Column(String)
    teacher_slug = Column(String)
    teacher_name = Column(String)
    teacher_email = Column(String)
    assignment_hash = Column(String, unique=True)
    notified_at = Column(DateTime)


class PodDutyAssignment(Base):
    __tablename__ = "pod_duty_assignments"

    id = Column(Integer, primary_key=True)
    assignment_date = Column(Date, index=True, nullable=False)
    day_code = Column(String, index=True)
    period_label = Column(String, index=True)
    pod_label = Column(String, index=True)
    teacher_name = Column(String)
    teacher_email = Column(String)
    teacher_slug = Column(String, index=True)
    created_at = Column(DateTime)


class PodDutyNotification(Base):
    __tablename__ = "pod_duty_notifications"

    id = Column(Integer, primary_key=True)
    assignment_date = Column(Date, index=True)
    period_label = Column(String, index=True)
    pod_label = Column(String)
    teacher_slug = Column(String)
    teacher_name = Column(String)
    teacher_email = Column(String)
    assignment_hash = Column(String, unique=True)
    notified_at = Column(DateTime)
