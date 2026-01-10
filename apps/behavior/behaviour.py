from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import joinedload
from datetime import datetime
from typing import List, Optional
import logging
import re
from extensions import db  # Import db from database.py

# Configure logging for better error tracking
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

EMAIL_SPLIT_RE = re.compile(r'[\s,;]+')
EMAIL_VALID_RE = re.compile(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')


def _clean_email_tokens(raw: Optional[str]) -> list[str]:
    tokens = [token.strip() for token in EMAIL_SPLIT_RE.split(raw or '') if token and token.strip()]
    return [token for token in tokens if EMAIL_VALID_RE.match(token)]

class Teacher(db.Model):
    __bind_key__ = 'teachers_bind'  # Ensure this bind is configured in Flask app
    __tablename__ = 'teachers'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    subject = db.Column(db.String(120), nullable=True)
    grade = db.Column(db.String(20), nullable=True)

    def __repr__(self):
        return f"<Teacher {self.name}>"


class TeacherRole(db.Model):
    __tablename__ = 'teacher_roles'
    __bind_key__ = 'teachers_bind'
    id = db.Column(db.Integer, primary_key=True)
    teacher_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    role = db.Column(db.String(20), nullable=False, default='teacher')  # 'teacher' or 'admin'

    def __repr__(self):
        return f"<TeacherRole teacher_id={self.teacher_id} role={self.role}>"


class Students(db.Model):
    __tablename__ = 'students'
    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, unique=True, index=True)
    name = db.Column(db.String(150), nullable=False)
    homeroom = db.Column(db.String(50), nullable=True)

    def __repr__(self):
        return f"<Student {self.esis}:{self.name}>"

class Incident(db.Model):
    __tablename__ = 'incidents'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    name = db.Column(db.String(100), nullable=False)
    homeroom = db.Column(db.String(50), nullable=False)
    date_of_incident = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    place_of_incident = db.Column(db.String(200), nullable=False)
    incident_grade = db.Column(db.String(50), nullable=False)
    action_taken = db.Column(db.String(200), nullable=False)
    incident_description = db.Column(db.Text, nullable=False)
    attachment = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    teacher_id = db.Column(db.Integer, nullable=False)
    teacher = db.relationship(
        'Teacher',
        foreign_keys=[teacher_id],
        primaryjoin="Incident.teacher_id == Teacher.id",
        backref=db.backref('incidents', lazy='select')
    )

    def __repr__(self):
        return f"<Incident {self.id} for {self.name}>"

    def to_dict(self):
        """Convert Incident object to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'esis': self.esis,
            'name': self.name,
            'homeroom': self.homeroom,
            'date_of_incident': self.date_of_incident.strftime('%Y-%m-%d %H:%M'),
            'place_of_incident': self.place_of_incident,
            'incident_grade': self.incident_grade,
            'action_taken': self.action_taken,
            'incident_description': self.incident_description,
            'attachment': self.attachment,
            'created_at': self.created_at.isoformat(),
            'teacher_name': self.teacher.name if self.teacher else 'Unknown'
        }


class Suspension(db.Model):
    __tablename__ = 'suspensions'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade_class = db.Column(db.String(50), nullable=True)
    date_of_suspension = db.Column(db.Date, nullable=False)
    duration = db.Column(db.String(50), nullable=False)
    reason = db.Column(db.Text, nullable=True)
    incident_details = db.Column(db.Text, nullable=True)
    parent_contacted = db.Column(db.Boolean, nullable=False, default=False)
    parent_meeting = db.Column(db.Boolean, nullable=False, default=False)
    behavior_plan = db.Column(db.Text, nullable=True)
    assigned_staff = db.Column(db.String(150), nullable=True)
    reintegration_plan = db.Column(db.Text, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<Suspension {self.id} {self.student_name} ({self.esis})>"

    def to_dict(self):
        return {
            'id': self.id,
            'esis': self.esis,
            'student_name': self.student_name,
            'grade_class': self.grade_class or '',
            'date_of_suspension': self.date_of_suspension.strftime('%Y-%m-%d'),
            'duration': self.duration,
            'reason': self.reason or '',
            'incident_details': self.incident_details or '',
            'parent_contacted': bool(self.parent_contacted),
            'parent_meeting': bool(self.parent_meeting),
            'behavior_plan': self.behavior_plan or '',
            'assigned_staff': self.assigned_staff or '',
            'reintegration_plan': self.reintegration_plan or '',
            'notes': self.notes or '',
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M')
        }


class ParentMeeting(db.Model):
    __tablename__ = 'parent_meetings'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade_session = db.Column(db.String(50), nullable=True)
    parent_name = db.Column(db.String(150), nullable=True)
    attended_by = db.Column(db.String(255), nullable=True)
    date = db.Column(db.Date, nullable=False)
    time = db.Column(db.String(20), nullable=True)
    requested_by = db.Column(db.String(100), nullable=True)
    parent_concerns = db.Column(db.Text, nullable=True)
    school_concerns = db.Column(db.Text, nullable=True)
    solutions_parent = db.Column(db.Text, nullable=True)
    solutions_school = db.Column(db.Text, nullable=True)
    agreed_next_steps = db.Column(db.Text, nullable=True)
    required_signature_roles = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<ParentMeeting {self.id} {self.student_name} ({self.esis})>"


class ParentAcknowledgment(db.Model):
    __tablename__ = 'parent_acknowledgments'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade_session = db.Column(db.String(50), nullable=True)
    date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<ParentAck {self.id} {self.student_name} ({self.esis})>"


class StudentStatement(db.Model):
    __tablename__ = 'student_statements'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    class_session = db.Column(db.String(50), nullable=True)
    date = db.Column(db.Date, nullable=False)
    time = db.Column(db.String(20), nullable=True)
    location = db.Column(db.String(200), nullable=True)
    statement = db.Column(db.Text, nullable=True)
    other_details = db.Column(db.Text, nullable=True)
    file_path = db.Column(db.String(500), nullable=True)
    file_name = db.Column(db.String(255), nullable=True)
    file_mime = db.Column(db.String(120), nullable=True)
    file_size = db.Column(db.Integer, nullable=True)
    completed_by = db.Column(db.String(150), nullable=True)
    completed_by_role = db.Column(db.String(100), nullable=True)
    reviewed_by = db.Column(db.String(150), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<StudentStatement {self.id} {self.student_name} ({self.esis})>"


class StaffStatement(db.Model):
    __tablename__ = 'staff_statements'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    staff_name = db.Column(db.String(150), nullable=False)
    position = db.Column(db.String(100), nullable=True)
    date_of_incident = db.Column(db.Date, nullable=True)
    time_of_incident = db.Column(db.String(20), nullable=True)
    location_of_incident = db.Column(db.String(200), nullable=True)
    date_of_statement = db.Column(db.Date, nullable=True)
    details = db.Column(db.Text, nullable=True)
    individuals_involved = db.Column(db.Text, nullable=True)
    actions_taken = db.Column(db.Text, nullable=True)
    witnesses = db.Column(db.Text, nullable=True)
    additional_comments = db.Column(db.Text, nullable=True)
    slt_name = db.Column(db.String(150), nullable=True)
    slt_position = db.Column(db.String(100), nullable=True)
    slt_date_review = db.Column(db.Date, nullable=True)
    slt_actions = db.Column(db.Text, nullable=True)
    required_signature_roles = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<StaffStatement {self.id} {self.staff_name} ({self.esis})>"


class SafeguardingConcern(db.Model):
    __tablename__ = 'safeguarding_concerns'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade_session = db.Column(db.String(50), nullable=True)

    reporting_name = db.Column(db.String(150), nullable=False)
    reporting_role = db.Column(db.String(50), nullable=False)
    report_date = db.Column(db.Date, nullable=False)
    report_time = db.Column(db.String(20), nullable=True)

    incident_date = db.Column(db.Date, nullable=True)
    incident_time = db.Column(db.String(20), nullable=True)
    incident_location = db.Column(db.String(200), nullable=True)
    description = db.Column(db.Text, nullable=True)
    concern_types = db.Column(db.Text, nullable=True)  # comma-separated list
    student_disclosure = db.Column(db.String(10), nullable=True)  # Yes/No
    disclosure_details = db.Column(db.Text, nullable=True)
    immediate_actions = db.Column(db.Text, nullable=True)
    referred_to = db.Column(db.String(200), nullable=True)
    referral_time = db.Column(db.String(20), nullable=True)
    referral_date = db.Column(db.Date, nullable=True)
    follow_up_actions = db.Column(db.Text, nullable=True)
    additional_notes = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<SafeguardingConcern {self.id} {self.student_name} ({self.esis})>"


class PhoneViolationContract(db.Model):
    __tablename__ = 'phone_violation_contracts'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade_session = db.Column(db.String(50), nullable=True)
    date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<PhoneViolation {self.id} {self.student_name} ({self.esis})>"




class CounselingSession(db.Model):
    __tablename__ = 'counseling_sessions'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    homeroom = db.Column(db.String(50), nullable=True)
    session_date = db.Column(db.Date, nullable=False)
    duration_minutes = db.Column(db.Integer, nullable=True)
    counselors = db.Column(db.String(255), nullable=True)

    # Individual counseling focus areas
    focus_study_skills = db.Column(db.Boolean, nullable=False, default=False)
    focus_time_management = db.Column(db.Boolean, nullable=False, default=False)
    focus_academic_goal_setting = db.Column(db.Boolean, nullable=False, default=False)
    focus_self_esteem = db.Column(db.Boolean, nullable=False, default=False)
    focus_decision_making = db.Column(db.Boolean, nullable=False, default=False)
    focus_mindfulness_relaxation = db.Column(db.Boolean, nullable=False, default=False)
    focus_stress_management = db.Column(db.Boolean, nullable=False, default=False)
    focus_coping_anxiety = db.Column(db.Boolean, nullable=False, default=False)
    focus_conflict_resolution = db.Column(db.Boolean, nullable=False, default=False)
    focus_immediate_crisis_support = db.Column(db.Boolean, nullable=False, default=False)
    focus_grief_loss = db.Column(db.Boolean, nullable=False, default=False)
    focus_support_trauma = db.Column(db.Boolean, nullable=False, default=False)
    focus_managing_anger = db.Column(db.Boolean, nullable=False, default=False)
    focus_improving_communication = db.Column(db.Boolean, nullable=False, default=False)
    focus_positive_habits = db.Column(db.Boolean, nullable=False, default=False)

    # Group counseling focus areas
    group_building_friendships = db.Column(db.Boolean, nullable=False, default=False)
    group_developing_empathy = db.Column(db.Boolean, nullable=False, default=False)
    group_grief_loss_support = db.Column(db.Boolean, nullable=False, default=False)
    group_anxiety_depression_support = db.Column(db.Boolean, nullable=False, default=False)
    group_learning_disabilities_support = db.Column(db.Boolean, nullable=False, default=False)
    group_stress_management = db.Column(db.Boolean, nullable=False, default=False)
    group_leadership_training = db.Column(db.Boolean, nullable=False, default=False)
    group_team_building = db.Column(db.Boolean, nullable=False, default=False)
    group_community_service = db.Column(db.Boolean, nullable=False, default=False)
    group_mediation_skills = db.Column(db.Boolean, nullable=False, default=False)
    group_role_play_conflict = db.Column(db.Boolean, nullable=False, default=False)
    group_communication_strategies = db.Column(db.Boolean, nullable=False, default=False)

    summary_of_progress = db.Column(db.Text, nullable=True)
    progress_toward_goals = db.Column(db.Text, nullable=True)
    follow_up_challenges = db.Column(db.Text, nullable=True)
    follow_up_support = db.Column(db.Text, nullable=True)
    future_sessions_planned = db.Column(db.Text, nullable=True)
    additional_support_needed = db.Column(db.Text, nullable=True)
    parent_guardian_communication = db.Column(db.Text, nullable=True)
    counselor_observations = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<CounselingSession {self.id} {self.student_name} ({self.esis})>"

class BehaviorContract(db.Model):
    __tablename__ = 'behavior_contracts'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade = db.Column(db.String(20), nullable=True)
    date = db.Column(db.Date, nullable=False)
    time = db.Column(db.String(20), nullable=True)

    # Consequences (checkboxes)
    cons_warning = db.Column(db.Boolean, nullable=False, default=False)
    cons_parent_meeting = db.Column(db.Boolean, nullable=False, default=False)
    cons_detention = db.Column(db.Boolean, nullable=False, default=False)
    cons_referral = db.Column(db.Boolean, nullable=False, default=False)
    cons_further_action = db.Column(db.Boolean, nullable=False, default=False)
    cons_further_action_text = db.Column(db.String(255), nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<BehaviorContract {self.id} {self.student_name} ({self.esis})>"


class StudentConductPledge(db.Model):
    __tablename__ = 'student_conduct_pledges'

    id = db.Column(db.Integer, primary_key=True)
    esis = db.Column(db.String(50), nullable=False, index=True)
    student_name = db.Column(db.String(150), nullable=False)
    grade = db.Column(db.String(20), nullable=True)
    date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<StudentConductPledge {self.id} {self.student_name} ({self.esis})>"


class AcademicTerm(db.Model):
    __tablename__ = 'academic_terms'

    slug = db.Column(db.String(50), primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    start_date = db.Column(db.Date, nullable=True)
    end_date = db.Column(db.Date, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<AcademicTerm {self.slug} ({self.name})>"


class IncidentNotificationSetting(db.Model):
    __tablename__ = 'incident_notification_settings'

    id = db.Column(db.Integer, primary_key=True)
    recipient_email = db.Column(db.String(255), nullable=True)
    enabled = db.Column(db.Boolean, nullable=False, default=False)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<IncidentNotificationSetting enabled={self.enabled} email={self.recipient_email}>"


class GradeNotificationSetting(db.Model):
    __tablename__ = 'grade_notification_settings'

    grade = db.Column(db.String(10), primary_key=True)
    lead_email = db.Column(db.String(255), nullable=True)
    recipient_emails = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<GradeNotificationSetting grade={self.grade}>"


class SickLeaveGradeRecipient(db.Model):
    __tablename__ = 'sick_leave_grade_recipients'

    grade = db.Column(db.String(10), primary_key=True)
    recipient_emails = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<SickLeaveGradeRecipient grade={self.grade}>"

    def email_list(self):
        return _clean_email_tokens(self.recipient_emails)


class Signature(db.Model):
    __tablename__ = 'signatures'

    id = db.Column(db.Integer, primary_key=True)
    entity_type = db.Column(db.String(20), nullable=False)  # 'incident' or 'suspension'
    entity_id = db.Column(db.Integer, nullable=False)
    signer_name = db.Column(db.String(150), nullable=False)
    signer_role = db.Column(db.String(50), nullable=False)
    image_data = db.Column(db.Text, nullable=False)  # base64 PNG data (may include data URL prefix)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_by_teacher_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<Signature {self.id} {self.entity_type}:{self.entity_id} {self.signer_role} {self.signer_name}>"

    def to_dict(self):
        # Ensure data URL prefix for direct <img src="...">
        data = self.image_data or ''
        prefix = 'data:image/png;base64,'
        if not data.startswith('data:'):
            data = prefix + data
        return {
            'id': self.id,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'signer_name': self.signer_name,
            'signer_role': self.signer_role,
            'image_data': data,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M')
        }


class TeacherSignature(db.Model):
    __tablename__ = 'teacher_signatures'
    id = db.Column(db.Integer, primary_key=True)
    teacher_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    image_data = db.Column(db.Text, nullable=False)  # base64 PNG (no data: prefix)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self):
        data = self.image_data or ''
        if not data.startswith('data:'):
            data = 'data:image/png;base64,' + data
        return {
            'teacher_id': self.teacher_id,
            'image_data': data,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M')
        }

def create_incident(
    esis: str, name: str, homeroom: str, date_of_incident: datetime,
    place_of_incident: str, incident_grade: str, action_taken: str,
    incident_description: str, teacher_id: int, attachment: Optional[str] = None
) -> Incident:
    """Create a new incident record with input validation."""
    if not all([esis, name, homeroom, place_of_incident, incident_grade, action_taken, incident_description]):
        raise ValueError("All required fields must be provided and non-empty.")
    if not isinstance(date_of_incident, datetime):
        raise ValueError("date_of_incident must be a datetime object.")
    if not db.session.get(Teacher, teacher_id):
        raise ValueError("Invalid teacher_id: Teacher does not exist.")

    incident = Incident(
        esis=esis, name=name, homeroom=homeroom,
        date_of_incident=date_of_incident, place_of_incident=place_of_incident,
        incident_grade=incident_grade, action_taken=action_taken,
        incident_description=incident_description, teacher_id=teacher_id,
        attachment=attachment
    )
    db.session.add(incident)
    db.session.commit()
    return incident

def get_all_incidents(
    page: int = 1, per_page: int = 10, start_date: Optional[str] = None,
    end_date: Optional[str] = None, grade: Optional[str] = None, homeroom: Optional[str] = None
) -> dict:
    """
    Retrieve a paginated and filtered list of all incidents.
    Args:
        page: Page number for pagination.
        per_page: Number of incidents per page.
        start_date: Filter incidents after this date (YYYY-MM-DD).
        end_date: Filter incidents before this date (YYYY-MM-DD).
        grade: Filter by incident grade.
        homeroom: Filter by homeroom.
    Returns:
        Dictionary containing incidents, total count, pages, and current page.
    """
    query = Incident.query.options(joinedload(Incident.teacher))

    if start_date:
        try:
            query = query.filter(Incident.date_of_incident >= datetime.strptime(start_date, '%Y-%m-%d'))
        except ValueError as e:
            logger.warning(f"Invalid start_date format: {start_date}. Error: {e}")
            raise ValueError(f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD.")
    if end_date:
        try:
            end = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            query = query.filter(Incident.date_of_incident <= end)
        except ValueError as e:
            logger.warning(f"Invalid end_date format: {end_date}. Error: {e}")
            raise ValueError(f"Invalid end_date format: {end_date}. Expected YYYY-MM-DD.")
    if grade:
        query = query.filter(Incident.incident_grade == grade)
    if homeroom:
        query = query.filter(Incident.homeroom == homeroom)

    pagination = query.order_by(
        Incident.date_of_incident.desc()
    ).paginate(page=page, per_page=per_page, error_out=False)
    
    return {
        'incidents': [incident.to_dict() for incident in pagination.items],
        'total': pagination.total,
        'pages': pagination.pages,
        'current_page': pagination.page
    }

def get_incident_by_id(incident_id: int) -> Optional[Incident]:
    """Retrieve an incident by its ID, including teacher info."""
    return db.session.get(Incident, incident_id, options=[joinedload(Incident.teacher)])

def get_incidents_by_esis(esis: str) -> List[Incident]:
    """Retrieve all incidents for a specific student by ESIS."""
    return Incident.query.filter_by(esis=esis).options(joinedload(Incident.teacher)).order_by(
        Incident.date_of_incident.desc()
    ).all()

def get_incidents_by_homeroom(homeroom: str) -> List[Incident]:
    """Retrieve all incidents for a specific homeroom."""
    return Incident.query.filter_by(homeroom=homeroom).options(joinedload(Incident.teacher)).order_by(
        Incident.date_of_incident.desc()
    ).all()

def get_incidents_by_grade(incident_grade: str) -> List[Incident]:
    """Retrieve all incidents of a specific grade (e.g., Minor, Major)."""
    return Incident.query.filter_by(incident_grade=incident_grade).options(joinedload(Incident.teacher)).order_by(
        Incident.date_of_incident.desc()
    ).all()

def update_incident(
    incident_id: int,
    esis: Optional[str] = None,
    name: Optional[str] = None,
    homeroom: Optional[str] = None,
    date_of_incident: Optional[datetime] = None,
    place_of_incident: Optional[str] = None,
    incident_grade: Optional[str] = None,
    action_taken: Optional[str] = None,
    incident_description: Optional[str] = None,
    attachment: Optional[str] = None
) -> Optional[Incident]:
    """Update an existing incident record with input validation."""
    incident = db.session.get(Incident, incident_id)
    if not incident:
        return None
    
    if esis is not None and esis.strip():
        incident.esis = esis
    if name is not None and name.strip():
        incident.name = name
    if homeroom is not None and homeroom.strip():
        incident.homeroom = homeroom
    if date_of_incident is not None:
        if not isinstance(date_of_incident, datetime):
            raise ValueError("date_of_incident must be a datetime object.")
        incident.date_of_incident = date_of_incident
    if place_of_incident is not None and place_of_incident.strip():
        incident.place_of_incident = place_of_incident
    if incident_grade is not None and incident_grade.strip():
        incident.incident_grade = incident_grade
    if action_taken is not None and action_taken.strip():
        incident.action_taken = action_taken
    if incident_description is not None and incident_description.strip():
        incident.incident_description = incident_description
    if attachment is not None:  # Attachment can be None or empty
        incident.attachment = attachment
    
    db.session.commit()
    return incident

def delete_incident(incident_id: int) -> bool:
    """Delete an incident by its ID."""
    incident = db.session.get(Incident, incident_id)
    if not incident:
        return False
    db.session.delete(incident)
    db.session.commit()
    return True
