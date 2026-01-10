
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from extensions import db
from datetime import datetime

class DynamicForm(db.Model):
    __tablename__ = 'dynamic_forms'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by_teacher_id = Column(Integer, nullable=True)
    fields = relationship('DynamicFormField', back_populates='form', cascade="all, delete-orphan")
    submissions = relationship('DynamicFormSubmission', back_populates='form', cascade="all, delete-orphan")

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'created_at': self.created_at.isoformat(),
            'created_by_teacher_id': self.created_by_teacher_id,
            'fields': [field.to_dict() for field in self.fields]
        }

class DynamicFormField(db.Model):
    __tablename__ = 'dynamic_form_fields'
    id = Column(Integer, primary_key=True)
    form_id = Column(Integer, ForeignKey('dynamic_forms.id'), nullable=False)
    label = Column(String(255), nullable=False)
    field_type = Column(String(50), nullable=False)  # e.g., 'text', 'textarea', 'date', 'checkbox', 'select'
    options = Column(JSON)  # For 'select', 'radio', etc.
    order = Column(Integer, nullable=False, default=0)
    form = relationship('DynamicForm', back_populates='fields')

    def to_dict(self):
        return {
            'id': self.id,
            'form_id': self.form_id,
            'label': self.label,
            'field_type': self.field_type,
            'options': self.options,
            'order': self.order
        }

class DynamicFormSubmission(db.Model):
    __tablename__ = 'dynamic_form_submissions'
    id = Column(Integer, primary_key=True)
    form_id = Column(Integer, ForeignKey('dynamic_forms.id'), nullable=False)
    student_esis = Column(String(50), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    submitted_by_teacher_id = Column(Integer, nullable=True)
    form = relationship('DynamicForm', back_populates='submissions')
    values = relationship('DynamicFormSubmissionValue', back_populates='submission', cascade="all, delete-orphan")

    def to_dict(self):
        return {
            'id': self.id,
            'form_id': self.form_id,
            'student_esis': self.student_esis,
            'created_at': self.created_at.isoformat(),
            'submitted_by_teacher_id': self.submitted_by_teacher_id,
            'values': [value.to_dict() for value in self.values]
        }

class DynamicFormSubmissionValue(db.Model):
    __tablename__ = 'dynamic_form_submission_values'
    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('dynamic_form_submissions.id'), nullable=False)
    field_id = Column(Integer, ForeignKey('dynamic_form_fields.id'), nullable=False)
    value = Column(Text, nullable=False)
    submission = relationship('DynamicFormSubmission', back_populates='values')
    field = relationship('DynamicFormField')

    def to_dict(self):
        return {
            'id': self.id,
            'submission_id': self.submission_id,
            'field_id': self.field_id,
            'value': self.value
        }
