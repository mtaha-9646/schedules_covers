from extensions import db
from dynamic_models import (
    DynamicForm,
    DynamicFormField,
    DynamicFormSubmission,
    DynamicFormSubmissionValue,
)


def ensure_dynamic_form_tables() -> None:
    """Create all dynamic form tables if they do not already exist."""
    with db.engine.connect() as connection:
        DynamicForm.__table__.create(bind=connection, checkfirst=True)
        DynamicFormField.__table__.create(bind=connection, checkfirst=True)
        DynamicFormSubmission.__table__.create(bind=connection, checkfirst=True)
        DynamicFormSubmissionValue.__table__.create(bind=connection, checkfirst=True)
