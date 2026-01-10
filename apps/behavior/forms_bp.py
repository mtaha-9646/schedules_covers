import json
import os
import re
from functools import wraps
from pathlib import Path
from uuid import uuid4

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    session,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

from auth import login_required
from behaviour import Students
from dynamic_forms_service import ensure_dynamic_form_tables
from dynamic_models import (
    DynamicForm,
    DynamicFormField,
    DynamicFormSubmission,
    DynamicFormSubmissionValue,
)
from extensions import db

forms_bp = Blueprint('forms_bp', __name__, url_prefix='/forms')

FIELD_TYPE_ALIASES = {
    'text': 'short_text',
    'textarea': 'long_text',
    'select': 'single_choice',
    'checkbox': 'boolean',
}

FORM_FIELD_TYPE_ALIASES = FIELD_TYPE_ALIASES.copy()

FORM_FIELD_LABELS = {
    'short_text': 'Short Text',
    'long_text': 'Paragraph',
    'single_choice': 'Single Choice',
    'multi_choice': 'Multiple Choice',
    'date': 'Date',
    'likert': 'Likert Scale',
    'file_upload': 'File Upload',
    'boolean': 'Yes / No',
}

DEFAULT_LIKERT_SCALE = {
    'labels': ['Strongly Disagree', 'Disagree', 'Neutral', 'Agree', 'Strongly Agree'],
    'values': [1, 2, 3, 4, 5],
}

FORM_TEMPLATES = [
    {
        'id': 'incident_follow_up',
        'name': 'Incident Follow-Up',
        'description': 'Capture post-incident reflections and next steps.',
        'fields': [
            {'type': 'short_text', 'label': 'Incident Title', 'description': '', 'required': True},
            {'type': 'date', 'label': 'Follow-Up Date', 'description': '', 'required': True},
            {'type': 'long_text', 'label': 'Summary of Actions Taken', 'description': 'Outline what was done in response to the incident.', 'required': True},
            {'type': 'likert', 'label': 'Student Reflection', 'description': 'How does the student feel about the incident now?', 'required': False},
            {'type': 'multi_choice', 'label': 'Planned Next Steps', 'description': 'Select all actions that will take place.', 'required': False, 'choices': ['Parent Meeting', 'Counseling Session', 'Behaviour Contract', 'Monitoring', 'No further action']},
            {'type': 'file_upload', 'label': 'Supporting Documents', 'description': 'Attach any evidence or reports.', 'required': False, 'file_types': ['pdf', 'jpg', 'png']},
        ],
    },
    {
        'id': 'parent_meeting_feedback',
        'name': 'Parent Meeting Feedback',
        'description': 'Document insights and outcomes from parent meetings.',
        'fields': [
            {'type': 'date', 'label': 'Meeting Date', 'description': '', 'required': True},
            {'type': 'single_choice', 'label': 'Meeting Outcome', 'description': '', 'required': True, 'choices': ['Resolved', 'Partially Resolved', 'Unresolved']},
            {'type': 'likert', 'label': 'Parent Satisfaction', 'description': "Rate the parent's satisfaction with the meeting.", 'required': True},
            {'type': 'long_text', 'label': 'Discussion Notes', 'description': 'Key talking points and agreements.', 'required': True},
            {'type': 'short_text', 'label': 'Next Follow-Up Date (if any)', 'description': 'Optional note for the next check-in.', 'required': False},
        ],
    },
    {
        'id': 'student_check_in',
        'name': 'Student Check-In',
        'description': 'Quick snapshot of student wellbeing and readiness.',
        'fields': [
            {'type': 'single_choice', 'label': 'Overall Mood', 'description': '', 'required': True, 'choices': ['Excellent', 'Good', 'Okay', 'Needs Support']},
            {'type': 'likert', 'label': 'Class Engagement', 'description': 'How engaged was the student today?', 'required': False},
            {'type': 'long_text', 'label': 'Notes', 'description': 'Additional context or observations.', 'required': False},
        ],
    },
]

DYNAMIC_UPLOAD_SUBDIR = Path('uploads') / 'dynamic'


def _admin_login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get('is_admin'):
            flash('Admin access required.', 'error')
            return redirect(url_for('admin_bp.login'))
        return func(*args, **kwargs)

    return wrapper


def _default_likert_scale():
    return {
        'labels': list(DEFAULT_LIKERT_SCALE['labels']),
        'values': list(DEFAULT_LIKERT_SCALE['values']),
    }


def _default_dynamic_likert():
    return {
        'labels': list(DEFAULT_LIKERT_SCALE['labels']),
        'values': list(DEFAULT_LIKERT_SCALE['values']),
    }


def _field_to_builder_payload(field: DynamicFormField) -> dict:
    field_type = FIELD_TYPE_ALIASES.get(field.field_type or 'short_text', field.field_type or 'short_text')
    config = field.options or {}
    if isinstance(config, list):
        config = {'choices': config}
    return {
        'uid': f'existing-{field.id}',
        'type': field_type,
        'label': field.label,
        'description': config.get('description', ''),
        'required': bool(config.get('required')),
        'choices': config.get('choices', []),
        'scale': config.get('scale') or _default_likert_scale(),
        'file_types': config.get('file_types', []),
        'max_size_mb': config.get('max_size_mb', 10),
        'true_label': config.get('true_label', 'Yes'),
        'false_label': config.get('false_label', 'No'),
    }


def _parse_form_payload(payload_raw: str) -> list:
    try:
        data = json.loads(payload_raw or '[]')
    except json.JSONDecodeError as exc:
        raise ValueError('Could not read the question configuration.') from exc

    if not isinstance(data, list):
        raise ValueError('Invalid question configuration payload.')

    parsed = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            continue
        label = (item.get('label') or '').strip()
        field_type = (item.get('type') or '').strip()
        if not label:
            raise ValueError(f'Question {index + 1} is missing a label.')
        if not field_type:
            raise ValueError(f'Question "{label}" is missing a type.')
        field_type = FIELD_TYPE_ALIASES.get(field_type, field_type)

        description = (item.get('description') or '').strip()
        required = bool(item.get('required'))

        config = {
            'description': description,
            'required': required,
        }

        if field_type in ('single_choice', 'multi_choice'):
            choices = item.get('choices') or []
            if isinstance(choices, str):
                choices = [choices]
            choices = [c.strip() for c in choices if isinstance(c, str) and c.strip()]
            if not choices:
                raise ValueError(f'Add at least one option for "{label}".')
            config['choices'] = choices
            if field_type == 'multi_choice':
                config['allow_multiple'] = True
        elif field_type == 'likert':
            scale = item.get('scale') or {}
            labels = scale.get('labels') or DEFAULT_LIKERT_SCALE['labels']
            values = scale.get('values') or list(range(1, len(labels) + 1))
            if len(labels) != len(values):
                values = list(range(1, len(labels) + 1))
            config['scale'] = {'labels': labels, 'values': values}
        elif field_type == 'file_upload':
            file_types = item.get('file_types') or []
            if isinstance(file_types, str):
                file_types = [file_types]
            file_types = [ft.lower().strip().lstrip('.') for ft in file_types if isinstance(ft, str) and ft.strip()]
            max_size = item.get('max_size_mb') or 10
            try:
                max_size = max(1, int(max_size))
            except (ValueError, TypeError):
                max_size = 10
            config['file_types'] = file_types
            config['max_size_mb'] = max_size
        elif field_type == 'boolean':
            true_label = (item.get('true_label') or 'Yes').strip() or 'Yes'
            false_label = (item.get('false_label') or 'No').strip() or 'No'
            config['true_label'] = true_label
            config['false_label'] = false_label
        elif field_type in ('short_text', 'long_text', 'date'):
            pass
        else:
            raise ValueError(f'Unsupported question type "{field_type}" for "{label}".')

        parsed.append({'label': label, 'type': field_type, 'config': config, 'order': index})

    return parsed


def _get_homerooms_grades():
    try:
        homerooms = [hr[0] for hr in db.session.query(Students.homeroom).distinct().all() if hr[0]]
        grades = sorted({extract.group(1) for hr in homerooms if (extract := re.match(r'G(\d+)', hr))})
    except Exception:
        homerooms = []
        grades = []
    return homerooms, grades


def _extract_grade_from_homeroom(homeroom: str | None):
    if not homeroom:
        return ''
    match = re.match(r'G(\d+)', homeroom)
    return match.group(1) if match else ''


def _prepare_fields_for_render(form: DynamicForm):
    prepared = []
    for field in sorted(form.fields, key=lambda f: f.order or 0):
        field_type = FORM_FIELD_TYPE_ALIASES.get(field.field_type or 'short_text', field.field_type or 'short_text')
        config = field.options or {}
        if isinstance(config, list):
            config = {'choices': config}
        config.setdefault('description', '')
        config.setdefault('required', False)
        if field_type in ('single_choice', 'multi_choice'):
            choices = config.get('choices', [])
            if not isinstance(choices, list):
                choices = [choices] if choices else []
            config['choices'] = [str(choice).strip() for choice in choices if str(choice).strip()]
        if field_type == 'likert':
            scale = config.get('scale') or _default_dynamic_likert()
            labels = scale.get('labels') or DEFAULT_LIKERT_SCALE['labels']
            values = scale.get('values') or DEFAULT_LIKERT_SCALE['values']
            if len(labels) != len(values):
                values = list(range(1, len(labels) + 1))
            config['scale'] = {'labels': labels, 'values': values}
        if field_type == 'file_upload':
            file_types = config.get('file_types', [])
            if isinstance(file_types, str):
                file_types = [file_types]
            file_types = [str(ft).lstrip('.').lower() for ft in file_types if str(ft).strip()]
            config['file_types'] = file_types
            config['max_size_mb'] = int(config.get('max_size_mb', 10) or 10)
            config['accept'] = ','.join(f".{ft}" for ft in file_types) if file_types else ''
        if field_type == 'boolean':
            config.setdefault('true_label', 'Yes')
            config.setdefault('false_label', 'No')
        prepared.append({'model': field, 'type': field_type, 'config': config})
    return prepared


@forms_bp.route('/', methods=['GET'])
@_admin_login_required
def manage_forms():
    ensure_dynamic_form_tables()
    forms = db.session.query(DynamicForm).order_by(DynamicForm.name).all()
    return render_template('manage_forms.html', forms=forms)


@forms_bp.route('/new', methods=['GET', 'POST'])
@_admin_login_required
def create_form():
    ensure_dynamic_form_tables()
    if request.method == 'POST':
        name = (request.form.get('name') or '').strip()
        if not name:
            flash('Form name is required.', 'danger')
            return redirect(url_for('forms_bp.create_form'))

        payload_raw = request.form.get('fields_payload') or '[]'
        try:
            fields_payload = _parse_form_payload(payload_raw)
        except ValueError as exc:
            flash(str(exc), 'danger')
            return redirect(url_for('forms_bp.create_form'))

        if not fields_payload:
            flash('Add at least one question before saving the form.', 'warning')
            return redirect(url_for('forms_bp.create_form'))

        new_form = DynamicForm(name=name, created_by_teacher_id=session.get('teacher_id'))
        db.session.add(new_form)
        db.session.commit()

        for field_data in fields_payload:
            db.session.add(
                DynamicFormField(
                    form_id=new_form.id,
                    label=field_data['label'],
                    field_type=field_data['type'],
                    options=field_data['config'],
                    order=field_data['order'],
                )
            )

        db.session.commit()
        flash('Form created successfully!', 'success')
        return redirect(url_for('forms_bp.manage_forms'))

    return render_template(
        'form_builder.html',
        form=None,
        initial_fields=[],
        form_templates=FORM_TEMPLATES,
    )


@forms_bp.route('/<int:form_id>/edit', methods=['GET', 'POST'])
@_admin_login_required
def edit_form(form_id: int):
    ensure_dynamic_form_tables()
    form = db.session.query(DynamicForm).get_or_404(form_id)

    if request.method == 'POST':
        form.name = (request.form.get('name') or '').strip()
        payload_raw = request.form.get('fields_payload') or '[]'
        try:
            fields_payload = _parse_form_payload(payload_raw)
        except ValueError as exc:
            flash(str(exc), 'danger')
            return redirect(url_for('forms_bp.edit_form', form_id=form.id))

        if not fields_payload:
            flash('Add at least one question before saving the form.', 'warning')
            return redirect(url_for('forms_bp.edit_form', form_id=form.id))

        for field in form.fields:
            db.session.delete(field)
        db.session.commit()

        for field_data in fields_payload:
            db.session.add(
                DynamicFormField(
                    form_id=form.id,
                    label=field_data['label'],
                    field_type=field_data['type'],
                    options=field_data['config'],
                    order=field_data['order'],
                )
            )

        db.session.commit()
        flash('Form updated successfully!', 'success')
        return redirect(url_for('forms_bp.manage_forms'))

    initial_fields = [
        _field_to_builder_payload(field)
        for field in sorted(form.fields, key=lambda f: f.order or 0)
    ]
    return render_template(
        'form_builder.html',
        form=form,
        initial_fields=initial_fields,
        form_templates=FORM_TEMPLATES,
    )


@forms_bp.route('/<int:form_id>/delete', methods=['POST'])
@_admin_login_required
def delete_form(form_id: int):
    ensure_dynamic_form_tables()
    form = db.session.query(DynamicForm).get_or_404(form_id)
    db.session.delete(form)
    db.session.commit()
    flash('Form deleted successfully!', 'success')
    return redirect(url_for('forms_bp.manage_forms'))


@forms_bp.route('/dynamic/<int:form_id>', methods=['GET', 'POST'])
@login_required
def fill_dynamic_form(form_id: int):
    ensure_dynamic_form_tables()
    form = db.session.query(DynamicForm).get_or_404(form_id)
    homerooms, detected_grades = _get_homerooms_grades()
    grades = sorted(set(detected_grades))
    prepared_fields = _prepare_fields_for_render(form)

    selected_grade = (request.values.get('grade_filter') or '').strip()
    selected_homeroom = (request.values.get('homeroom_filter') or '').strip()

    esis = (request.form.get('student_esis') or request.args.get('esis') or '').strip()
    student = db.session.query(Students).filter_by(esis=esis).first() if esis else None

    student_name_fallback = (request.form.get('student_name') or '').strip()
    student_homeroom_fallback = (request.form.get('student_homeroom') or '').strip()

    if student and student.homeroom:
        selected_grade = selected_grade or _extract_grade_from_homeroom(student.homeroom) or ''
        selected_homeroom = selected_homeroom or student.homeroom

    student_payload = {
        'esis': student.esis if student else esis,
        'name': student.name if student else student_name_fallback,
        'homeroom': student.homeroom if student else student_homeroom_fallback,
        'grade': _extract_grade_from_homeroom(student.homeroom if student else student_homeroom_fallback),
    }

    form_data = {}

    if request.method == 'POST':
        errors = []
        collected_values = []
        saved_files = []

        if not esis:
            errors.append('Please select a student before submitting the form.')
        elif not student:
            errors.append('Selected student could not be found. Please choose again.')

        uploads_root = Path(current_app.root_path) / DYNAMIC_UPLOAD_SUBDIR
        uploads_root.mkdir(parents=True, exist_ok=True)

        for entry in prepared_fields:
            field = entry['model']
            field_type = entry['type']
            config = entry['config']
            field_key = f'field_{field.id}'
            required = bool(config.get('required'))
            stored_value = ''

            if field_type == 'multi_choice':
                selections = request.form.getlist(field_key)
                form_data[field_key] = {'selected': selections}
                if required and not selections:
                    errors.append(f'Please select at least one option for "{field.label}".')
                stored_value = ', '.join(selections)
            elif field_type == 'file_upload':
                file_obj = request.files.get(field_key)
                filename = secure_filename(file_obj.filename) if file_obj and file_obj.filename else ''
                form_data[field_key] = {'value': filename}
                if required and not filename:
                    errors.append(f'Upload a file for "{field.label}".')
                elif file_obj and filename:
                    extension = Path(filename).suffix.lstrip('.').lower()
                    allowed = config.get('file_types', [])
                    if allowed and extension not in allowed:
                        errors.append(f'"{field.label}" only accepts: {", ".join(allowed)}.')
                    else:
                        max_size = int(config.get('max_size_mb', 10) or 10) * 1024 * 1024
                        file_obj.stream.seek(0, os.SEEK_END)
                        size_bytes = file_obj.stream.tell()
                        file_obj.stream.seek(0)
                        if size_bytes > max_size:
                            errors.append(f'"{field.label}" exceeds the {config.get("max_size_mb", 10)} MB limit.')
                        else:
                            target_dir = uploads_root / str(form.id)
                            target_dir.mkdir(parents=True, exist_ok=True)
                            unique_name = f"{uuid4().hex}{Path(filename).suffix.lower()}"
                            dest_path = target_dir / unique_name
                            file_obj.save(dest_path)
                            saved_files.append(dest_path)
                            relative_path = dest_path.relative_to(Path(current_app.root_path))
                            stored_value = f"file::{relative_path.as_posix()}::{filename}"
            elif field_type == 'boolean':
                value = request.form.get(field_key, '')
                form_data[field_key] = {'value': value}
                if required and not value:
                    errors.append(f'Please choose an option for "{field.label}".')
                if value == 'true':
                    stored_value = config.get('true_label', 'Yes')
                elif value == 'false':
                    stored_value = config.get('false_label', 'No')
            else:
                value = (request.form.get(field_key) or '').strip()
                form_data[field_key] = {'value': value}
                if required and not value:
                    errors.append(f'"{field.label}" is required.')
                stored_value = value

            if stored_value:
                collected_values.append((field, stored_value))

        if errors:
            for path in saved_files:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    current_app.logger.warning('Failed to remove temporary upload: %s', path)
            for message in errors:
                flash(message, 'danger')
            return render_template(
                'dynamic_form.html',
                form=form,
                prepared_fields=prepared_fields,
                form_data=form_data,
                student_info=student_payload,
                grades=grades,
                selected_grade=selected_grade,
                selected_homeroom=selected_homeroom,
                field_type_labels=FORM_FIELD_LABELS,
            )

        submission = DynamicFormSubmission(
            form_id=form.id,
            student_esis=esis,
            submitted_by_teacher_id=session.get('teacher_id'),
        )
        db.session.add(submission)
        db.session.commit()

        for field, stored_value in collected_values:
            db.session.add(
                DynamicFormSubmissionValue(
                    submission_id=submission.id,
                    field_id=field.id,
                    value=stored_value,
                )
            )

        db.session.commit()
        flash(f'{form.name} submitted successfully!', 'success')
        return redirect(url_for('behaviour_bp.student_incidents_page', esis=esis))

    return render_template(
        'dynamic_form.html',
        form=form,
        prepared_fields=prepared_fields,
        form_data=form_data,
        student_info=student_payload,
        grades=grades,
        selected_grade=selected_grade,
        selected_homeroom=selected_homeroom,
        field_type_labels=FORM_FIELD_LABELS,
    )


@forms_bp.route('/dynamic/submission/<int:submission_id>', methods=['GET'])
@login_required
def view_dynamic_submission(submission_id: int):
    ensure_dynamic_form_tables()
    submission = db.session.query(DynamicFormSubmission).get_or_404(submission_id)
    return render_template(
        'view_dynamic_submission.html',
        submission=submission,
        field_type_labels=FORM_FIELD_LABELS,
        field_type_aliases=FORM_FIELD_TYPE_ALIASES,
    )


@forms_bp.route('/dynamic/file/<int:value_id>', methods=['GET'])
@login_required
def download_dynamic_file(value_id: int):
    ensure_dynamic_form_tables()
    value = db.session.query(DynamicFormSubmissionValue).get_or_404(value_id)
    if not value.value or not value.value.startswith('file::'):
        abort(404)
    try:
        _, relative_path, original_name = value.value.split('::', 2)
    except ValueError:
        abort(404)
    abs_path = Path(current_app.root_path) / relative_path
    if not abs_path.exists() or not abs_path.is_file():
        abort(404)
    return send_file(abs_path, as_attachment=True, download_name=original_name or abs_path.name)
