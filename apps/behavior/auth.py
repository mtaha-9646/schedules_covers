# auth.py
from flask import Blueprint, request, render_template, session, redirect, url_for, flash, current_app
from extensions import db
from behaviour import Teacher
from werkzeug.security import check_password_hash

# Optional role mapping without altering teachers table
try:
    from behaviour import TeacherRole
except Exception:
    TeacherRole = None

auth_bp = Blueprint('auth_bp', __name__)

GRADE_LEAD_PREFIX = 'grade_lead_'
GRADE_LEAD_GRADES = {'6', '7', '10', '11', '12'}
EXTRA_PERMISSIONS = {'slt', 'administrator'}
ALLOWED_ROLES = {'teacher', 'admin', 'pa'} | EXTRA_PERMISSIONS | {f'{GRADE_LEAD_PREFIX}{grade}' for grade in GRADE_LEAD_GRADES}


def _normalise_role(value: str) -> str:
    role = (value or 'teacher').strip().lower()
    if role not in ALLOWED_ROLES:
        return 'teacher'
    return role


def login_required(func):
    from functools import wraps
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Allow either a logged-in teacher or super admin session
        if not (session.get('teacher_id') or session.get('is_admin')):
            return redirect(url_for('auth_bp.login'))
        return func(*args, **kwargs)
    return wrapper

def is_admin():
    return session.get('is_admin', False)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''

        teacher = db.session.query(Teacher).filter_by(email=email).first()
        valid_password = False
        if teacher:
            # Accept either plaintext (legacy) or hashed password
            try:
                if check_password_hash(teacher.password, password):
                    valid_password = True
            except Exception:
                # teacher.password might be plaintext
                valid_password = (teacher.password == password)

        if not teacher or not valid_password:
            flash('Invalid credentials', 'error')
            return redirect(url_for('auth_bp.login'))

        session['teacher_id'] = teacher.id
        session['teacher_name'] = teacher.name
        # Role resolution: prefer explicit mapping, fallback to legacy email check
        role_value = 'teacher'
        if TeacherRole is not None:
            try:
                role_row = db.session.query(TeacherRole).filter_by(teacher_id=teacher.id).first()
                if role_row:
                    role_value = _normalise_role(role_row.role or 'teacher')
            except Exception:
                # Roles table may not exist yet; default to teacher
                role_value = 'teacher'
        role_value = _normalise_role(role_value)
        session['role'] = role_value
        session['is_admin'] = (role_value == 'admin') or (teacher.email == 'admin@example.com')
        session['is_pa'] = (role_value == 'pa')
        if role_value.startswith(GRADE_LEAD_PREFIX):
            session['is_grade_lead'] = True
            session['grade_lead_grade'] = role_value[len(GRADE_LEAD_PREFIX):]
        else:
            session['is_grade_lead'] = False
            session.pop('grade_lead_grade', None)
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))

    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth_bp.login'))
