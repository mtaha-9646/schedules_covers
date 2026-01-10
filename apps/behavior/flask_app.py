# flask_app.py
import os
from flask import Flask, redirect, url_for, session
from sqlalchemy import inspect, text
from extensions import db
from auth import auth_bp
from behaviour_bp import behaviour_bp
from admin_bp import admin_bp
from api import api_bp
from grade_lead import grade_lead_bp
from duty_admin import duty_admin_bp
from forms_bp import forms_bp

app = Flask(__name__)
app.secret_key = "your-very-secret-key"

# Suite Auth Integration
from shared.auth_lib import SuiteAuthMiddleware
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://control-plane:8000")
app.wsgi_app = SuiteAuthMiddleware(app.wsgi_app, CONTROL_PLANE_URL)

LOADING_SNIPPET = """
<style id="global-loading-style">
  #global-loading-overlay {
    position: fixed;
    inset: 0;
    background: rgba(15, 23, 42, 0.8);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 2147483000;
    opacity: 0;
    pointer-events: none;
    transition: opacity 0.2s ease-in-out;
  }
  #global-loading-overlay.visible {
    opacity: 1;
    pointer-events: all;
  }
  #global-loading-overlay .global-loading-panel {
    background: #fff;
    border-radius: 1rem;
    padding: 1.5rem 2rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    box-shadow: 0 25px 45px -20px rgba(15, 23, 42, 0.35);
    min-width: 260px;
  }
  #global-loading-overlay .global-loading-panel .global-loading-spin {
    width: 2.75rem;
    height: 2.75rem;
    border-radius: 9999px;
    border: 3px solid rgba(14, 165, 233, 0.25);
    border-top-color: #0ea5e9;
    animation: global-spin 1s linear infinite;
  }
  #global-loading-overlay .global-loading-title {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #94a3b8;
    margin: 0;
  }
  #global-loading-overlay .global-loading-message {
    margin: 0.1rem 0 0;
    font-weight: 600;
    color: #0f172a;
  }
  @keyframes global-spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }
</style>
<div id="global-loading-overlay" aria-live="polite" aria-busy="true">
  <div class="global-loading-panel">
    <span class="global-loading-spin" aria-hidden="true"></span>
    <div>
      <p class="global-loading-title">Please wait</p>
      <p class="global-loading-message" data-loading-label>Processing...</p>
    </div>
  </div>
</div>
<script>
  (function(){
    const overlay = document.getElementById('global-loading-overlay');
    if (!overlay) return;
    const label = overlay.querySelector('[data-loading-label]');
    let hideTimer = null;

    const show = (text) => {
      if (hideTimer) {
        clearTimeout(hideTimer);
        hideTimer = null;
      }
      if (label) {
        label.textContent = text || 'Working...';
      }
      overlay.classList.add('visible');
    };

    const hide = () => {
      hideTimer = setTimeout(() => {
        overlay.classList.remove('visible');
        hideTimer = null;
      }, 150);
    };

    const shouldSkip = (node) => node && node.closest('[data-disable-global-loader]');

    window.__loadingOverlay = {
      show,
      hide,
      isVisible: () => overlay.classList.contains('visible'),
    };

    document.addEventListener('submit', (event) => {
      const form = event.target;
      if (!(form instanceof HTMLFormElement)) return;
      if (event.defaultPrevented || shouldSkip(form)) return;
      show(form.getAttribute('data-loader-text') || 'Processing...');
    }, true);

    document.addEventListener('click', (event) => {
      const trigger = event.target.closest('[data-show-loader]');
      if (!trigger || shouldSkip(trigger)) return;
      show(trigger.getAttribute('data-loader-text') || 'Working...');
    });
  })();
</script>
"""

# Database config
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://admin:password@db:5432/saas_suite')

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Postgres schema configurations
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "connect_args": {
        "options": "-c search_path=behavior,public"
    }
}
app.config['SQLALCHEMY_BINDS'] = {
    'teachers_bind': DATABASE_URL
}

# Initialize extensions with the app
db.init_app(app)

def _ensure_teacher_optional_columns():
    """Add optional teacher columns (subject, grade) if they do not exist (SQLite only)."""
    try:
        inspector = inspect(db.engine)
        columns = {col['name'] for col in inspector.get_columns('teachers')}
        if 'subject' not in columns:
            with db.engine.begin() as conn:
                conn.execute(text("ALTER TABLE teachers ADD COLUMN subject VARCHAR(120)"))
        if 'grade' not in columns:
            with db.engine.begin() as conn:
                conn.execute(text("ALTER TABLE teachers ADD COLUMN grade VARCHAR(20)"))
    except Exception as exc:
        app.logger.warning('Teacher optional column check skipped/failed: %s', exc)

with app.app_context():
    _ensure_teacher_optional_columns()

# Register blueprints (leave_bp removed)
app.register_blueprint(auth_bp)
app.register_blueprint(behaviour_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(grade_lead_bp)
app.register_blueprint(duty_admin_bp)
app.register_blueprint(api_bp)
app.register_blueprint(forms_bp)

@app.after_request
def inject_loading_overlay(response):
    content_type = response.headers.get('Content-Type', '')
    if (
        response.direct_passthrough
        or not content_type
        or 'text/html' not in content_type.lower()
    ):
        return response
    body = response.get_data(as_text=True)
    if '</body>' not in body or 'global-loading-overlay' in body:
        return response
    updated_body = body.replace('</body>', f'{LOADING_SNIPPET}</body>', 1)
    response.set_data(updated_body)
    response.headers['Content-Length'] = len(response.get_data())
    return response

@app.route('/')
def index():
    if session.get('teacher_id'):
        return redirect(url_for('behaviour_bp.behaviour_dashboard'))
    return redirect(url_for('auth_bp.login'))

if __name__ == '__main__':
    app.run(debug=True, port=4000)
