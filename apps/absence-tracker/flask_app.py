import os
from flask import Flask, redirect, url_for, session, request
from extensions import db
from views import leave_bp
from shared.auth_lib import SuiteAuthMiddleware

app = Flask(__name__)
app.secret_key = os.getenv("PORTAL_SECRET_KEY", "absence-tracker-secret")

# Suite Auth Integration
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://control-plane:8000")
app.wsgi_app = SuiteAuthMiddleware(app.wsgi_app, CONTROL_PLANE_URL)

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

# Initialize extensions
db.init_app(app)

# Register blueprints
app.register_blueprint(leave_bp)

@app.route('/')
def index():
    return redirect(url_for('leave_bp.list_requests'))

if __name__ == '__main__':
    app.run(debug=True, port=4001)
