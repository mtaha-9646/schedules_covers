import os
import requests
from typing import Optional, Dict, Any
from jose import jwt, JWTError
from flask import request, jsonify, _request_ctx_stack
from functools import wraps

# Constants
JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-suite-key")
ALGORITHM = "HS256"

def verify_suite_token(token: str) -> Optional[Dict[str, Any]]:
    """Verifies the Suite JWT and returns the payload."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None

def auth_required(f):
    """Decorator to enforce Suite JWT authentication on Flask routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid authorization header"}), 401
        
        token = auth_header.split(" ")[1]
        payload = verify_suite_token(token)
        if not payload:
            return jsonify({"error": "Invalid or expired token"}), 401
        
        # Store user/tenant info in request context
        request.suite_user = payload
        return f(*args, **kwargs)
    return decorated

def get_current_tenant_id() -> Optional[str]:
    """Retrieves the tenant_id from the current request context."""
    if hasattr(request, 'suite_user'):
        return request.suite_user.get('tenant_id')
    return None

def has_permission(permission: str) -> bool:
    """Checks if the current user has a specific permission."""
    if hasattr(request, 'suite_user'):
        permissions = request.suite_user.get('permissions', [])
        return permission in permissions
    return False

class SuiteAuthMiddleware:
    """WSGI Middleware for Suite JWT authentication."""
    def __init__(self, app, control_plane_url: str):
        self.app = app
        self.control_plane_url = control_plane_url

    def __call__(self, environ, start_response):
        # Allow health checks and other public routes
        path = environ.get('PATH_INFO', '')
        if path in ['/health', '/public']:
            return self.app(environ, start_response)

        auth_header = environ.get('HTTP_AUTHORIZATION', '')
        token = None
        if auth_header.startswith('Bearer '):
            token = auth_header[7:]
        
        if not token:
            # Fallback to cookie check for browser apps
            from http.cookies import SimpleCookie
            cookie = SimpleCookie(environ.get('HTTP_COOKIE', ''))
            if 'suite_token' in cookie:
                token = cookie['suite_token'].value

        if token:
            payload = verify_suite_token(token)
            if payload:
                environ['suite_user'] = payload
                return self.app(environ, start_response)

        # If no token, redirect to Portal login for HTML requests
        if 'text/html' in environ.get('HTTP_ACCEPT', ''):
            login_url = "http://localhost:3000/login"
            start_response('302 Found', [('Location', login_url)])
            return [b'Redirecting to Portal Login...']
        
        start_response('401 Unauthorized', [('Content-Type', 'application/json')])
        return [b'{"error": "Unauthorized"}']
