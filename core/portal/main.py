import os
from flask import Flask, render_template, redirect, url_for, session, request, jsonify
import requests
from jose import jwt

app = Flask(__name__)
app.secret_key = os.getenv("PORTAL_SECRET_KEY", "portal-dev-key")

# Configuration
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://control-plane:8000")
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
REALM = "saas-suite"
CLIENT_ID = "suite-portal"
CALLBACK_URL = "http://localhost:3000/callback"

@app.route("/")
def index():
    if "suite_token" not in session:
        return render_template("login.html")
    
    # In a real app, fetch apps and tenant info from Control Plane
    user_info = jwt.get_unverified_claims(session["suite_token"])
    return render_template("dashboard.html", user=user_info)

@app.route("/login")
def login():
    # Redirect to Keycloak
    login_url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/auth"
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": "openid email profile",
        "redirect_uri": CALLBACK_URL
    }
    target = f"{login_url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}"
    return redirect(target)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "Login failed", 400
    
    # Exchange code for ID Token with Keycloak
    token_url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
    res = requests.post(token_url, data={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "code": code,
        "redirect_uri": CALLBACK_URL
    })
    
    if res.status_code != 200:
        return f"Keycloak error: {res.text}", 500
    
    id_token = res.json().get("id_token")
    
    # Exchange ID Token for Suite JWT with Control Plane
    exchange_res = requests.post(f"{CONTROL_PLANE_URL}/auth/exchange", json={"id_token": id_token})
    if exchange_res.status_code != 200:
        return f"Control Plane error: {exchange_res.text}", 500
    
    suite_token = exchange_res.json().get("access_token")
    session["suite_token"] = suite_token
    
    return redirect(url_for("index"))

@app.route("/admin")
def admin():
    if "suite_token" not in session:
        return redirect(url_for("index"))
    
    user_info = jwt.get_unverified_claims(session["suite_token"])
    if "platform_super_admin" not in user_info.get("roles", []):
        return "Access Forbidden", 403
    
    return render_template("admin.html", user=user_info)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True, port=3000, host="0.0.0.0")
