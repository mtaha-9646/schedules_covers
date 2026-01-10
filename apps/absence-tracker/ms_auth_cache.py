import os
import re
import sys
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import msal
from msal.token_cache import SerializableTokenCache

# ---- Config ----
CLIENT_ID = "b0cd23c9-4f5f-4ce1-b844-c280a3adb962"
TENANT_ID = "585133c8-a29a-45c2-808a-17ad024eac14"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"

# IMPORTANT:
# Do NOT include "offline_access" here.
# MSAL automatically adds the reserved scopes: openid, profile, offline_access.
# If you include them yourself, MSAL raises:
#   "You cannot use any scope value that is reserved."
SCOPES = [
    "User.Read",
    "Mail.Send",
    "Files.ReadWrite",  # OneDrive / files
]

# Cache file for tokens (access + refresh)
CACHE_FILE = os.getenv(
    "GRAPH_CACHE_FILE",
    os.path.expanduser("~/.msal_cache_pythonexcel.json")
)
DEFAULT_PROFILE = "behaviour"

_device_flow_lock = threading.Lock()
_device_flows: Dict[str, dict] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalise_profile(profile: Optional[str]) -> str:
    value = (profile or DEFAULT_PROFILE or "").strip().lower()
    if not value:
        value = DEFAULT_PROFILE
    return re.sub(r"[^a-z0-9_-]", "_", value)


def _cache_file(profile: Optional[str] = None) -> str:
    profile_slug = _normalise_profile(profile)
    default_slug = _normalise_profile(DEFAULT_PROFILE)
    if profile_slug == default_slug:
        return CACHE_FILE
    root, ext = os.path.splitext(CACHE_FILE)
    suffix = f"_{profile_slug}"
    return f"{root}{suffix}{ext or '.json'}"


def _load_cache(profile: Optional[str] = None) -> SerializableTokenCache:
    path = _cache_file(profile)
    cache = SerializableTokenCache()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                cache.deserialize(f.read())
        except Exception:
            cache = SerializableTokenCache()
    return cache


def _save_cache(cache: SerializableTokenCache, profile: Optional[str] = None) -> None:
    if cache.has_state_changed:
        path = _cache_file(profile)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(cache.serialize())
        os.replace(tmp, path)


def _app(cache: Optional[SerializableTokenCache] = None) -> msal.PublicClientApplication:
    return msal.PublicClientApplication(
        CLIENT_ID, authority=AUTHORITY, token_cache=cache or _load_cache()
    )


# -----------------------------
# MAIN TOKEN ACCESS FUNCTION
# -----------------------------
def get_token_silent(profile: Optional[str] = None) -> str:
    """Return an access token using the cached refresh token (no UI)."""
    profile_slug = _normalise_profile(profile)
    cache = _load_cache(profile_slug)
    app = _app(cache)

    accounts = app.get_accounts()
    if not accounts:
        raise RuntimeError("No cached account found. Please sign in first.")

    # This will use the refresh token if needed
    result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if not result or "access_token" not in result:
        raise RuntimeError("Silent token refresh failed. Please reauthenticate.")

    # Persist updated tokens
    _save_cache(cache, profile_slug)
    return result["access_token"]


def _record_flow(device_code: str, payload: dict) -> None:
    with _device_flow_lock:
        _device_flows[device_code] = payload


def _update_flow(device_code: str, **updates) -> None:
    with _device_flow_lock:
        entry = _device_flows.get(device_code)
        if not entry:
            entry = {"device_code": device_code}
            _device_flows[device_code] = entry
        entry.update(updates)


# ---------------------------------------------------------
# START DEVICE LOGIN FLOW (used in your dashboard buttons)
# ---------------------------------------------------------
def start_device_flow(profile: Optional[str] = None) -> dict:
    """Start the device-code flow for a given profile (behaviour/absence/etc)."""
    profile_slug = _normalise_profile(profile)
    cache = _load_cache(profile_slug)
    app = _app(cache)

    # Device code flow: user goes to URL + enters code + does MFA once
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError("Failed to initiate device flow. Check app registration & permissions.")

    device_code = flow["device_code"]
    expires_in = flow.get("expires_in", 900)

    payload = {
        "device_code": device_code,
        "user_code": flow["user_code"],
        "verification_uri": flow.get("verification_uri") or flow.get("verification_uri_complete"),
        "expires_in": expires_in,
        "expires_at": (_utc_now() + timedelta(seconds=expires_in)).isoformat(),
        "interval": flow.get("interval", 5),
        "status": "pending",
        "created_at": _utc_now().isoformat(),
        "profile": profile_slug,
    }

    _record_flow(device_code, payload)

    # Background thread waits for user to complete sign-in in browser
    def worker():
        try:
            result = app.acquire_token_by_device_flow(flow)
            if "access_token" in result:
                # This includes refresh token; MSAL stores it in cache
                _save_cache(cache, profile_slug)
                username = (
                    result.get("id_token_claims", {}).get("preferred_username")
                    or result.get("account", {}).get("username")
                )
                _update_flow(
                    device_code,
                    status="success",
                    completed_at=_utc_now().isoformat(),
                    account=username,
                )
            else:
                _update_flow(
                    device_code,
                    status="error",
                    completed_at=_utc_now().isoformat(),
                    message=result.get("error_description") or "Device flow error",
                )
        except Exception as exc:
            _update_flow(
                device_code,
                status="error",
                completed_at=_utc_now().isoformat(),
                message=str(exc),
            )

    threading.Thread(target=worker, daemon=True).start()
    return payload.copy()


def get_device_flows(profile: Optional[str] = None) -> list:
    profile_slug = _normalise_profile(profile) if profile else None
    with _device_flow_lock:
        cutoff = _utc_now() - timedelta(minutes=30)
        stale = [
            code
            for code, info in _device_flows.items()
            if datetime.fromisoformat(info.get("expires_at") or info.get("created_at")) < cutoff
            and info.get("status") in {"error", "success"}
        ]
        for code in stale:
            _device_flows.pop(code, None)

        entries = [info.copy() for info in _device_flows.values()]
        if profile_slug:
            entries = [x for x in entries if x.get("profile") == profile_slug]
        return entries


def status(profile: Optional[str] = None) -> dict:
    profile_slug = _normalise_profile(profile)
    cache = _load_cache(profile_slug)
    app = _app(cache)
    accts = app.get_accounts()
    return {
        "profile": profile_slug,
        "cache_file": _cache_file(profile_slug),
        "has_cache_file": os.path.exists(_cache_file(profile_slug)),
        "accounts": [a.get("username") or a.get("home_account_id") for a in accts],
        "scopes": SCOPES,
        "checked_at": _utc_now().isoformat(),
    }


def token_ready(profile: Optional[str] = None) -> tuple[bool, Optional[str]]:
    """Used by your dashboard card to say 'Connected' / 'Sign in required'."""
    try:
        get_token_silent(profile)
        return True, None
    except Exception as exc:
        return False, str(exc)


def logout(profile: Optional[str] = None, clear_file: bool = True) -> None:
    """Clear cached tokens + in-memory device flows for a profile."""
    profile_slug = _normalise_profile(profile)
    if clear_file:
        path = _cache_file(profile_slug)
        if os.path.exists(path):
            os.remove(path)
    with _device_flow_lock:
        stale = [
            code for code, info in _device_flows.items()
            if info.get("profile") == profile_slug
        ]
        for code in stale:
            _device_flows.pop(code, None)


def _print_json(data):
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "help"
    profile_arg = sys.argv[2] if len(sys.argv) > 2 else None

    if cmd in ("signin", "login"):
        start = start_device_flow(profile=profile_arg)
        _print_json({
            "message": "Complete sign-in in your browser.",
            "verification_uri": start["verification_uri"],
            "user_code": start["user_code"],
            "expires_in": start["expires_in"],
            "profile": start.get("profile"),
        })

    elif cmd == "status":
        _print_json(status(profile=profile_arg))

    elif cmd in ("logout", "clear"):
        logout(profile=profile_arg, clear_file=True)
        print("Cache cleared:", _cache_file(profile_arg))

    else:
        print(
            "Usage:\n"
            "  python ms_auth_cache.py signin [profile]\n"
            "  python ms_auth_cache.py status [profile]\n"
            "  python ms_auth_cache.py logout [profile]\n"
        )
