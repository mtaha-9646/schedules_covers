import json
import requests

GRAPH = "https://graph.microsoft.com/v1.0"


def _get_token(profile=None):
    try:
        from ms_auth_cache import get_token_silent  # type: ignore
    except Exception:
        from .ms_auth_cache import get_token_silent  # type: ignore
    return get_token_silent(profile=profile)


def send_mail(
    to_email,
    subject: str,
    html_body: str,
    cc=None,
    bcc=None,
    save_to_sent=True,
    *,
    profile: str = "behaviour",
):
    token = _get_token(profile=profile)
    if isinstance(to_email, (list, tuple)):
        to_list = [str(a).strip() for a in to_email if a and str(a).strip()]
    else:
        to_list = [str(to_email).strip()] if to_email else []
    if not to_list:
        raise RuntimeError("sendMail: to_email is required")

    message = {
        "subject": subject,
        "body": {"contentType": "HTML", "content": html_body},
        "toRecipients": [{"emailAddress": {"address": a}} for a in to_list],
    }
    if cc:
        addrs = cc if isinstance(cc, (list, tuple)) else [cc]
        message["ccRecipients"] = [{"emailAddress": {"address": str(a).strip()}} for a in addrs if str(a).strip()]
    if bcc:
        addrs = bcc if isinstance(bcc, (list, tuple)) else [bcc]
        message["bccRecipients"] = [{"emailAddress": {"address": str(a).strip()}} for a in addrs if str(a).strip()]

    response = requests.post(
        f"{GRAPH}/me/sendMail",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps({"message": message, "saveToSentItems": bool(save_to_sent)}),
        timeout=30,
    )
    if response.status_code != 202:
        raise RuntimeError(f"sendMail failed: {response.status_code} {response.text}")
    return True
