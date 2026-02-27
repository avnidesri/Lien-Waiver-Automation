#!/usr/bin/env python3
"""
mail_send_oauth.py

Mail via Microsoft Graph with Azure AD client credentials (app-only), same as your other app.
Env: OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_TENANT_ID,
     SMTP_USER or OAUTH_USER_EMAIL (sender mailbox), ALERT_EMAIL_TO.

Send failure alerts: from mail_send_oauth import send_alert_email
                     send_alert_email([("Subject 1", "reason 1"), ...])
"""

import os
import requests
from urllib.parse import quote

OAUTH_CLIENT_ID = (os.getenv("OAUTH_CLIENT_ID") or "").strip()
OAUTH_CLIENT_SECRET = (os.getenv("OAUTH_CLIENT_SECRET") or "").strip()
OAUTH_TENANT_ID = (os.getenv("OAUTH_TENANT_ID") or "").strip()
SENDER_EMAIL = (os.getenv("SMTP_USER") or os.getenv("OAUTH_USER_EMAIL") or "avni.goyal@desri.com").strip()
ALERT_EMAIL_TO = (os.getenv("ALERT_EMAIL_TO") or "avni.goyal@desri.com").strip()

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE = "https://graph.microsoft.com/.default"


def _get_access_token():
    """Get access token via client_credentials (app-only)."""
    if not all([OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_TENANT_ID]):
        return None
    url = f"https://login.microsoftonline.com/{OAUTH_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "scope": SCOPE,
    }
    try:
        r = requests.post(url, data=data, timeout=30)
        r.raise_for_status()
        return r.json().get("access_token")
    except requests.RequestException as e:
        print("OAuth token failed:", e)
        resp = getattr(e, "response", None)
        if resp is not None and getattr(resp, "text", None):
            print("Response:", resp.text[:500])
        return None


def send_alert_email(failed_list):
    """
    Send an email with a table of failed mails (subject, reason) to ALERT_EMAIL_TO via Graph.
    failed_list: list of (subject, reason) tuples.
    Returns True if sent, False otherwise.
    """
    if not failed_list:
        return True
    token = _get_access_token()
    if not token:
        return False
    rows = "".join(
        f"<tr><td>{_html_esc(s)}</td><td>{_html_esc(r)}</td></tr>"
        for s, r in failed_list
    )
    html = f"""<p>Lien Waiver run had {len(failed_list)} failed message(s).</p>
<table border="1" cellpadding="4" cellspacing="0">
<thead><tr><th>Subject</th><th>Failed reason</th></tr></thead>
<tbody>{rows}</tbody>
</table>"""
    payload = {
        "message": {
            "subject": f"Lien Waiver run: {len(failed_list)} failed",
            "body": {"contentType": "HTML", "content": html},
            "toRecipients": [{"emailAddress": {"address": ALERT_EMAIL_TO}}],
        }
    }
    user_path = quote(SENDER_EMAIL, safe="")
    url = f"{GRAPH_BASE}/users/{user_path}/sendMail"
    try:
        r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=30)
        if r.status_code in (200, 202):
            return True
        print("Graph sendMail failed:", r.status_code, r.text[:300])
        return False
    except requests.RequestException as e:
        print("Graph sendMail error:", e)
        return False


def _html_esc(s):
    if s is None:
        return ""
    s = str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    return s


def main():
    """Check config; optionally send test email if TEST_SEND=1."""
    missing = []
    if not OAUTH_CLIENT_ID: missing.append("OAUTH_CLIENT_ID")
    if not OAUTH_CLIENT_SECRET: missing.append("OAUTH_CLIENT_SECRET")
    if not OAUTH_TENANT_ID: missing.append("OAUTH_TENANT_ID")
    if not SENDER_EMAIL: missing.append("SMTP_USER or OAUTH_USER_EMAIL")
    if missing:
        print("Add to .env:", ", ".join(missing))
        return
    print("OAuth config OK. From:", SENDER_EMAIL, "To:", ALERT_EMAIL_TO)
    if os.getenv("TEST_SEND"):
        ok = send_alert_email([("Test subject", "Test reason")])
        print("Test email sent." if ok else "Test send failed.")
    else:
        print("Run with TEST_SEND=1 to send a test email.")


if __name__ == "__main__":
    main()
