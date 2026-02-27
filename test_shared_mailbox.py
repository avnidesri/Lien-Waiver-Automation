#!/usr/bin/env python3
"""
test_shared_mailbox.py

Minimal script to test if the app can read the shared mailbox inbox
(lien-waivers@desri.com) via Microsoft Graph. No Egnyte, no processing.

Supports:
  - Certificate auth: set AZURE_PRIVATE_KEY_PATH to your .pem (from .pfx).
    Thumbprint is computed from the certificate in the .pem (no need from Don).
  - Client secret auth: set AZURE_CLIENT_SECRET (legacy).

Requires in .env: AZURE_TENANT_ID, AZURE_CLIENT_ID, and either
  AZURE_PRIVATE_KEY_PATH (certificate) or AZURE_CLIENT_SECRET.
Optional: LIEN_WAIVERS_MAILBOX, LIEN_WAIVERS_MAILBOX_OBJECT_ID.

Usage: python test_shared_mailbox.py
"""

import os
from urllib.parse import quote
import requests
from dotenv import load_dotenv
import base64, json

load_dotenv()

AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_PRIVATE_KEY_PATH = (os.getenv("AZURE_PRIVATE_KEY_PATH") or "").strip() or None
AZURE_CERT_THUMBPRINT = (os.getenv("AZURE_CERT_THUMBPRINT") or "").strip() or None
GRAPH_MAILBOX = os.getenv("LIEN_WAIVERS_MAILBOX", "lien-waivers@desri.com")
GRAPH_MAILBOX_OBJECT_ID = os.getenv("LIEN_WAIVERS_MAILBOX_OBJECT_ID", "").strip() or None
GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def _get_certificate_credential():
    """Load private key from PEM and compute thumbprint from cert in same file. Returns (thumbprint, private_key_pem) or (None, None)."""
    if not AZURE_PRIVATE_KEY_PATH:
        return None, None
    path = AZURE_PRIVATE_KEY_PATH
    if os.path.islink(path):
        path = os.path.realpath(path)
    if not os.path.isabs(path):
        # Resolve relative to script dir, then cwd
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path_in_script = os.path.abspath(os.path.join(script_dir, path))
        path_in_cwd = os.path.abspath(path)
        path = path_in_script if os.path.isfile(path_in_script) else path_in_cwd
    if not os.path.isfile(path):
        print(f"ERROR: AZURE_PRIVATE_KEY_PATH file not found: {path}")
        return None, None
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption

        data = open(path, "rb").read()
        # Load first private key in PEM
        key = load_pem_private_key(data, password=None)
        private_key_pem = key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=NoEncryption(),
        ).decode("utf-8")

        # Thumbprint: use env if set, else compute from first certificate in PEM
        thumbprint = AZURE_CERT_THUMBPRINT
        if not thumbprint:
            try:
                cert = x509.load_pem_x509_certificate(data)
                # Azure uses SHA-1 thumbprint, hex uppercase, no separators
                thumbprint = cert.fingerprint(hashes.SHA1()).hex().upper()
            except Exception:
                print("ERROR: No certificate in PEM file and AZURE_CERT_THUMBPRINT not set. Set one or the other.")
                return None, None

        return thumbprint, private_key_pem
    except Exception as e:
        print(f"ERROR: Failed to load certificate/private key: {e}")
        return None, None


def get_token():
    if not AZURE_TENANT_ID or not AZURE_CLIENT_ID:
        print("ERROR: Set AZURE_TENANT_ID and AZURE_CLIENT_ID in .env")
        return None

    # Prefer certificate auth if private key path is set
    thumbprint, private_key_pem = _get_certificate_credential()
    if thumbprint and private_key_pem:
        try:
            import msal
            authority = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
            app = msal.ConfidentialClientApplication(
                AZURE_CLIENT_ID,
                authority=authority,
                client_credential={
                    "thumbprint": thumbprint,
                    "private_key": private_key_pem,
                },
            )
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            if result and "access_token" in result:
                return result["access_token"]
            err = result.get("error_description") or result.get("error", "Unknown error")
            print(f"ERROR: Certificate auth failed: {err}")
            return None
        except ImportError:
            print("ERROR: pip install msal (required for certificate auth)")
            return None
        except Exception as e:
            print(f"ERROR: Certificate auth failed: {e}")
            return None

    # Fallback: client secret
    if not AZURE_CLIENT_SECRET:
        print("ERROR: Set AZURE_PRIVATE_KEY_PATH (certificate) or AZURE_CLIENT_SECRET in .env")
        return None
    url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    }
    try:
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        return resp.json().get("access_token")
    except requests.RequestException as e:
        print(f"ERROR: Failed to get token: {e}")
        return None


def test_inbox():
    token = get_token()
    if not token:
        return

    user_path = quote(GRAPH_MAILBOX, safe="")
    url = f"{GRAPH_BASE}/users/lien-waivers@desri.com/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {"$top": 5, "$select": "id,subject,receivedDateTime,from"}

    print(f"Testing shared mailbox: {GRAPH_MAILBOX}")
    print(f"Request: GET {url}")
    print()

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            messages = data.get("value", [])
            print("SUCCESS: Shared mailbox inbox is accessible.")
            print(f"Messages in inbox (showing up to 5): {len(messages)}")
            for i, m in enumerate(messages, 1):
                subj = (m.get("subject") or "(no subject)")[:60]
                from_info = m.get("from", {}) or {}
                addr = (from_info.get("emailAddress") or {}).get("address", "?")
                print(f"  {i}. {subj} ... (from {addr})")
            return
        if resp.status_code == 404:
            print("FAIL: 404 Mailbox not found.")
            print("  Ensure the shared mailbox exists and Exchange Application Access Policy")
            print(f"  allows this app to access '{GRAPH_MAILBOX}'.")
            if not GRAPH_MAILBOX_OBJECT_ID:
                print("  You can try setting LIEN_WAIVERS_MAILBOX_OBJECT_ID to the mailbox Object ID.")
            return
        if resp.status_code == 401:
            print("FAIL: 401 Unauthorized. Check Azure app credentials and Mail.Read permission.")
            return
        if resp.status_code == 403:
            print("FAIL: 403 Forbidden. App may lack Mail.Read or access to this mailbox.")
            try:
                err = resp.json()
                print("Graph error:", err.get("error", {}).get("message") or err.get("error", {}).get("code") or resp.text[:300])
            except Exception:
                print("Response body:", resp.text[:500])
            return      
        print(f"FAIL: {resp.status_code} {resp.reason}")
        print(resp.text[:500] if resp.text else "")
    except requests.RequestException as e:
        print(f"ERROR: Request failed: {e}")


if __name__ == "__main__":
    test_inbox()
