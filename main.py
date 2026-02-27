#!/usr/bin/env python3
"""
outlook_lien_monitor.py

Reads mail from shared mailbox lien-waivers@desri.com via Microsoft Graph API,
matches project names against authoritative project folders from Egnyte API,
and uploads PDF attachments to Egnyte (EGNYTE_LIEN_WAVERS_ROOT).

Project list: Egnyte API (EGNYTE_PROJECTS_PATH, e.g. /Shared/Projects/Portfolio).
Set in environment (e.g. Render cron job env vars): OPENAI_API_KEY, EGNYTE_*, AZURE_*, OAUTH_* for alert email; AZURE_PRIVATE_KEY_PATH (PEM) or AZURE_CLIENT_SECRET for Graph auth.

Dependencies: pip install openai requests cryptography msal
"""

import base64
import json
import os
import re
import logging
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote
import requests
from openai import OpenAI

_SCRIPT_DIR = Path(__file__).resolve().parent
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EGNYTE_DOMAIN = os.getenv("EGNYTE_DOMAIN")          # e.g. "yourcompany" (no .egnyte.com)
EGNYTE_TOKEN = os.getenv("EGNYTE_ACCESS_TOKEN")
EGNYTE_PROJECTS_PATH = os.getenv("EGNYTE_PROJECTS_PATH", "/Shared/Projects/Portfolio")

# Microsoft Graph (shared mailbox lien-waivers@desri.com)
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_PRIVATE_KEY_PATH = (os.getenv("AZURE_PRIVATE_KEY_PATH") or "").strip() or None
AZURE_CERT_THUMBPRINT = (os.getenv("AZURE_CERT_THUMBPRINT") or "").strip() or None
GRAPH_MAILBOX = os.getenv("LIEN_WAIVERS_MAILBOX", "lien-waivers@desri.com")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ---------------- CONFIG ----------------
EGNYTE_LIEN_WAVERS_ROOT = (os.getenv("EGNYTE_LIEN_WAVERS_ROOT") or "").strip() or "/Shared/Company/Functions/Accounting/3. Financial Controlling/3. Accounts Payable/3. Payments/Bank Draws/201. Lien Waivers"
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "1"))       # process emails from past N hours (default 1 for hourly cron)
# WATCH_FOLDER_PATH: default attempt is ['Inbox', 'Lien Waivers']
# If the folder is under a shared mailbox, resolver will search automatically.
WATCH_FOLDER_PATH = ["Inbox", "Lien Wavers"]
LOG_FILE = _SCRIPT_DIR / "outlook_lien_monitor.log"
# ----------------------------------------

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(LOG_FILE), encoding="utf-8"), logging.StreamHandler()]
)

# ---------------- Helpers ----------------

def safe_name(text):
    if not text:
        return "Unknown"
    text = str(text).strip()
    # Remove illegal characters for Windows file/folder names
    text = re.sub(r'[\\/:*?"<>|]+', "", text)
    text = re.sub(r'\s+', " ", text)
    return text

def strip_project_words_from_counterparty(matched_project_folder_name, counterparty_str):
    """
    Remove leading words from counterparty that are part of the project folder name.
    E.g. project "052 White Tail", counterparty "Tail Primoris" -> "Primoris".
    E.g. project "075 Show Me State", counterparty "Me State SOLV" -> "SOLV".
    """
    if not matched_project_folder_name or not counterparty_str:
        return counterparty_str
    proj_words = matched_project_folder_name.split()
    cparty_words = counterparty_str.split()
    if not proj_words or not cparty_words:
        return counterparty_str
    # Strip longest suffix of project name that matches a prefix of counterparty (case-insensitive)
    for suffix_len in range(min(len(proj_words), len(cparty_words)), 0, -1):
        suffix = [w.lower() for w in proj_words[-suffix_len:]]
        prefix = [w.lower() for w in cparty_words[:suffix_len]]
        if prefix == suffix:
            return " ".join(cparty_words[suffix_len:]).strip()
    return counterparty_str

def timestamped_filename(original):
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    stem = Path(original).stem
    ext = Path(original).suffix or ".pdf"
    # sanitize stem
    stem = re.sub(r'[^A-Za-z0-9 _\-\.]', '_', stem)[:80]
    return f"{now}__{stem}{ext}"

def list_project_folders():
    """Return list of subfolder names under EGNYTE_PROJECTS_PATH via the Egnyte API."""
    if not EGNYTE_DOMAIN or not EGNYTE_TOKEN:
        logging.error("EGNYTE_DOMAIN or EGNYTE_ACCESS_TOKEN not set in .env; cannot list project folders.")
        return []
    url = f"https://{EGNYTE_DOMAIN}.egnyte.com/pubapi/v1/fs{EGNYTE_PROJECTS_PATH}"
    headers = {"Authorization": f"Bearer {EGNYTE_TOKEN}"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        folders = [f["name"] for f in data.get("folders", [])]
        logging.info(f"Loaded {len(folders)} project folders from Egnyte: {EGNYTE_PROJECTS_PATH}")
        return sorted(folders)
    except requests.HTTPError as e:
        logging.exception(f"Egnyte API error listing project folders ({resp.status_code}): {e}")
        return []
    except Exception as e:
        logging.exception(f"Failed to list project folders from Egnyte: {e}")
        return []


def _egnyte_encode_path(path):
    """Encode each path segment; slashes stay (per Egnyte docs)."""
    path = (path or "").strip().strip("/")
    if not path:
        return ""
    return "/".join(quote(segment, safe="") for segment in path.split("/"))


def _egnyte_ensure_folder(folder_path):
    """Create folder and parents in Egnyte. 403 = already exists."""
    if not folder_path or not EGNYTE_DOMAIN or not EGNYTE_TOKEN:
        return False
    parts = [p for p in folder_path.strip("/").split("/") if p]
    for i in range(1, len(parts) + 1):
        sub = "/" + "/".join(parts[:i])
        enc = _egnyte_encode_path(sub)
        url = f"https://{EGNYTE_DOMAIN}.egnyte.com/pubapi/v1/fs/{enc}"
        try:
            r = requests.post(url, headers={"Authorization": f"Bearer {EGNYTE_TOKEN}"}, json={"action": "add_folder"}, timeout=30)
            if r.status_code in (200, 201) or r.status_code == 403:
                continue
            logging.warning("Egnyte create folder %s: %s %s", sub, r.status_code, r.text[:200])
            return False
        except requests.RequestException as e:
            logging.exception("Egnyte create folder failed: %s", e)
            return False
    return True


def _egnyte_upload_file(full_path, content_bytes):
    """Upload file to Egnyte. Returns True on success."""
    if not full_path or not EGNYTE_DOMAIN or not EGNYTE_TOKEN:
        return False
    enc = _egnyte_encode_path(full_path)
    url = f"https://{EGNYTE_DOMAIN}.egnyte.com/pubapi/v1/fs-content/{enc}"
    try:
        r = requests.post(url, headers={"Authorization": f"Bearer {EGNYTE_TOKEN}"}, data=content_bytes, timeout=60)
        if r.status_code in (200, 201):
            return True
        logging.warning("Egnyte upload %s: %s %s", full_path, r.status_code, r.text[:200])
        return False
    except requests.RequestException as e:
        logging.exception("Egnyte upload failed: %s", e)
        return False


# ---------------- Microsoft Graph (lien-waivers@desri.com) ----------------

def _get_certificate_credential():
    """Load private key from PEM and compute thumbprint from cert in same file. Returns (thumbprint, private_key_pem) or (None, None)."""
    if not AZURE_PRIVATE_KEY_PATH:
        return None, None
    path = AZURE_PRIVATE_KEY_PATH
    if os.path.islink(path):
        path = os.path.realpath(path)
    if not os.path.isabs(path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path_in_script = os.path.abspath(os.path.join(script_dir, path))
        path_in_cwd = os.path.abspath(path)
        path = path_in_script if os.path.isfile(path_in_script) else path_in_cwd
    if not os.path.isfile(path):
        logging.error("AZURE_PRIVATE_KEY_PATH file not found: %s", path)
        return None, None
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption

        data = open(path, "rb").read()
        key = load_pem_private_key(data, password=None)
        private_key_pem = key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=NoEncryption(),
        ).decode("utf-8")

        thumbprint = AZURE_CERT_THUMBPRINT
        if not thumbprint:
            try:
                cert = x509.load_pem_x509_certificate(data)
                thumbprint = cert.fingerprint(hashes.SHA1()).hex().upper()
            except Exception:
                logging.error("No certificate in PEM and AZURE_CERT_THUMBPRINT not set.")
                return None, None

        return thumbprint, private_key_pem
    except Exception as e:
        logging.exception("Failed to load certificate/private key: %s", e)
        return None, None


def get_graph_token():
    """Get access token for Microsoft Graph (certificate or client secret)."""
    if not AZURE_TENANT_ID or not AZURE_CLIENT_ID:
        logging.error("AZURE_TENANT_ID and AZURE_CLIENT_ID must be set in .env")
        return None

    thumbprint, private_key_pem = _get_certificate_credential()
    if thumbprint and private_key_pem:
        try:
            import msal
            authority = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
            app = msal.ConfidentialClientApplication(
                AZURE_CLIENT_ID,
                authority=authority,
                client_credential={"thumbprint": thumbprint, "private_key": private_key_pem},
            )
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            if result and "access_token" in result:
                return result["access_token"]
            logging.error("Certificate auth failed: %s", result.get("error_description") or result.get("error"))
            return None
        except ImportError:
            logging.error("pip install msal (required for certificate auth)")
            return None
        except Exception as e:
            logging.exception("Certificate auth failed: %s", e)
            return None

    if not AZURE_CLIENT_SECRET:
        logging.error("Set AZURE_PRIVATE_KEY_PATH (certificate) or AZURE_CLIENT_SECRET in .env")
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
        logging.exception("Failed to get Graph token: %s", e)
        return None


def _graph_headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _graph_user_path():
    """URL path segment for the mailbox (encoded email)."""
    return quote(GRAPH_MAILBOX, safe="")


def fetch_inbox_messages(token, received_after):
    """Fetch messages from shared mailbox (default view) received after given datetime (UTC)."""
    user_path = _graph_user_path()
    received_utc = received_after.replace(microsecond=0)
    after_str = received_utc.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    url = f"{GRAPH_BASE}/users/{user_path}/messages"
    params = {
        "$filter": f"receivedDateTime ge {after_str}",
        "$orderby": "receivedDateTime desc",
        "$top": 100,
        "$select": "id,subject,receivedDateTime,from,hasAttachments",
    }
    try:
        resp = requests.get(url, headers=_graph_headers(token), params=params, timeout=30)
        if resp.status_code == 404:
            logging.error(
                "Mailbox not found (404). For app-only access, ensure Exchange Application Access "
                "Policy allows this app to access '%s'.",
                GRAPH_MAILBOX,
            )
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("value", [])
    except requests.RequestException as e:
        logging.exception(f"Failed to fetch inbox messages: {e}")
        return []


def fetch_attachments_with_content(token, message_id):
    """Fetch attachments for a message; for file attachments, include content (base64). Returns list of {name, content_bytes}."""
    user_path = _graph_user_path()
    list_url = f"{GRAPH_BASE}/users/{user_path}/messages/{message_id}/attachments"
    try:
        resp = requests.get(list_url, headers=_graph_headers(token), timeout=30)
        resp.raise_for_status()
        items = resp.json().get("value", [])
    except requests.RequestException as e:
        logging.exception(f"Failed to list attachments for message {message_id}: {e}")
        return []
    result = []
    for att in items:
        if att.get("@odata.type") != "#microsoft.graph.fileAttachment":
            continue
        name = (att.get("name") or "").strip()
        if not name or not name.lower().endswith(".pdf"):
            continue
        # Graph may include contentBytes in list response for small attachments; otherwise get by id
        content_b64 = att.get("contentBytes")
        if not content_b64:
            get_url = f"{GRAPH_BASE}/users/{user_path}/messages/{message_id}/attachments/{att['id']}"
            try:
                r2 = requests.get(get_url, headers=_graph_headers(token), timeout=30)
                r2.raise_for_status()
                content_b64 = r2.json().get("contentBytes")
            except requests.RequestException as e:
                logging.warning(f"Failed to get attachment {att.get('id')}: {e}")
                continue
        if not content_b64:
            continue
        try:
            result.append({"name": name, "content": base64.b64decode(content_b64)})
        except Exception as e:
            logging.warning(f"Failed to decode attachment {name}: {e}")
    return result


class _GraphAttachmentAdapter:
    """Adapter so process_mail_item can call .FileName and .SaveAsFile(path)."""
    def __init__(self, name, content_bytes):
        self.FileName = name
        self._content = content_bytes
    def SaveAsFile(self, path):
        with open(path, "wb") as f:
            f.write(self._content)


class _GraphMessageAdapter:
    """Adapter so process_mail_item works with Graph message + attachments."""
    def __init__(self, subject, sender, attachments_list):
        self.Subject = subject or ""
        self.SenderName = sender or ""
        self.Sender = self.SenderName
        self._attachments = attachments_list  # list of _GraphAttachmentAdapter
    @property
    def Attachments(self):
        class _Attachments:
            def __init__(self, lst):
                self._list = lst
            @property
            def Count(self):
                return len(self._list)
            def Item(self, one_based_index):
                return self._list[one_based_index - 1]
        return _Attachments(self._attachments)


def parse_project_and_counterparty_with_ai(cleaned_subject, project_folders):
    """
    Single OpenAI call: given cleaned subject text and list of project folder names,
    return the best matching project folder name and the counterparty (entity name).
    The model infers that words not belonging to the project name are the counterparty.
    Returns: dict with project, counterparty, asset_type, match_reason, counterparty_reason.
    """
    if not cleaned_subject or not project_folders:
        return {"project": None, "counterparty": None, "asset_type": None, "match_reason": None, "counterparty_reason": None}
    if not OPENAI_API_KEY:
        logging.warning("OPENAI_API_KEY not set in .env; cannot parse project/counterparty.")
        return {"project": None, "counterparty": None, "asset_type": None, "match_reason": "No API key.", "counterparty_reason": None}

    folder_list = "\n".join(f"- {f}" for f in project_folders)
    prompt = f"""You are parsing a lien waiver email subject to extract:
1) The project: must be exactly one of the project folder names from the list below.
   The subject may contain the full project name OR an abbreviation/short code (e.g. RE2 for Red Horse 2, FW for Freshwater, BE2 for Blue Elk II). Match either to the exact project folder name from the list. Use the list to infer what any code means (e.g. RE2 + list has "Red Horse 2" -> match that). Return the exact folder name from the list.
2) The counterparty: the company or entity name (e.g. contractor, vendor). Words that do NOT belong to the matched project name are the counterparty. Do NOT treat PV or BESS as counterparty — they are asset types. Acronyms and short tokens (e.g. EPCS, SOLV, ABC) are valid as counterparty when they are the only remaining part of the subject after the project name; treat them as the entity name.
3) Asset type: if the subject clearly mentions "PV" or "BESS" as a distinct token, return that; otherwise null.
4) When project is null: set match_reason to a brief explanation of why no folder matched (e.g. "Subject 'XYZ' does not correspond to any project in the list" or "No folder contains or abbreviates to the given name"). Otherwise set match_reason to null.
5) When counterparty is null: set counterparty_reason to a brief explanation (e.g. "Could not identify entity name in subject"). Otherwise set counterparty_reason to null.

Subject (type keywords like UCLW/CLW already removed):
"{cleaned_subject}"

List of valid project folder names (return one of these exactly, or null if no match):
{folder_list}

Respond with ONLY a single JSON object, no other text, in this exact format:
{{"project": "<exact folder name from list or null>", "counterparty": "<entity name or null>", "asset_type": "<PV or BESS or null>", "match_reason": "<reason when project is null, else null>", "counterparty_reason": "<reason when counterparty is null, else null>"}}"""

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-5",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = (response.choices[0].message.content or "").strip()
        # Strip markdown code block if present
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = raw.rstrip("`").strip()
        data = json.loads(raw)
        project = data.get("project")
        counterparty = data.get("counterparty")
        asset_type = data.get("asset_type")
        match_reason = data.get("match_reason")
        counterparty_reason = data.get("counterparty_reason")
        if project is None or project == "" or str(project).upper() == "NONE":
            project = None
            if not match_reason:
                match_reason = "No project matched."
        else:
            project = str(project).strip()
            if project not in project_folders:
                for f in project_folders:
                    if f.lower() == project.lower():
                        project = f
                        break
                else:
                    logging.warning(f"OpenAI returned project '{project}' not in list; treating as no match.")
                    match_reason = match_reason or f"Returned project '{project}' not in folder list."
                    project = None
        if counterparty is None or str(counterparty).strip() == "" or str(counterparty).upper() == "NONE":
            # Fallback: use remaining token(s) after project as counterparty (e.g. "Catclaw EPCS" -> EPCS)
            remainder = (cleaned_subject or "").strip()
            if project:
                for w in project.split():
                    remainder = re.sub(re.escape(w), "", remainder, count=1, flags=re.IGNORECASE).strip()
            remainder = re.sub(r"\s+", " ", remainder).strip()
            if remainder:
                counterparty = remainder
                counterparty_reason = None
            else:
                counterparty = None
                if not counterparty_reason:
                    counterparty_reason = "Could not identify counterparty."
        else:
            counterparty = str(counterparty).strip()
        if asset_type not in ("PV", "BESS"):
            asset_type = None
        match_reason = str(match_reason).strip() if match_reason else None
        counterparty_reason = str(counterparty_reason).strip() if counterparty_reason else None
        return {"project": project, "counterparty": counterparty, "asset_type": asset_type, "match_reason": match_reason, "counterparty_reason": counterparty_reason}
    except json.JSONDecodeError as e:
        logging.exception(f"OpenAI returned invalid JSON: {e}")
        return {"project": None, "counterparty": None, "asset_type": None, "match_reason": "Invalid JSON from API.", "counterparty_reason": None}
    except Exception as e:
        logging.exception(f"OpenAI project/counterparty parsing failed: {e}")
        return {"project": None, "counterparty": None, "asset_type": None, "match_reason": str(e), "counterparty_reason": None}


def parse_subject_flexible(subject, project_folders=None):
    """
    Parse subject line with spaces as separators.
    Format: "<Project Name> <Counterparty> <Type>"

    Supported type formats (case-insensitive):
      Unconditional: "UCLW", "Unconditional Lien Waiver", "Unconditional"
      Conditional:   "CLW",  "Conditional Lien Waiver",   "Conditional"

    If subject contains both unconditional AND conditional indicators,
    type is set to "both" -> save PDF to both UCLW and CLW folders.
    """
    subj = (subject or "").strip()
    result = {"project": None, "counterparty": None, "type": None}
    if not subj:
        return result

    # 1) Detect type - check all possible formats (case-insensitive)
    #    Unconditional: "UCLW" or "unconditional lien waiver" or "unconditional"
    #    Conditional:   "CLW"  or "conditional lien waiver"   or "conditional"
    #    Note: "UCLW" contains "CLW" and "unconditional" contains "conditional"
    #    so we check the longer/more specific forms first

    has_unconditional = bool(re.search(r'\bUCLW\b', subj, re.IGNORECASE)) or \
                        bool(re.search(r'\bunconditional\b', subj, re.IGNORECASE))

    # For conditional: strip out unconditional/UCLW first, then check what remains
    stripped_for_cond = re.sub(r'\bUCLW\b', '', subj, flags=re.IGNORECASE)
    stripped_for_cond = re.sub(r'\bunconditional\b', '', stripped_for_cond, flags=re.IGNORECASE)
    has_conditional = bool(re.search(r'\bCLW\b', stripped_for_cond, re.IGNORECASE)) or \
                      bool(re.search(r'\bconditional\b', stripped_for_cond, re.IGNORECASE))

    if has_unconditional and has_conditional:
        result["type"] = "both"
    elif has_unconditional:
        result["type"] = "UCLW"
    elif has_conditional:
        result["type"] = "CLW"

    # 2) Strip all type-related keywords to isolate project + counterparty
    cleaned = re.sub(r'\bUCLW\b', '', subj, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCLW\b', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'(?i)\bunconditional\b', '', cleaned)
    cleaned = re.sub(r'(?i)\bconditional\b', '', cleaned)
    cleaned = re.sub(r'(?i)\blien\s*waiver[s]?\b', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # 3) Project + counterparty (+ asset_type) from single AI call when possible
    result["match_reason"] = None
    result["counterparty_reason"] = None
    if project_folders and cleaned and OPENAI_API_KEY:
        ai_result = parse_project_and_counterparty_with_ai(cleaned, project_folders)
        result["project"] = ai_result.get("project")
        result["counterparty"] = ai_result.get("counterparty")
        result["asset_type"] = ai_result.get("asset_type")
        result["match_reason"] = ai_result.get("match_reason")
        result["counterparty_reason"] = ai_result.get("counterparty_reason")
    else:
        # Fallback: no API key or no project list — simple split
        result["asset_type"] = None
        tokens = cleaned.split()
        if len(tokens) >= 2:
            result["project"] = " ".join(tokens[:-1])
            result["counterparty"] = tokens[-1]
        elif len(tokens) == 1:
            result["project"] = tokens[0]

    # sanitize (only project/counterparty for file paths; leave reason strings as-is)
    for k in result:
        if result[k] and k not in ("type", "asset_type", "match_reason", "counterparty_reason"):
            result[k] = safe_name(result[k])

    return result

# ---------------- Processing ----------------

def process_mail_item(msg, project_folders):
    subj = str(getattr(msg, "Subject", "") or "")
    sender = getattr(msg, "SenderName", "") or getattr(msg, "Sender", "")
    logging.info(f"Processing email. Subject: '{subj}' From: '{sender}'")

    parsed = parse_subject_flexible(subj, project_folders)
    # Project is already the matched folder name from AI (or None)
    project_folder_name = parsed.get("project")
    counterparty_guess = parsed.get("counterparty")
    match_reason = parsed.get("match_reason")
    counterparty_reason = parsed.get("counterparty_reason")

    # Fail if project or counterparty missing — do not save to _UNMATCHED or with UNKNOWN
    if not project_folder_name:
        reason = match_reason or "No project matched."
        logging.warning(f"FAILED (no project): {reason}")
        return False, f"failed:no_project — {reason}"
    if not counterparty_guess or counterparty_guess.upper() == "UNKNOWN":
        reason = counterparty_reason or "Could not identify counterparty."
        logging.warning(f"FAILED (no counterparty): {reason}")
        return False, f"failed:no_counterparty — {reason}"

    project_folder_name = safe_name(project_folder_name)
    counterparty_guess = safe_name(counterparty_guess)
    # Safety net: strip any leading words in counterparty that are part of the project name
    counterparty_guess = strip_project_words_from_counterparty(project_folder_name, counterparty_guess) or "UNKNOWN"
    if counterparty_guess.upper() == "UNKNOWN":
        reason = counterparty_reason or "Counterparty empty after stripping project words."
        logging.warning(f"FAILED (no counterparty): {reason}")
        return False, f"failed:no_counterparty — {reason}"

    type_guess = parsed.get("type") or "Unknown"
    asset_type = parsed.get("asset_type")  # PV, BESS, or None

    # Determine which type folders to save into
    # "both" means save to both UCLW and CLW folders
    if type_guess.lower() == "both":
        type_list = ["UCLW", "CLW"]
    else:
        type_list = [type_guess]

    # Build Egnyte folder path: root / Project / [PV|BESS] / Counterparty / Type
    root = EGNYTE_LIEN_WAVERS_ROOT.strip().rstrip("/")
    final_folders = []
    for t in type_list:
        if asset_type:
            folder = f"{root}/{project_folder_name}/{asset_type}/{counterparty_guess}/{t}"
        else:
            folder = f"{root}/{project_folder_name}/{counterparty_guess}/{t}"
        if not _egnyte_ensure_folder(folder):
            return False, f"egnyte_create_folder_failed:{folder}"
        final_folders.append(folder)

    attachments = getattr(msg, "Attachments", None)
    if not attachments or attachments.Count == 0:
        logging.info("No attachments found; skipping message.")
        return False, "no_attachments"

    saved_any = False
    saved_paths = []
    for i in range(1, attachments.Count + 1):
        att = attachments.Item(i)
        name = att.FileName
        if not name or not name.lower().endswith(".pdf"):
            if name:
                logging.info("Skipping non-PDF attachment: %s", name)
            continue
        final_name = timestamped_filename(name)
        content = getattr(att, "_content", None)
        if not content:
            continue
        for folder in final_folders:
            full_path = f"{folder}/{final_name}"
            if not _egnyte_upload_file(full_path, content):
                return False, f"egnyte_upload_failed:{full_path}"
            saved_any = True
            saved_paths.append(full_path)
            logging.info("Uploaded to Egnyte: %s", full_path)

    if not saved_any:
        logging.info("No PDF attachments were saved.")
        return False, "no_pdf_attachments"

    return True, saved_paths

# ---------------- Main loop ----------------

def main():
    logging.info("Starting Lien Waiver processor (Graph API, inbox %s).", GRAPH_MAILBOX)
    token = get_graph_token()
    if not token:
        return

    project_folders = list_project_folders()
    if not project_folders:
        logging.error("No project folders loaded. Check EGNYTE_DOMAIN, EGNYTE_ACCESS_TOKEN, EGNYTE_PROJECTS_PATH in .env.")
        return

    cutoff = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    logging.info("Processing emails received after: %s (UTC)", cutoff.isoformat() + "Z")
    messages = fetch_inbox_messages(token, cutoff)
    count = len(messages)
    if count == 0:
        logging.info("No emails found in the lookback window. Done.")
        return

    logging.info("Found %d email(s) in inbox.", count)
    processed = 0
    failed_list = []
    for m in messages:
        subject = m.get("subject") or ""
        from_info = m.get("from", {}) or {}
        email_addr = from_info.get("emailAddress", {}) or {}
        sender = email_addr.get("name") or email_addr.get("address") or ""
        msg_id = m.get("id")
        if not msg_id:
            continue
        attachments_data = fetch_attachments_with_content(token, msg_id)
        adapters = [_GraphAttachmentAdapter(d["name"], d["content"]) for d in attachments_data]
        adapter = _GraphMessageAdapter(subject, sender, adapters)
        try:
            ok, info = process_mail_item(adapter, project_folders)
            if ok:
                logging.info("Processed mail: '%s'", subject)
                processed += 1
            else:
                logging.warning("Could not process mail: '%s'. Reason: %s", subject, info)
                failed_list.append((subject, info))
        except Exception as e:
            logging.exception("Error processing mail Subject='%s': %s", subject, e)
            failed_list.append((subject, str(e)))

    logging.info("Done. Processed: %d, Failed/Skipped: %d, Total: %d", processed, len(failed_list), count)
    if failed_list:
        try:
            from mail_send_oauth import send_alert_email
            if send_alert_email(failed_list):
                logging.info("Failure alert email sent.")
            else:
                logging.warning("Failed to send failure alert email.")
        except Exception as e:
            logging.exception("Could not send failure alert email: %s", e)


if __name__ == "__main__":
    if not AZURE_TENANT_ID or not AZURE_CLIENT_ID:
        logging.error("AZURE_TENANT_ID and AZURE_CLIENT_ID must be set in .env. Cannot continue.")
    elif not EGNYTE_DOMAIN or not EGNYTE_TOKEN:
        logging.error("EGNYTE_DOMAIN or EGNYTE_ACCESS_TOKEN not set in .env. Cannot continue.")
    elif not (os.getenv("AZURE_PRIVATE_KEY_PATH") or os.getenv("AZURE_CLIENT_SECRET")):
        logging.error("Set AZURE_PRIVATE_KEY_PATH or AZURE_CLIENT_SECRET in .env. Cannot continue.")
    else:
        main()
