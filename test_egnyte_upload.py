#!/usr/bin/env python3
"""
test_egnyte_upload.py

Tests Egnyte token by uploading a small text file to your private folder
(Private/avni.goyal). Uses EGNYTE_ACCESS_TOKEN and EGNYTE_DOMAIN from .env.

Run: python test_egnyte_upload.py
"""

import os
from datetime import datetime
from urllib.parse import quote
import requests
from dotenv import load_dotenv

load_dotenv()

EGNYTE_DOMAIN = os.getenv("EGNYTE_DOMAIN")
ACCESS_TOKEN = os.getenv("EGNYTE_ACCESS_TOKEN")
# API path: /Private/avni.goyal (Z:\Private\avni.goyal in Egnyte drive)
UPLOAD_PATH = "/Private/avni.goyal/egnyte_upload_test.txt"


def encode_path(path):
    """Encode each path segment; slashes stay (per Egnyte docs)."""
    path = path.strip("/")
    if not path:
        return ""
    return "/".join(quote(segment, safe="") for segment in path.split("/"))


def main():
    if not EGNYTE_DOMAIN or not ACCESS_TOKEN:
        print("Missing EGNYTE_DOMAIN or EGNYTE_ACCESS_TOKEN in .env")
        return

    path_encoded = encode_path(UPLOAD_PATH.strip())
    url = f"https://{EGNYTE_DOMAIN.strip()}.egnyte.com/pubapi/v1/fs-content/{path_encoded}"
    content = f"Egnyte upload test at {datetime.utcnow().isoformat()}Z\n"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

    print(f"Uploading to: {UPLOAD_PATH}")
    print(f"(Same as Z:\\Private\\avni.goyal\\egnyte_upload_test.txt in Egnyte)")
    print()
    try:
        resp = requests.post(url, headers=headers, data=content.encode("utf-8"), timeout=30)
        if resp.status_code in (200, 201):
            print("SUCCESS: File uploaded. Egnyte token has write access.")
            return
        print(f"FAIL: {resp.status_code} {resp.reason}")
        print(resp.text[:500] if resp.text else "")
    except requests.RequestException as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    main()
