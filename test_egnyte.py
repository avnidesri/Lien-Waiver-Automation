"""
test_egnyte.py

Uses EGNYTE_ACCESS_TOKEN from .env (obtained via get_token.py) to list
project folders from Egnyte.

Run: python get_token.py  (once, or when token expires)
     python test_egnyte.py
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

EGNYTE_DOMAIN = os.getenv("EGNYTE_DOMAIN")
ACCESS_TOKEN = os.getenv("EGNYTE_ACCESS_TOKEN")
PROJECTS_PATH = os.getenv("EGNYTE_PROJECTS_PATH", "/Shared/Projects/Portfolio")

print(f"Domain : {EGNYTE_DOMAIN}.egnyte.com")
print(f"Path   : {PROJECTS_PATH}")
print("-" * 50)

if not EGNYTE_DOMAIN or not ACCESS_TOKEN:
    print("Missing in .env: EGNYTE_DOMAIN and/or EGNYTE_ACCESS_TOKEN")
    print("Run get_token.py first to obtain a token.")
    raise SystemExit(1)

print("Using token from .env (EGNYTE_ACCESS_TOKEN)")
print("Listing project folders...")
url = f"https://{EGNYTE_DOMAIN}.egnyte.com/pubapi/v1/fs{PROJECTS_PATH}"
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
resp = requests.get(url, headers=headers, timeout=30)

if resp.status_code != 200:
    print(f"  FAILED ({resp.status_code}): {resp.text}")
    raise SystemExit(1)

folders = sorted([f["name"] for f in resp.json().get("folders", [])])
print(f"  SUCCESS — found {len(folders)} project folders:\n")
for name in folders:
    print(f"    {name}")
