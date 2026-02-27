"""
get_token.py

Gets an Egnyte access token via Implicit Grant (browser). Use with ngrok
so the redirect URI is HTTPS (required by Egnyte).

1. Run: ngrok http 8080
2. In .env set REDIRECT_URI=https://<your-ngrok-host>/callback
3. In Egnyte app set Registered OAuth Redirect URI to the same URL
4. Run: python get_token.py

Requires in .env: EGNYTE_DOMAIN, EGNYTE_CLIENT_ID, REDIRECT_URI
Saves the token to .env as EGNYTE_ACCESS_TOKEN.
"""

import os
import re
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, quote
from dotenv import load_dotenv

load_dotenv()

EGNYTE_DOMAIN = os.getenv("EGNYTE_DOMAIN")
CLIENT_ID = os.getenv("EGNYTE_CLIENT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
PORT = 8080

# Page at /callback: read token from URL fragment and send to /save so Python can capture it
CALLBACK_HTML = """
<html><body>
<p>Extracting token, please wait...</p>
<script>
  var hash = window.location.hash.substring(1);
  var params = {};
  hash.split('&').forEach(function(part) {
    var item = part.split('=');
    params[item[0]] = decodeURIComponent(item[1] || '');
  });
  if (params['access_token']) {
    window.location = '/save?access_token=' + encodeURIComponent(params['access_token']);
  } else {
    document.body.innerHTML = '<p style="color:red">No access_token in URL. Try again.</p>';
  }
</script>
</body></html>
"""

token_holder = {"token": None}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/callback":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(CALLBACK_HTML.encode())
        elif parsed.path == "/save":
            params = parse_qs(parsed.query)
            token = params.get("access_token", [None])[0]
            if token:
                token_holder["token"] = token
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h2>Token saved. You can close this tab.</h2></body></html>")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"No token found.")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def save_token_to_env(token):
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    with open(env_path, "r") as f:
        content = f.read()
    if "EGNYTE_ACCESS_TOKEN=" in content:
        content = re.sub(r"EGNYTE_ACCESS_TOKEN=.*", f"EGNYTE_ACCESS_TOKEN={token}", content)
    else:
        content += f"\nEGNYTE_ACCESS_TOKEN={token}\n"
    with open(env_path, "w") as f:
        f.write(content)
    print(f"  Saved to .env: EGNYTE_ACCESS_TOKEN={token[:6]}...{token[-4:]}")


def main():
    if not EGNYTE_DOMAIN or not CLIENT_ID:
        print("Missing in .env: EGNYTE_DOMAIN and EGNYTE_CLIENT_ID")
        return
    if not REDIRECT_URI or not REDIRECT_URI.strip().lower().startswith("https://"):
        print("Missing or invalid REDIRECT_URI in .env. Set it to your ngrok HTTPS URL, e.g.:")
        print("  REDIRECT_URI=https://abc123.ngrok-free.app/callback")
        return

    redirect_uri = REDIRECT_URI.strip()
    auth_url = (
        f"https://{EGNYTE_DOMAIN.strip()}.egnyte.com/puboauth/token"
        f"?client_id={CLIENT_ID.strip()}"
        f"&redirect_uri={quote(redirect_uri)}"
        f"&response_type=token"
        f"&scope=Egnyte.filesystem"
    )

    print("Opening browser for Egnyte login...")
    print("(Ensure ngrok is running: ngrok http 8080)")
    webbrowser.open(auth_url)

    print(f"Waiting for callback on port {PORT} (ngrok forwards to this)...")
    server = HTTPServer(("localhost", PORT), Handler)
    while not token_holder["token"]:
        server.handle_request()

    server.server_close()
    print("\nToken received!")
    save_token_to_env(token_holder["token"])
    print("\nDone. Now run: python test_egnyte.py")


if __name__ == "__main__":
    main()
