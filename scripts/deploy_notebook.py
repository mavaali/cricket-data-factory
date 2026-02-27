#!/usr/bin/env python3
"""Push CricketETL notebook definition to Fabric via REST API.

Required env vars (or .env file):
  FABRIC_WORKSPACE_ID  - Fabric workspace GUID
  FABRIC_NOTEBOOK_ID   - Notebook item GUID
"""
import json, base64, os, sys, urllib.request, subprocess
from pathlib import Path

# Load .env if present
def load_dotenv():
    env_path = Path(__file__).resolve().parent.parent / '.env'
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

load_dotenv()

workspace_id = os.environ.get('FABRIC_WORKSPACE_ID', '')
notebook_id = os.environ.get('FABRIC_NOTEBOOK_ID', '')

if not all([workspace_id, notebook_id]):
    print('Error: Set FABRIC_WORKSPACE_ID, FABRIC_NOTEBOOK_ID in .env or environment')
    sys.exit(1)

# Get token via Azure CLI
result = subprocess.run(
    ['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com', '--query', 'accessToken', '-o', 'tsv'],
    capture_output=True, text=True
)
token = result.stdout.strip()
print(f"Token length: {len(token)}")

# Read notebook
nb_path = Path(__file__).resolve().parent.parent / 'notebooks' / 'CricketETL.ipynb'
with open(nb_path, 'r') as f:
    nb_content = f.read()

# Base64 encode
nb_b64 = base64.b64encode(nb_content.encode('utf-8')).decode('utf-8')
print(f"Notebook base64 length: {len(nb_b64)}")

payload = {
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.py",
                "payload": nb_b64,
                "payloadType": "InlineBase64"
            }
        ]
    }
}

url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition"

data = json.dumps(payload).encode('utf-8')
req = urllib.request.Request(url, data=data, method='POST')
req.add_header('Authorization', f'Bearer {token}')
req.add_header('Content-Type', 'application/json')

try:
    resp = urllib.request.urlopen(req)
    print(f"Status: {resp.status}")
    body = resp.read().decode('utf-8')
    print(f"Response: {body[:500] if body else '(empty - success)'}")
except urllib.error.HTTPError as e:
    print(f"Error: {e.code}")
    body = e.read().decode('utf-8')
    print(f"Response: {body[:500]}")
