#!/usr/bin/env python3
"""Push CricketETL notebook definition to Fabric via REST API"""
import json, base64, urllib.request, subprocess

# Get token via Azure CLI
result = subprocess.run(
    ['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com', '--query', 'accessToken', '-o', 'tsv'],
    capture_output=True, text=True
)
token = result.stdout.strip()
print(f"Token length: {len(token)}")

# Read notebook
with open('/Users/mihirwagle/projects/cricket-data-factory/notebooks/CricketETL.ipynb', 'r') as f:
    nb_content = f.read()

# Base64 encode
nb_b64 = base64.b64encode(nb_content.encode('utf-8')).decode('utf-8')
print(f"Notebook base64 length: {len(nb_b64)}")

workspace_id = "4995b8e1-65cc-46f6-b456-644a46e082d5"
notebook_id = "8afec919-e692-4099-adc0-a019fa0d581f"

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
