#!/usr/bin/env python3
"""Deploy pipeline definition to Fabric.

Required env vars (or .env file):
  FABRIC_WORKSPACE_ID  - Fabric workspace GUID
  FABRIC_PIPELINE_ID   - Pipeline item GUID
  FABRIC_NOTEBOOK_ID   - Notebook item GUID
  FABRIC_DATAFLOW_ID   - Dataflow item GUID
"""
import json, base64, os, subprocess, ssl, sys, urllib.request
from pathlib import Path

ssl._create_default_https_context = ssl._create_unverified_context

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

WS = os.environ.get('FABRIC_WORKSPACE_ID', '')
PIPELINE_ID = os.environ.get('FABRIC_PIPELINE_ID', '')
NOTEBOOK_ID = os.environ.get('FABRIC_NOTEBOOK_ID', '')
DATAFLOW_ID = os.environ.get('FABRIC_DATAFLOW_ID', '')

if not all([WS, PIPELINE_ID, NOTEBOOK_ID, DATAFLOW_ID]):
    print('Error: Set FABRIC_WORKSPACE_ID, FABRIC_PIPELINE_ID, FABRIC_NOTEBOOK_ID, FABRIC_DATAFLOW_ID in .env or environment')
    sys.exit(1)

# Get token
r = subprocess.run(['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com', '--query', 'accessToken', '-o', 'tsv'], capture_output=True, text=True)
token = r.stdout.strip()

# Pipeline definition
pipeline = {
    "properties": {
        "activities": [
            {
                "name": "RunCricketETL",
                "type": "TridentNotebook",
                "dependsOn": [],
                "typeProperties": {
                    "notebookId": NOTEBOOK_ID,
                    "workspaceId": WS
                }
            },
            {
                "name": "RunPlayerEnrichment",
                "type": "DataflowV2",
                "dependsOn": [
                    {
                        "activity": "RunCricketETL",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "dataflowId": DATAFLOW_ID,
                    "workspaceId": WS
                }
            }
        ]
    }
}

payload_b64 = base64.b64encode(json.dumps(pipeline).encode()).decode()
body = json.dumps({
    "definition": {
        "parts": [
            {
                "path": "pipeline-content.json",
                "payload": payload_b64,
                "payloadType": "InlineBase64"
            }
        ]
    }
}).encode()

url = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{PIPELINE_ID}/updateDefinition"
req = urllib.request.Request(url, data=body, method='POST')
req.add_header('Authorization', f'Bearer {token}')
req.add_header('Content-Type', 'application/json')

try:
    resp = urllib.request.urlopen(req, timeout=30)
    print(f"OK: {resp.status}")
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code}: {e.read().decode()[:500]}")
except Exception as e:
    print(f"Error: {e}")
