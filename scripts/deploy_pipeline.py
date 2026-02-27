#!/usr/bin/env python3
"""Deploy pipeline definition to Fabric"""
import json, base64, subprocess, ssl, urllib.request

ssl._create_default_https_context = ssl._create_unverified_context

WS = "4995b8e1-65cc-46f6-b456-644a46e082d5"
PIPELINE_ID = "7fbc8e79-9a5b-4e59-ba00-200f96d34062"
NOTEBOOK_ID = "8afec919-e692-4099-adc0-a019fa0d581f"
DATAFLOW_ID = "28e9ae1c-26ad-4d1a-9072-ceb97c166943"

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
