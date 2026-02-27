#!/usr/bin/env python3
"""Find connection creation API schema"""
import json

with open('/Users/mihirwagle/Library/Application Support/Code/User/workspaceStorage/56e2e6e03ed4809f8e03a57db2b64b06/GitHub.copilot-chat/chat-session-resources/97d20618-cc1e-4dd7-a70b-76b314acf550/toolu_013vbbqwpjJN5kmMCSTA7sv6__vscode-1772125560618/content.json') as f:
    data = json.load(f)

schemas = data.get('components', {}).get('schemas', {})
for name, schema in schemas.items():
    if 'connection' in name.lower():
        props = list(schema.get('properties', {}).keys())
        print(f"  {name}: {props[:8]}")

# Also check paths
paths = data.get('paths', {})
for path, methods in paths.items():
    if 'connection' in path.lower():
        for method, spec in methods.items():
            print(f"\n{method.upper()} {path}")
            rb = spec.get('requestBody', {})
            if rb:
                schema = rb.get('content', {}).get('application/json', {}).get('schema', {})
                ref = schema.get('$ref', '')
                if ref:
                    ref_name = ref.split('/')[-1]
                    print(f"  Body schema: {ref_name}")
                    if ref_name in schemas:
                        print(json.dumps(schemas[ref_name], indent=2)[:1500])
