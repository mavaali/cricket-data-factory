#!/usr/bin/env python3
"""List all connections and their types"""
import json

with open('/Users/mihirwagle/Library/Application Support/Code/User/workspaceStorage/56e2e6e03ed4809f8e03a57db2b64b06/GitHub.copilot-chat/chat-session-resources/97d20618-cc1e-4dd7-a70b-76b314acf550/toolu_01NYLKo6kxQL55mxvXB9asPh__vscode-1772125560585/content.json') as f:
    data = json.load(f)

conns = data.get('connections', [])
print(f"Total connections: {len(conns)}")

# Group by connectivityType
types = {}
for c in conns:
    ct = c.get('connectivityType','')
    types.setdefault(ct, []).append(c)

print("\nConnection types:")
for ct, items in sorted(types.items()):
    print(f"  {ct}: {len(items)}")

# Show all with their paths
print("\nAll connections:")
for c in conns[:30]:
    ct = c.get('connectivityType','')
    path = c.get('connectionDetails',{}).get('path','')
    dtype = c.get('connectionDetails',{}).get('type','')
    cid = c.get('id','')
    name = c.get('displayName','') or '(unnamed)'
    print(f"  {cid[:12]}... | {ct} | {dtype} | {name} | {path[:80]}")
