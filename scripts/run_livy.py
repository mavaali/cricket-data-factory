#!/usr/bin/env python3
"""Submit a code cell to the Fabric Livy session and poll for result.

Required env vars (or .env file):
  FABRIC_WORKSPACE_ID  - Fabric workspace GUID
  FABRIC_LAKEHOUSE_ID  - Lakehouse GUID
  FABRIC_LIVY_SESSION  - Livy session GUID
"""
import json, os, subprocess, sys, time, ssl, urllib.request
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
LH = os.environ.get('FABRIC_LAKEHOUSE_ID', '')
SESSION = os.environ.get('FABRIC_LIVY_SESSION', '')

if not all([WS, LH, SESSION]):
    print('Error: Set FABRIC_WORKSPACE_ID, FABRIC_LAKEHOUSE_ID, FABRIC_LIVY_SESSION in .env or environment')
    sys.exit(1)

BASE = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/lakehouses/{LH}/livyApi/versions/2023-12-01/sessions/{SESSION}"

def get_token():
    r = subprocess.run(['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com', '--query', 'accessToken', '-o', 'tsv'], capture_output=True, text=True)
    return r.stdout.strip()

def submit(code, kind="pyspark"):
    token = get_token()
    data = json.dumps({"code": code, "kind": kind}).encode()
    req = urllib.request.Request(f"{BASE}/statements", data=data, method='POST')
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Content-Type', 'application/json')
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode())

def poll(stmt_id, max_wait=600):
    token = get_token()
    for i in range(max_wait // 5):
        time.sleep(5)
        req = urllib.request.Request(f"{BASE}/statements/{stmt_id}")
        req.add_header('Authorization', f'Bearer {token}')
        resp = urllib.request.urlopen(req, timeout=30)
        d = json.loads(resp.read().decode())
        state = d.get("state", "?")
        if state == "available":
            output = d.get("output", {})
            status = output.get("status")
            data = output.get("data", {})
            traceback = output.get("traceback", [])
            print(f"[Statement {stmt_id}] {status}")
            for v in data.values():
                print(v[:2000])
            for line in traceback[:10]:
                print(line)
            return status == "ok"
        if i % 6 == 0:
            print(f"  ...waiting ({i*5}s, state={state})")
    print(f"TIMEOUT after {max_wait}s")
    return False

if __name__ == "__main__":
    cell_name = sys.argv[1] if len(sys.argv) > 1 else "parse"
    
    # Read the notebook
    with open("notebooks/CricketETL.py", "r") as f:
        content = f.read()
    
    # Parse cells
    raw_cells = content.split("# CELL ********************")
    code_cells = []
    for raw in raw_cells:
        raw = raw.strip()
        if not raw or raw.startswith("# Fabric notebook") or raw.startswith("# METADATA") or raw.startswith("# MARKDOWN"):
            continue
        code_cells.append(raw)
    
    # Cell mapping
    cell_map = {
        "params": 0,      # CRICSHEET_URL
        "imports": 1,      # import json...
        "download": 2,     # Download ZIP
        "parse": 3,        # Big parse loop
        "players": 4,      # Write players
        "matches": 5,      # Write matches
        "innings": 6,      # Write innings
        "deliveries": 7,   # Write deliveries
        "optimize": 8,     # OPTIMIZE
        "validate": 9,     # Validation queries
        "enrich": 10,      # Merge player_enrichment into players
        "cleanup": 11,     # Clean up
    }
    
    if cell_name == "list":
        for name, idx in cell_map.items():
            if idx < len(code_cells):
                preview = code_cells[idx][:80].replace('\n', ' ')
                print(f"  {name} (cell {idx}): {preview}...")
        sys.exit(0)
    
    if cell_name == "all":
        # Run all remaining cells
        cells_to_run = list(cell_map.items())
    elif cell_name in cell_map:
        cells_to_run = [(cell_name, cell_map[cell_name])]
    else:
        print(f"Unknown cell: {cell_name}. Use: {', '.join(cell_map.keys())}")
        sys.exit(1)
    
    for name, idx in cells_to_run:
        if idx >= len(code_cells):
            print(f"Skipping {name}: cell {idx} not found")
            continue
        code = code_cells[idx]
        print(f"\n{'='*60}")
        print(f"Running cell: {name} ({len(code)} chars)")
        print(f"{'='*60}")
        
        result = submit(code)
        stmt_id = result.get("id")
        print(f"Submitted as statement {stmt_id}")
        
        ok = poll(stmt_id)
        if not ok:
            print(f"FAILED at cell: {name}")
            sys.exit(1)
    
    print("\nAll cells completed successfully!")
