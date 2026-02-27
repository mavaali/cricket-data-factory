#!/usr/bin/env python3
"""Convert CricketETL.py (Fabric .py format) to .ipynb.

Required env vars (or .env file):
  FABRIC_WORKSPACE_ID  - Fabric workspace GUID
  FABRIC_LAKEHOUSE_ID  - Lakehouse GUID
"""
import json, os, sys
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

WORKSPACE_ID = os.environ.get('FABRIC_WORKSPACE_ID', '')
LAKEHOUSE_ID = os.environ.get('FABRIC_LAKEHOUSE_ID', '')

if not all([WORKSPACE_ID, LAKEHOUSE_ID]):
    print('Error: Set FABRIC_WORKSPACE_ID, FABRIC_LAKEHOUSE_ID in .env or environment')
    sys.exit(1)

project_root = Path(__file__).resolve().parent.parent
input_path = project_root / 'notebooks' / 'CricketETL.py'
output_path = project_root / 'notebooks' / 'CricketETL.ipynb'

with open(input_path, 'r') as f:
    content = f.read()

raw_cells = content.split('# CELL ********************')

cells = []
for raw in raw_cells:
    raw = raw.strip()
    if not raw:
        continue
    if raw.startswith('# Fabric notebook source') or raw.startswith('# METADATA'):
        continue
    
    if raw.startswith('# MARKDOWN ********************'):
        md_content = raw.replace('# MARKDOWN ********************', '').strip()
        lines = []
        for line in md_content.split('\n'):
            if line.startswith('# '):
                lines.append(line[2:])
            elif line == '#':
                lines.append('')
            else:
                lines.append(line)
        cells.append({
            'cell_type': 'markdown',
            'metadata': {},
            'source': [l + '\n' for l in lines]
        })
    else:
        metadata = {}
        if raw.startswith('# PARAMETERS'):
            metadata = {'tags': ['parameters']}
        cells.append({
            'cell_type': 'code',
            'metadata': metadata,
            'source': [l + '\n' for l in raw.split('\n')],
            'outputs': [],
            'execution_count': None
        })

notebook = {
    'nbformat': 4,
    'nbformat_minor': 5,
    'metadata': {
        'kernel_info': {'name': 'synapse_pyspark'},
        'kernelspec': {
            'name': 'synapse_pyspark',
            'display_name': 'Synapse PySpark'
        },
        'language_info': {'name': 'python'},
        'trident': {
            'lakehouse': {
                'default_lakehouse': LAKEHOUSE_ID,
                'default_lakehouse_name': 'CricketLakehouse',
                'default_lakehouse_workspace_id': WORKSPACE_ID,
                'known_lakehouses': [
                    {
                        'id': LAKEHOUSE_ID
                    }
                ]
            }
        }
    },
    'cells': cells
}

print(f'Cells: {len(cells)}')
print(f'Code cells: {sum(1 for c in cells if c["cell_type"] == "code")}')
print(f'Markdown cells: {sum(1 for c in cells if c["cell_type"] == "markdown")}')

with open(output_path, 'w') as f:
    json.dump(notebook, f, indent=2)
print(f'Written {output_path}')
