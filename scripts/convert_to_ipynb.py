#!/usr/bin/env python3
"""Convert CricketETL.py (Fabric .py format) to .ipynb"""
import json

with open('/Users/mihirwagle/projects/cricket-data-factory/notebooks/CricketETL.py', 'r') as f:
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
                'default_lakehouse': '3c76307d-170f-423b-abfc-5b52e5f01e5e',
                'default_lakehouse_name': 'CricketLakehouse',
                'default_lakehouse_workspace_id': '4995b8e1-65cc-46f6-b456-644a46e082d5',
                'known_lakehouses': [
                    {
                        'id': '3c76307d-170f-423b-abfc-5b52e5f01e5e'
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

with open('/Users/mihirwagle/projects/cricket-data-factory/notebooks/CricketETL.ipynb', 'w') as f:
    json.dump(notebook, f, indent=2)
print('Written CricketETL.ipynb')
