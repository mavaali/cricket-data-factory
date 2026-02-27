#!/usr/bin/env python3
"""Deploy CricketAnalytics semantic model to Fabric via REST API.

Required env vars (or .env file):
  FABRIC_WORKSPACE_ID     - Fabric workspace GUID
  FABRIC_SQL_ENDPOINT     - Lakehouse SQL endpoint hostname
  FABRIC_SQL_ENDPOINT_ID  - SQL endpoint GUID
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
SQL_ENDPOINT = os.environ.get('FABRIC_SQL_ENDPOINT', '')
SQL_ENDPOINT_ID = os.environ.get('FABRIC_SQL_ENDPOINT_ID', '')

if not all([WS, SQL_ENDPOINT, SQL_ENDPOINT_ID]):
    print('Error: Set FABRIC_WORKSPACE_ID, FABRIC_SQL_ENDPOINT, FABRIC_SQL_ENDPOINT_ID in .env or environment')
    sys.exit(1)

def get_token():
    r = subprocess.run(['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com', '--query', 'accessToken', '-o', 'tsv'], capture_output=True, text=True)
    return r.stdout.strip()

# Build the TMDL model definition with proper DirectLake expressions
model_bim = {
    "compatibilityLevel": 1604,
    "model": {
        "culture": "en-US",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "sourceQueryCulture": "en-US",
        "tables": [],
        "relationships": [],
        "expressions": [
            {
                "name": "DatabaseQuery",
                "kind": "m",
                "expression": [
                    "let",
                    f"    database = Sql.Database(\"{SQL_ENDPOINT}\", \"{SQL_ENDPOINT_ID}\")",
                    "in",
                    "    database"
                ]
            }
        ],
        "annotations": [
            {"name": "PBI_QueryOrder", "value": "[\"deliveries\",\"matches\",\"innings\",\"players\",\"player_enrichment\"]"}
        ]
    }
}

# Helper to create a DirectLake table
def make_table(name, columns, measures=None):
    table = {
        "name": name,
        "columns": columns,
        "partitions": [
            {
                "name": name,
                "mode": "directLake",
                "source": {
                    "type": "entity",
                    "entityName": name,
                    "schemaName": "dbo",
                    "expressionSource": "DatabaseQuery"
                }
            }
        ]
    }
    if measures:
        table["measures"] = measures
    return table

# Column helper
def col(name, dtype="string"):
    type_map = {"string": "string", "int64": "int64", "boolean": "boolean", "double": "double"}
    return {"name": name, "dataType": type_map.get(dtype, "string"), "sourceColumn": name}

# Deliveries table with measures
deliveries_measures = [
    {"name": "Total Runs", "expression": "SUM(deliveries[runs_total])", "formatString": "#,##0"},
    {"name": "Batter Runs", "expression": "SUM(deliveries[runs_batter])", "formatString": "#,##0"},
    {"name": "Balls Faced", "expression": "COUNTROWS(FILTER(deliveries, deliveries[extras_wides] = 0))", "formatString": "#,##0"},
    {"name": "Strike Rate", "expression": "DIVIDE([Batter Runs], [Balls Faced], 0) * 100", "formatString": "#,##0.00"},
    {"name": "Dismissals", "expression": "COUNTROWS(FILTER(deliveries, deliveries[is_wicket] = TRUE() && NOT(deliveries[wicket_kind] IN {\"retired hurt\", \"retired not out\", \"retired out\"})))", "formatString": "#,##0"},
    {"name": "Batting Average", "expression": "DIVIDE([Batter Runs], [Dismissals], 0)", "formatString": "#,##0.00"},
    {"name": "Wickets", "expression": "COUNTROWS(FILTER(deliveries, deliveries[is_wicket] = TRUE() && NOT(deliveries[wicket_kind] IN {\"run out\", \"retired hurt\", \"retired not out\", \"retired out\", \"obstructing the field\"})))", "formatString": "#,##0"},
    {"name": "Bowler Runs", "expression": "SUMX(deliveries, deliveries[runs_total] - deliveries[extras_byes] - deliveries[extras_legbyes])", "formatString": "#,##0"},
    {"name": "Legal Deliveries", "expression": "COUNTROWS(FILTER(deliveries, deliveries[extras_wides] = 0 && deliveries[extras_noballs] = 0))", "formatString": "#,##0"},
    {"name": "Economy Rate", "expression": "DIVIDE([Bowler Runs], [Legal Deliveries], 0) * 6", "formatString": "#,##0.00"},
    {"name": "Bowling Average", "expression": "DIVIDE([Bowler Runs], [Wickets], 0)", "formatString": "#,##0.00"},
    {"name": "Bowling Strike Rate", "expression": "DIVIDE([Legal Deliveries], [Wickets], 0)", "formatString": "#,##0.00"},
    {"name": "Dot Ball %", "expression": "DIVIDE(COUNTROWS(FILTER(deliveries, deliveries[runs_total] = 0)), COUNTROWS(deliveries), 0) * 100", "formatString": "#,##0.0"},
    {"name": "Boundary %", "expression": "DIVIDE(COUNTROWS(FILTER(deliveries, deliveries[runs_batter] IN {4, 6} && deliveries[runs_non_boundary] = FALSE())), [Balls Faced], 0) * 100", "formatString": "#,##0.0"},
]

model_bim["model"]["tables"] = [
    make_table("deliveries", [
        col("match_id"), col("innings_number", "int64"), col("over_number", "int64"), col("ball_number", "int64"),
        col("batter"), col("batter_id"), col("bowler"), col("bowler_id"), col("non_striker"),
        col("runs_batter", "int64"), col("runs_extras", "int64"), col("runs_total", "int64"),
        col("runs_non_boundary", "boolean"),
        col("extras_wides", "int64"), col("extras_noballs", "int64"), col("extras_byes", "int64"), col("extras_legbyes", "int64"),
        col("is_wicket", "boolean"), col("wicket_kind"), col("wicket_player_out"), col("wicket_fielder1"),
        col("batting_team"), col("bowling_team"),
    ], deliveries_measures),
    make_table("matches", [
        col("match_id"), col("match_type"), col("gender"), col("team_type"),
        col("venue"), col("city"), col("date_start"), col("team1"), col("team2"),
        col("toss_winner"), col("toss_decision"), col("outcome_winner"), col("outcome_result"),
        col("event_name"), col("season"),
    ]),
    make_table("innings", [
        col("match_id"), col("innings_number", "int64"), col("batting_team"), col("bowling_team"),
        col("target_runs", "int64"), col("declared", "boolean"), col("forfeited", "boolean"), col("is_super_over", "boolean"),
    ]),
    make_table("players", [
        col("player_id"), col("player_name"), col("batting_style"), col("bowling_style"), col("playing_role"), col("country"),
    ]),
    make_table("player_enrichment", [
        col("cricinfo_id"), col("cricsheet_id"), col("unique_name"), col("full_name"),
        col("country"), col("dob"), col("batting_style"), col("bowling_style"), col("playing_role"),
    ]),
]

# Relationships
model_bim["model"]["relationships"] = [
    {"name": "deliveries_to_matches", "fromTable": "deliveries", "fromColumn": "match_id", "toTable": "matches", "toColumn": "match_id"},
    {"name": "deliveries_batter_to_players", "fromTable": "deliveries", "fromColumn": "batter_id", "toTable": "players", "toColumn": "player_id"},
    {"name": "deliveries_bowler_to_players", "fromTable": "deliveries", "fromColumn": "bowler_id", "toTable": "players", "toColumn": "player_id", "isActive": False},
    {"name": "players_to_enrichment", "fromTable": "players", "fromColumn": "player_id", "toTable": "player_enrichment", "toColumn": "cricsheet_id"},
]

# Encode as base64
bim_json = json.dumps(model_bim)
bim_b64 = base64.b64encode(bim_json.encode()).decode()

# definition.pbism (required)
pbism = {"version": "1.0", "settings": {}}
pbism_b64 = base64.b64encode(json.dumps(pbism).encode()).decode()

# Create the semantic model with definition
payload = {
    "displayName": "CricketAnalytics",
    "description": "Cricket analytics - 14 DAX measures, 4 relationships, DirectLake on CricketLakehouse",
    "definition": {
        "parts": [
            {
                "path": "model.bim",
                "payload": bim_b64,
                "payloadType": "InlineBase64"
            },
            {
                "path": "definition.pbism",
                "payload": pbism_b64,
                "payloadType": "InlineBase64"
            }
        ]
    }
}

token = get_token()
url = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/semanticModels"
data = json.dumps(payload).encode()
req = urllib.request.Request(url, data=data, method='POST')
req.add_header('Authorization', f'Bearer {token}')
req.add_header('Content-Type', 'application/json')

print(f"Creating CricketAnalytics semantic model...")
print(f"Payload size: {len(data)} bytes")

try:
    resp = urllib.request.urlopen(req, timeout=60)
    print(f"OK: {resp.status}")
    loc = resp.getheader('Location')
    if loc:
        print(f"Operation URL: {loc}")
    body = resp.read().decode()
    if body:
        result = json.loads(body)
        print(f"ID: {result.get('id')}")
        print(f"Name: {result.get('displayName')}")
    else:
        print("Accepted (async) - check workspace for new semantic model")
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code}: {e.read().decode()[:500]}")
except Exception as e:
    print(f"Error: {e}")
