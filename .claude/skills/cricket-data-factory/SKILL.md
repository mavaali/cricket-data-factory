---
name: cricket-data-factory
description: Use when working on the cricket-data-factory project — Cricsheet data pipeline in Microsoft Fabric with cricket-mcp schema compatibility.
---

# Cricket Data Factory Project Skill

## Overview

This project brings cricket-mcp's data into Microsoft Fabric. The critical constraint is **schema compatibility** — the Fabric lakehouse tables MUST match cricket-mcp's DuckDB schema exactly so all 26 MCP tools work unchanged.

## When to Use

- Working in the `cricket-data-factory` repo
- Creating or modifying the ETL notebook
- Building dataflows for player enrichment or incremental updates
- Configuring the semantic model
- Debugging schema mismatches between Fabric and cricket-mcp

## Schema Reference (cricket-mcp)

Source: https://github.com/mavaali/cricket-mcp/blob/main/src/db/schema.ts

### players
```sql
CREATE TABLE players (
  player_id       VARCHAR PRIMARY KEY,    -- Cricsheet registry hex ID (e.g., "b8d490fd")
  player_name     VARCHAR NOT NULL,       -- Display name
  batting_style   VARCHAR,                -- From enrichment (e.g., "Right hand Bat")
  bowling_style   VARCHAR,                -- From enrichment (e.g., "Right arm Fast")
  playing_role    VARCHAR,                -- From enrichment (e.g., "Batter", "Allrounder")
  country         VARCHAR                 -- From enrichment
);
```

### matches
```sql
CREATE TABLE matches (
  match_id            VARCHAR PRIMARY KEY, -- Cricsheet filename (e.g., "1234567")
  match_type          VARCHAR NOT NULL,    -- Test, ODI, T20, IT20, ODM, MDM
  gender              VARCHAR NOT NULL,    -- male, female
  season              VARCHAR,             -- "2024", "2023/24"
  date_start          VARCHAR NOT NULL,    -- "2024-01-15"
  date_end            VARCHAR,
  team1               VARCHAR NOT NULL,
  team2               VARCHAR NOT NULL,
  venue               VARCHAR,
  city                VARCHAR,
  toss_winner         VARCHAR,
  toss_decision       VARCHAR,             -- "bat" or "field"
  outcome_winner      VARCHAR,
  outcome_by_runs     INTEGER,
  outcome_by_wickets  INTEGER,
  outcome_by_innings  INTEGER,
  outcome_result      VARCHAR,             -- "draw", "tie", "no result"
  outcome_method      VARCHAR,             -- "D/L", "VJD", "Awarded"
  player_of_match     VARCHAR,             -- Comma-separated names
  event_name          VARCHAR,             -- "Indian Premier League", "ICC World Cup"
  event_match_number  INTEGER,
  event_group         VARCHAR,
  event_stage         VARCHAR,
  overs_per_side      INTEGER,             -- 20, 50, NULL for Tests
  balls_per_over      INTEGER DEFAULT 6,
  team_type           VARCHAR              -- "international" or "club"
);
```

### innings
```sql
CREATE TABLE innings (
  match_id         VARCHAR NOT NULL,
  innings_number   INTEGER NOT NULL,       -- 1, 2, 3, 4 (Tests can have 4)
  batting_team     VARCHAR NOT NULL,
  bowling_team     VARCHAR NOT NULL,
  is_super_over    BOOLEAN DEFAULT FALSE,
  declared         BOOLEAN DEFAULT FALSE,
  forfeited        BOOLEAN DEFAULT FALSE,
  target_runs      INTEGER,                -- Chase target
  target_overs     INTEGER,
  PRIMARY KEY (match_id, innings_number)
);
```

### deliveries
```sql
CREATE TABLE deliveries (
  match_id             VARCHAR NOT NULL,
  innings_number       INTEGER NOT NULL,
  over_number          INTEGER NOT NULL,   -- 0-indexed (over 0 = first over)
  ball_number          INTEGER NOT NULL,   -- 1-indexed within over
  batter               VARCHAR NOT NULL,   -- Player name
  batter_id            VARCHAR,            -- Cricsheet registry ID
  bowler               VARCHAR NOT NULL,
  bowler_id            VARCHAR,
  non_striker          VARCHAR NOT NULL,
  non_striker_id       VARCHAR,
  runs_batter          INTEGER NOT NULL DEFAULT 0,
  runs_extras          INTEGER NOT NULL DEFAULT 0,
  runs_total           INTEGER NOT NULL DEFAULT 0,
  runs_non_boundary    BOOLEAN DEFAULT FALSE,
  extras_wides         INTEGER DEFAULT 0,
  extras_noballs       INTEGER DEFAULT 0,
  extras_byes          INTEGER DEFAULT 0,
  extras_legbyes       INTEGER DEFAULT 0,
  extras_penalty       INTEGER DEFAULT 0,
  is_wicket            BOOLEAN DEFAULT FALSE,
  wicket_kind          VARCHAR,            -- bowled, caught, lbw, run out, stumped, etc.
  wicket_player_out    VARCHAR,
  wicket_player_out_id VARCHAR,
  wicket_fielder1      VARCHAR,
  wicket_fielder2      VARCHAR,
  PRIMARY KEY (match_id, innings_number, over_number, ball_number)
);
```

## Common Column Name Mistakes

These will break cricket-mcp tools:

| Wrong | Correct | Notes |
|---|---|---|
| `name` | `player_name` | players table |
| `date` | `date_start` | matches table |
| `overs` | `overs_per_side` | matches table |
| `super_over` | `is_super_over` | innings table |
| `over` | `over_number` | deliveries table |
| `ball` | `ball_number` | deliveries table |
| `fielder` | `wicket_fielder1` | deliveries table |

## Cricket Logic Rules

These affect all analytics calculations:

| Rule | Implementation |
|---|---|
| Batting average = runs / dismissals (NOT innings) | Count `is_wicket = true` where `wicket_kind` is NOT 'retired hurt', 'retired not out' |
| Balls faced excludes wides | `WHERE extras_wides = 0` |
| Bowler runs exclude byes and legbyes | `runs_total - extras_byes - extras_legbyes` |
| Legal deliveries exclude wides AND noballs | `WHERE extras_wides = 0 AND extras_noballs = 0` |
| Bowling wickets exclude run outs | `WHERE wicket_kind NOT IN ('run out', 'retired hurt', 'retired not out', 'obstructing the field')` |
| Chasing = 2nd innings (T20/ODI), 4th innings (Test) | Check `innings_number` against `match_type` |
| Powerplay = overs 0-5, Middle = 6-14, Death = 15-19 | `over_number` (0-indexed) |

## Data Sources

| Source | URL | Size | What |
|---|---|---|---|
| Cricsheet (full) | `https://cricsheet.org/downloads/all_json.zip` | ~94 MB | 21K+ matches, all formats |
| Cricsheet (recent) | `https://cricsheet.org/downloads/recently_played_7_json.zip` | Small | Last 7 days of matches |
| Player metadata | `https://raw.githubusercontent.com/mavaali/cricket-mcp/main/data/player_meta.csv` | 2 MB | 16K players from ESPNCricinfo |

## Workspace Configuration

Workspace-specific IDs, URLs, and connection details are in `.claude/local-config.md` (gitignored).
Copy `.claude/local-config.example.md` to `.claude/local-config.md` and fill in your Fabric workspace details.

**Always read `.claude/local-config.md` at the start of a session for workspace IDs, portal URLs, SQL endpoints, connection IDs, and Livy session details.**

## Key Learnings

- **Fabric Jobs API** returns "failed without detail error" — always use **Livy API** for notebook execution
- **DataFactory MCP NuGet version**: use `Microsoft.DataFactory.MCP@0.16.0-beta` (not `latest`)
- **DataFactory MCP feature flags**: `--pipeline` and `--dataflow-query` enable pipeline and query tools
- **Dataflow AllowCombine**: required for multi-source (Web + Lakehouse), must be on fresh dataflow
- **Player CSV columns**: `cricinfo_id` (NOT `espn_id`), `cricsheet_id`, `unique_name`, `full_name`, `name`, `country`, `dob`, `batting_style`, `bowling_style`, `birthplace`, `playing_role`
- **DirectLake model.bim**: needs `definition.pbism` alongside, `compatibilityLevel: 1604`, `expressionSource` referencing `Sql.Database()` expression
- **Power BI Modeling MCP on macOS**: needs `codesign -s -` to ad-hoc sign the unsigned binary

## Livy Session (for interactive Spark execution)

- **Livy API base**: `https://api.fabric.microsoft.com/v1/workspaces/{WS}/lakehouses/{LH}/livyApi/versions/2023-12-01/sessions/{SESSION}`
- **Script**: `scripts/run_livy.py` — submits cells from `notebooks/CricketETL.py` to the Livy session
- **Cell names**: imports, download, parse, players, matches, innings, deliveries, optimize, validate, enrich, cleanup

### Why Livy over Jobs API

The Fabric Jobs API (`/jobs/instances?jobType=RunNotebook`) returns "Job instance failed without detail error" with no stack trace. The Livy API returns actual Python output per statement, making debugging possible. Always use Livy for programmatic notebook execution.

### Creating a new Livy session
```bash
TOKEN=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
curl -s -X POST "https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}/livyApi/versions/2023-12-01/sessions" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{}'
```

## Player Enrichment

The `player_meta.csv` has these columns (verified from actual file):
- `cricinfo_id` — ESPNCricinfo profile ID (NOT `espn_id`)
- `cricsheet_id` — maps to `players.player_id`
- `unique_name` — ESPNCricinfo canonical name
- `full_name` — full player name
- `name` — short name
- `country`, `dob`, `batting_style`, `bowling_style`, `birthplace`, `playing_role`

Join key: `player_enrichment.cricsheet_id = players.player_id`
