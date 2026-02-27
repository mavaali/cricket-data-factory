# Plan: Cricket Analytics Pipeline in Microsoft Fabric

## Narrative

"I have a cricket MCP server that works locally. Fabric **extends** it for enterprise use — shared dashboards, governed access, scheduled refreshes, team collaboration, and rich Power BI visualization. Four MCP servers collaborated in a single VS Code session to build the entire pipeline — zero portal clicks."

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        VS Code + Copilot                             │
│                                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  ┌─────────────┐ │
│  │ Fabric MCP   │  │ DataFactory  │  │ Power BI  │  │ cricket-mcp │ │
│  │ Server       │  │ MCP          │  │ MCP       │  │             │ │
│  │              │  │              │  │           │  │             │ │
│  │ • Create     │  │ • Dataflows  │  │ • TMDL    │  │ • 26 query  │ │
│  │   lakehouse  │  │ • Pipelines  │  │ • DAX     │  │   tools     │ │
│  │ • Upload     │  │ • M code     │  │ • Semantic │  │ • Matchups  │ │
│  │   files      │  │ • Connections│  │   model   │  │ • Stats     │ │
│  │ • Inspect    │  │ • Refresh    │  │           │  │ • Records   │ │
│  │   tables     │  │              │  │           │  │             │ │
│  │ • Best       │  │              │  │           │  │             │ │
│  │   practices  │  │              │  │           │  │             │ │
│  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘  └──────┬──────┘ │
│         │                 │                │               │        │
└─────────┼─────────────────┼────────────────┼───────────────┼────────┘
          │                 │                │               │
          ▼                 ▼                ▼               ▼
    ┌─────────────────────────────────────────────────────────────┐
    │              OneLake (CricketLakehouse)                      │
    │  players | matches | innings | deliveries | player_enrichment│
    └─────────────────────────────────────────────────────────────┘
```

**Key insight:** The semantic model IS the star schema. No physical dimension tables needed. Power BI semantic models add relationships, hierarchies, and measures on top of any physical schema. The 4-table schema with a semantic model gives everything a physical star schema would — without extra tables.

**Two consumers, one copy of data:**
- **cricket-mcp** (26 MCP tools) — DuckDB reads Delta tables from OneLake. All SQL queries work unchanged.
- **Power BI** (dashboards + DAX) — DirectLake mode reads the same Delta files. Virtual star schema via TMDL relationships.

## Four MCP Servers

| Server | Role | Genuine showcase |
|---|---|---|
| **Fabric MCP** (microsoft/mcp) | Infrastructure + inspection + best practices | Create lakehouse/notebook items, browse files, inspect tables, get Fabric API schemas and patterns — all from chat |
| **DataFactory MCP** (Microsoft/DataFactory.MCP) | Data movement + transformation + orchestration | Author M code dataflows, build pipelines, manage connections, refresh monitoring — external data ingestion via MCP |
| **Power BI MCP** (learn.microsoft.com) | Semantic modeling + analytics | TMDL model creation, DAX measures, execute DAX queries from VS Code |
| **cricket-mcp** (mavaali/cricket-mcp) | Domain-specific cricket analytics | 26 parameterized SQL tools against OneLake data — matchups, phase stats, GOAT queries |

## Two repos

| Repo | What it contains |
|---|---|
| **cricket-mcp** (existing) | Add `--backend onelake` flag (~50 lines in `src/db/connection.ts`). DuckDB delta + azure extensions. Server supports both local and OneLake modes. |
| **cricket-data-factory** (this repo) | PySpark ETL notebook, DataFactory MCP dataflow definitions, Pipeline config, Power BI TMDL semantic model, architecture docs. References cricket-mcp. |

---

## Steps

### 1. Set up MCP servers

**Fabric MCP Server** — install the VS Code extension:
- [Fabric MCP Server extension](https://marketplace.visualstudio.com/items?itemName=fabric.vscode-fabric-mcp-server) (or `ms-fabric.vscode-fabric-mcp-server`)
- Runs locally, provides API specs + OneLake operations

**DataFactory MCP** — add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "DataFactory.MCP": {
      "type": "stdio",
      "command": "dnx",
      "args": ["Microsoft.DataFactory.MCP", "--version", "latest", "--yes"]
    }
  }
}
```

**Power BI MCP** — already set up.

**cricket-mcp** — already set up (will add OneLake backend in step 9).

### 2. Create Lakehouse and Notebook via Fabric MCP

Use Fabric MCP Server's OneLake tools — infrastructure-as-prompt:

1. **`onelake item list`** — find the "Cricket Data Factory" workspace, confirm workspace ID
2. **`publicapis_bestpractices_itemdefinition_get`** — get the Lakehouse JSON schema to ensure correct creation payload
3. **`onelake item create`** — create `CricketLakehouse` in the workspace
4. **`onelake item create`** — create `CricketETL` notebook item
5. **`onelake directory create`** — create `/Files/raw/` directory in the lakehouse for staging

Then authenticate DataFactory MCP:
- **`authenticate_interactive`** — Entra ID login
- **`list_workspaces`** — confirm workspace ID matches

### 3. Build PySpark Notebook — "CricketETL"

Get Fabric best practices first:
- **`publicapis_bestpractices_get`** (Fabric MCP) — retrieve notebook and Spark best practices for Fabric
- **`publicapis_get`** (Fabric MCP) — get the Notebook workload API spec for reference

Downloads Cricsheet data and writes cricket-mcp's native 4-table schema as Delta tables:

- Download `https://cricsheet.org/downloads/all_json.zip` (~94 MB)
- Extract 21K+ JSON files
- Define explicit `StructType` schema for the nested Cricsheet JSON (meta, info, innings → overs → deliveries)
- Explode nested structures, parse deliveries
- Write **4 Delta tables** (matching cricket-mcp's schema):

| Table | Rows | Key columns |
|---|---|---|
| `players` | ~14K | player_id (Cricsheet registry ID), name |
| `matches` | ~21K | match_id, match_type, gender, venue, city, teams[], toss_winner, toss_decision, outcome, event_name, season, date |
| `innings` | ~50K | match_id, innings_number, batting_team, bowling_team, target_runs, target_overs, declared, forfeited, super_over |
| `deliveries` | ~10.9M | match_id, innings_number, over, ball, batter, bowler, non_striker, runs_batter, runs_extras, runs_total, extras_wides, extras_noballs, extras_byes, extras_legbyes, wicket_kind, wicket_player_out, fielder, batting_team, bowling_team |

- Run `OPTIMIZE` with V-Order + ZSTD compression across all tables

### 3b. Verify ETL output via Fabric MCP

Use Fabric MCP to inspect the lakehouse without leaving VS Code:

1. **`onelake table list`** — confirm all 4 tables exist in CricketLakehouse
2. **`onelake table get`** — inspect schema of `deliveries` table (verify all columns landed correctly)
3. **`onelake table get`** — inspect schema of `matches`, `innings`, `players`
4. **`onelake file list`** — browse the Delta files in `Tables/` directory (confirm V-Order optimization ran)

### 4. Player Enrichment via DataFactory MCP Dataflow

This is where DataFactory MCP does genuine external data ingestion:

1. **`create_dataflow`** — "PlayerEnrichment" in the workspace
2. **`save_dataflow_definition`** — M code that:
   - Fetches `https://raw.githubusercontent.com/mavaali/cricket-mcp/main/data/player_meta.csv` via `Web.Contents` + `Csv.Document`
   - Source: `cricketdata` R package → scraped from ESPNCricinfo (16K players, 11 columns)
   - Transforms column types: cricsheet_id, espn_id, name, country, batting_style, bowling_style, playing_role, DOB
   - Writes to `player_enrichment` table in Lakehouse via DataDestination (Automatic, new table)
3. **`add_connection_to_dataflow`** — Web connection + Lakehouse connection
4. **`execute_query`** — preview data before committing (validates M code from chat)
5. **`refresh_dataflow_background`** with `executeOption: "ApplyChangesIfNeeded"` (required for first refresh of API-created dataflow)

The `players` table joins with `player_enrichment` at query time (in cricket-mcp SQL and in the Power BI semantic model) to add batting/bowling style attributes.

### 5. Incremental Updates via DataFactory MCP Dataflow

Cricsheet publishes new matches daily. DataFactory MCP handles the incremental feed:

1. **`create_dataflow`** — "CricketIncrementalUpdate" in the workspace
2. **`save_dataflow_definition`** — M code that:
   - Fetches `https://cricsheet.org/downloads/recently_played_7_json.zip` via `Web.Contents`
   - Decompresses with `Binary.Decompress`
   - Parses the small batch of JSON files (~50 matches, small enough for M)
   - Appends to `matches`, `innings`, `deliveries` tables via DataDestination (Append mode)
3. **`add_connection_to_dataflow`** — Web + Lakehouse connections
4. **`refresh_dataflow_background`** — can be scheduled for daily runs

### 6. Create DataFactory MCP Pipeline — "CricketPipeline"

Orchestrates the full refresh:

```
CricketPipeline
│
├── Notebook Activity: CricketETL
│   (PySpark: download full ZIP → write 4 base tables)
│
└── Dataflow Activity: PlayerEnrichment
    (M: fetch GitHub CSV → write player_enrichment)
    (depends on notebook completing first)
```

Created via `create_pipeline` + `update_pipeline_definition`. Demonstrates:
- Multi-activity pipeline with dependencies
- Notebook + Dataflow composition
- Schedulable for periodic full rebuilds

### 7. Create Semantic Model (TMDL) via Power BI MCP tools

The semantic model adds the "virtual star schema" — relationships and measures over the 4 physical tables:

**Tables** (from Lakehouse SQL endpoint):
- `deliveries` — the fact table (10.9M rows, delivery grain)
- `matches` — match dimension
- `innings` — innings dimension
- `players` + `player_enrichment` — player dimension (joined in model)

**Relationships:**
- `deliveries.match_id` → `matches.match_id` (many-to-one)
- `deliveries.match_id + deliveries.innings_number` → `innings.match_id + innings.innings_number`
- `deliveries.batter` → `players.player_id` (active)
- `deliveries.bowler` → `players.player_id` (inactive — use via `USERELATIONSHIP`)
- `deliveries.non_striker` → `players.player_id` (inactive)
- `deliveries.wicket_player_out` → `players.player_id` (inactive)

**DAX measures** (organized into folders):

*Batting:*
- Total Runs, Batter Runs, Balls Faced (excluding wides)
- Strike Rate = Batter Runs / Balls Faced × 100
- Batting Average = Batter Runs / Dismissals (not innings)
- 100s, 50s, Boundary %, Dot Ball %

*Bowling:*
- Wickets (excluding run outs), Bowler Runs (excluding byes/legbyes)
- Economy Rate, Bowling Average, Bowling Strike Rate
- Dot Ball %, 5-wicket hauls

*Match:*
- Win %, Toss Impact, Runs per Over

### 8. Build Power BI Reports

- Player career comparison dashboards
- Format-specific breakdowns (Test vs ODI vs T20)
- Batter vs bowler matchup explorer
- Venue analysis (avg scores, bat-first win %)
- Tournament leaderboards
- Phase analysis (powerplay/middle/death)
- "GOAT" weighted scoring model

### 9. Add OneLake backend to cricket-mcp

In the cricket-mcp repo, add `--backend onelake` support:

- New `src/backends/onelake.ts` — DuckDB `delta` + `azure` extensions, Entra ID auth
- Update `src/db/connection.ts` — if `--backend onelake`, connect to `abfss://` Delta tables instead of local `.duckdb` file
- Config: `--workspace`, `--lakehouse` flags
- All 26 tools work unchanged — same SQL, same tables, different storage

---

## What each component genuinely showcases

| Component | What it demonstrates |
|---|---|
| **Fabric MCP: Item creation** | Create lakehouse + notebook from chat — infrastructure-as-prompt. No portal. |
| **Fabric MCP: OneLake inspection** | `table list`, `table get`, `file list` — verify ETL output, inspect schemas, browse files without leaving VS Code |
| **Fabric MCP: Best practices** | `publicapis_bestpractices_get` — AI agent gets Fabric-specific patterns before generating notebook code |
| **Fabric MCP: API knowledge** | `publicapis_get`, `publicapis_bestpractices_itemdefinition_get` — correct item creation payloads, API specs |
| **PySpark Notebook** | Fabric Spark integration, Delta Lake, V-Order optimization, handling 21K nested JSON files |
| **DataFactory MCP: PlayerEnrichment** | External data ingestion via MCP — `Web.Contents` → GitHub CSV → Lakehouse DataDestination. Real external source, real transform. Authored entirely from VS Code chat. |
| **DataFactory MCP: IncrementalUpdate** | Scheduled external ingestion — Cricsheet daily feed → append to Lakehouse. Keeps data fresh. |
| **DataFactory MCP: Pipeline** | Multi-step orchestration — Notebook → Dataflow with dependencies. Created and managed via MCP tools. |
| **DataFactory MCP: Connection/Workspace mgmt** | Discovery (`list_workspaces`), auth (`authenticate_interactive`), connection setup — all from VS Code. |
| **DataFactory MCP: `execute_query`** | Preview/validate M code interactively before committing to a dataflow. |
| **Power BI Semantic Model (TMDL)** | Virtual dimensional modeling — relationships + cricket-specific DAX over the raw tables. DirectLake mode. No physical star schema needed. |
| **Power BI MCP tools** | DAX query execution from VS Code — "Who's the GOAT?" answered via prompt. |
| **cricket-mcp on OneLake** | OneLake as universal data store — same data, two optimized engines, zero duplication. |

## Fabric MCP Server tools used

| Tool | Where | Purpose |
|---|---|---|
| `onelake item list` | Step 2 | Find workspace, list existing items |
| `onelake item create` | Step 2 | Create CricketLakehouse and CricketETL notebook |
| `onelake directory create` | Step 2 | Create `/Files/raw/` staging directory |
| `publicapis_bestpractices_itemdefinition_get` | Step 2 | Get Lakehouse JSON schema for correct creation payload |
| `publicapis_bestpractices_get` | Step 3 | Retrieve notebook + Spark best practices for Fabric |
| `publicapis_get` | Step 3 | Get Notebook workload API spec |
| `onelake table list` | Steps 3b, 4 (verify) | Confirm tables exist after ETL |
| `onelake table get` | Steps 3b, 4 (verify) | Inspect table schemas — verify columns landed correctly |
| `onelake file list` | Steps 3b | Browse Delta files, confirm V-Order ran |
| `onelake table namespace list` | Step 7 | Discover schemas available for semantic model |

## DataFactory MCP tools used

| Tool | Where | Purpose |
|---|---|---|
| `authenticate_interactive` | Setup | Entra ID login |
| `list_workspaces` | Setup | Find "DI Explorations" workspace ID |
| `list_capacities` | Setup | Verify workspace capacity |
| `create_dataflow` | Steps 4, 5 | Create PlayerEnrichment, IncrementalUpdate |
| `save_dataflow_definition` | Steps 4, 5 | Author complete M documents with DataDestinations |
| `add_connection_to_dataflow` | Steps 4, 5 | Bind Web + Lakehouse connections |
| `execute_query` | Steps 4, 5 | Preview M query results before saving |
| `refresh_dataflow_background` | Steps 4, 5 | Kick off refresh, get toast notification on completion |
| `refresh_dataflow_status` | Steps 4, 5 | Check refresh progress |
| `get_dataflow_definition` | Steps 4, 5 | Verify saved M code and connections |
| `list_dataflows` | Validation | Confirm dataflows exist |
| `create_pipeline` | Step 6 | Create CricketPipeline |
| `update_pipeline_definition` | Step 6 | Define Notebook → Dataflow activity chain |
| `list_pipelines` | Validation | Confirm pipeline exists |
| `list_connections` | Steps 4, 5 | Find Web and Lakehouse connection IDs |

---

## Step-by-step flow with MCP server attribution

| # | Action | MCP Server | Tool(s) |
|---|---|---|---|
| 1 | Get Fabric best practices for lakehouse creation | Fabric MCP | `publicapis_bestpractices_get`, `publicapis_bestpractices_itemdefinition_get` |
| 2 | Create CricketLakehouse | Fabric MCP | `onelake item create` |
| 3 | Create CricketETL notebook item | Fabric MCP | `onelake item create` |
| 4 | Create staging directory | Fabric MCP | `onelake directory create` |
| 5 | Get PySpark/notebook best practices | Fabric MCP | `publicapis_bestpractices_get`, `publicapis_get` |
| 6 | Auth to Fabric for data operations | DataFactory MCP | `authenticate_interactive` |
| 7 | Find workspace, discover connections | DataFactory MCP | `list_workspaces`, `list_connections` |
| 8 | Run PySpark notebook (bulk JSON ETL) | *(manual or pipeline)* | — |
| 9 | Verify tables created | Fabric MCP | `onelake table list`, `onelake table get` |
| 10 | Browse lakehouse Delta files | Fabric MCP | `onelake file list` |
| 11 | Create PlayerEnrichment dataflow | DataFactory MCP | `create_dataflow`, `save_dataflow_definition` |
| 12 | Bind connections to dataflow | DataFactory MCP | `add_connection_to_dataflow` |
| 13 | Preview M query results | DataFactory MCP | `execute_query` |
| 14 | Refresh dataflow | DataFactory MCP | `refresh_dataflow_background` |
| 15 | Verify enrichment table | Fabric MCP | `onelake table list`, `onelake table get` |
| 16 | Create incremental update dataflow | DataFactory MCP | `create_dataflow`, `save_dataflow_definition` |
| 17 | Create pipeline (notebook → dataflows) | DataFactory MCP | `create_pipeline`, `update_pipeline_definition` |
| 18 | Create semantic model (TMDL) | Power BI MCP | *(TMDL creation + DAX measures)* |
| 19 | Query: "Who's the GOAT?" | Power BI MCP | `execute_dax` |
| 20 | Query: "Kohli vs Hazlewood in ODIs?" | cricket-mcp | `get_matchup` |

---

## Verification

| Step | Check | Expected |
|---|---|---|
| After step 3 | Query `SELECT COUNT(*) FROM deliveries` via SQL endpoint | ~10.9M rows |
| After step 3 | Query `SELECT COUNT(*) FROM matches` | ~21K rows |
| After step 4 | Query `SELECT COUNT(*) FROM player_enrichment` | ~16K rows with batting_style populated |
| After step 5 | Run incremental refresh, check new match count | Matches added since last full load |
| After step 7 | DAX query via Power BI MCP: Kohli batting average in ODIs | ~58 |
| After step 9 | `npx tsx src/index.ts serve --backend onelake` — ask "Kohli vs Hazlewood in ODIs?" | Same results as local DuckDB |

## Reference Sources

- **cricket-mcp**: https://github.com/mavaali/cricket-mcp — 26 tools, 4-table schema (players/matches/innings/deliveries), DuckDB, Cricsheet data
- **Fabric MCP Server**: https://github.com/microsoft/mcp/tree/main/servers/Fabric.Mcp.Server — OneLake operations, Fabric API specs, item definitions, best practices
- **DataFactory MCP**: https://github.com/Microsoft/DataFactory.MCP — Dataflow Gen2, Pipeline management, connection management, workspace discovery
- **Substack article**: https://tpeplow.substack.com/p/from-json-to-goat-building-a-cricket — PySpark ETL, TMDL semantic model, Power BI MCP, GOAT analysis
- **Cricsheet data**: https://cricsheet.org/ — 21K+ matches, ball-by-ball JSON, free and open
- **Player profiles**: `cricketdata` R package (https://github.com/robjhyndman/cricketdata) → ESPNCricinfo scraper → 16K players with batting/bowling style metadata
