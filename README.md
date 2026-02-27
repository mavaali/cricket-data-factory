# Cricket Data Factory

Build a cricket analytics pipeline in Microsoft Fabric using six MCP servers — from raw JSON to enterprise dashboards, entirely from VS Code.

## What This Is

[cricket-mcp](https://github.com/mavaali/cricket-mcp) is an MCP server that gives AI agents access to 10.9 million ball-by-ball cricket deliveries from [Cricsheet](https://cricsheet.org/). It runs locally on DuckDB with 26 analytical tools — matchups, phase stats, career analysis, tournament leaderboards, and more.

This project extends cricket-mcp into Microsoft Fabric for enterprise use: shared Power BI dashboards, governed data access, scheduled refreshes, and team collaboration. Six MCP servers work together in a single VS Code session — zero portal clicks.

Inspired by [From JSON to GOAT](https://tpeplow.substack.com/p/from-json-to-goat-building-a-cricket) by Tom Peplow.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    VS Code + Copilot                                         │
│                                                                                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ ┌───────────┐ │
│  │ Fabric MCP  │ │ Fabric      │ │ DataFactory │ │ Power BI    │ │ Power BI  │ │ cricket-  │ │
│  │ Server      │ │ Analytics   │ │ MCP         │ │ Modeling    │ │ MCP       │ │ mcp       │ │
│  │             │ │ MCP         │ │             │ │ MCP         │ │           │ │           │ │
│  │ • Create    │ │ • Create    │ │ • Dataflows │ │ • Tables    │ │ • DAX     │ │ • 26 query│ │
│  │   lakehouse │ │   notebooks │ │ • Pipelines │ │ • Measures  │ │   queries │ │   tools   │ │
│  │ • Upload    │ │ • Execute   │ │ • M code    │ │ • Relations │ │ • Semantic│ │ • Matchups│ │
│  │   files     │ │   notebooks │ │ • Connect-  │ │ • TMDL      │ │   model   │ │ • Stats   │ │
│  │ • Inspect   │ │ • Livy      │ │   ions      │ │   import/   │ │   schema  │ │ • Records │ │
│  │   tables    │ │   sessions  │ │ • Refresh   │ │   export    │ │ • Natural │ │ • Career  │ │
│  │ • Best      │ │ • Monitor   │ │             │ │ • Deploy to │ │   language│ │   trends  │ │
│  │   practices │ │   Spark     │ │             │ │   Fabric    │ │           │ │           │ │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └─────┬─────┘ └─────┬─────┘ │
│         │               │               │               │              │             │       │
└─────────┼───────────────┼───────────────┼───────────────┼──────────────┼─────────────┼───────┘
          │               │               │               │              │             │
          ▼               ▼               ▼               ▼              ▼             ▼
    ┌──────────────────────────────────────────────────────────────────────────────────────┐
    │                            OneLake (CricketLakehouse)                                │
    │         players | matches | innings | deliveries | player_enrichment                 │
    └──────────────────────────────────────────────────────────────────────────────────────┘
```

**Key design decision:** The Power BI semantic model IS the star schema — no physical dimension tables needed. Relationships and DAX measures are defined in TMDL on top of the 4-table schema. This means cricket-mcp's 26 SQL tools work unchanged against the same data Power BI uses.

**Two consumers, one copy of data:**
- **cricket-mcp** — DuckDB reads Delta tables from OneLake via the `delta` + `azure` extensions. All 26 tools work unchanged.
- **Power BI** — DirectLake mode reads the same Delta files. Virtual star schema via TMDL relationships.

## Six MCP Servers

| Server | Role | What it does in this project |
|---|---|---|
| [**Fabric MCP Server**](https://github.com/microsoft/mcp/tree/main/servers/Fabric.Mcp.Server) | Infrastructure + inspection | Create lakehouse items, upload files, inspect table schemas, browse OneLake files, retrieve Fabric API specs and best practices |
| [**Fabric Analytics MCP**](https://github.com/santhoshravindran7/Fabric-Analytics-MCP) | Notebook lifecycle + execution | Create notebooks with code content, update definitions, execute notebooks, Livy Spark sessions, monitor Spark applications |
| [**DataFactory MCP**](https://github.com/Microsoft/DataFactory.MCP) | Data movement + orchestration | Author M code dataflows for external data ingestion, build multi-step pipelines, manage connections, monitor refresh status |
| [**Power BI MCP**](https://learn.microsoft.com/en-us/power-bi/developer/mcp/mcp-servers-overview) | Semantic querying + analytics | Execute DAX queries from VS Code, get semantic model schemas, generate DAX from natural language |
| [**Power BI Modeling MCP**](https://github.com/microsoft/powerbi-modeling-mcp) | Semantic model authoring | Create/modify tables, columns, measures, relationships in Power BI semantic models via natural language. TMDL import/export, deploy to Fabric |
| [**cricket-mcp**](https://github.com/mavaali/cricket-mcp) | Domain-specific analytics | 26 parameterized SQL tools for cricket statistics — matchups, phase analysis, career trends, tournament leaderboards |

## Prerequisites

- [VS Code](https://code.visualstudio.com/) with [GitHub Copilot](https://code.visualstudio.com/docs/copilot/overview)
- [Fabric MCP Server extension](https://marketplace.visualstudio.com/items?itemName=fabric.vscode-fabric-mcp-server)
- [.NET 10+](https://dotnet.microsoft.com/download) (for DataFactory MCP via `dnx`)
- A Microsoft Fabric workspace with capacity
- [Node.js 18+](https://nodejs.org/) (for cricket-mcp)

## Setup

### 1. Configure MCP servers

Add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "DataFactory.MCP": {
      "type": "stdio",
      "command": "dnx",
      "args": ["Microsoft.DataFactory.MCP@0.16.0-beta", "--yes", "--", "mcp", "start", "--", "--pipeline", "--dataflow-query"]
    },
    "fabric-analytics": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "mcp-for-microsoft-fabric-analytics"],
      "env": { "FABRIC_AUTH_METHOD": "azure_cli" }
    },
    "powerbi-remote": {
      "type": "http",
      "url": "https://api.fabric.microsoft.com/v1/mcp/powerbi"
    }
  }
}
```

The Fabric MCP Server is configured automatically by its VS Code extension.
Fabric Analytics MCP uses Azure CLI auth — run `az login` first.

### 2. Create Lakehouse via Fabric MCP

Use the Fabric MCP Server's OneLake tools — infrastructure-as-prompt:

1. `onelake item list` — list workspaces, find workspace ID
2. `onelake item create` — create `CricketLakehouse` (Lakehouse item)
3. `onelake directory create` — create `Files/raw/` staging directory
4. `onelake upload file` — upload the PySpark notebook to lakehouse

### 3. Deploy and Run the PySpark Notebook — CricketETL

The notebook code is deployed and executed programmatically via two APIs:

**Deploy code** — Fabric REST API `updateDefinition`:
1. Convert `.py` to `.ipynb` format (`scripts/convert_to_ipynb.py`)
2. Base64 encode and POST to `/notebooks/{id}/updateDefinition`

**Execute code** — Fabric Livy API (interactive Spark sessions):
1. Create a Livy session on the lakehouse
2. Submit code cells as statements via `scripts/run_livy.py`
3. Poll each statement for output/errors
4. Real-time feedback — see exactly which cell fails and why

```bash
# Deploy notebook definition
python3 scripts/convert_to_ipynb.py
# (then curl to updateDefinition API)

# Execute via Livy
python3 scripts/run_livy.py imports
python3 scripts/run_livy.py download    # Downloads 94MB ZIP
python3 scripts/run_livy.py parse       # Parses 21K JSON files
python3 scripts/run_livy.py players     # Write players table
python3 scripts/run_livy.py matches     # Write matches table
python3 scripts/run_livy.py innings     # Write innings table
python3 scripts/run_livy.py deliveries  # Write 10.9M row deliveries table
python3 scripts/run_livy.py optimize    # OPTIMIZE with V-Order
python3 scripts/run_livy.py validate    # Validation queries
python3 scripts/run_livy.py enrich      # Merge player_enrichment → players
```

The notebook ([`notebooks/CricketETL.py`](notebooks/CricketETL.py)) downloads [Cricsheet](https://cricsheet.org/) data and writes cricket-mcp's native 4-table schema as Delta tables:

| Table | ~Rows | Description |
|---|---|---|
| `players` | 14K | Player registry — Cricsheet ID, name (enrichment columns initially NULL) |
| `matches` | 21K | Match metadata — format, gender, teams, venue, toss, outcome, event, season |
| `innings` | 50K | Innings-level — batting/bowling team, target, declared/forfeited/super over |
| `deliveries` | 10.9M | Ball-by-ball — batter, bowler, runs, extras (broken out), wicket details, fielders |

The schema matches [cricket-mcp's DuckDB schema](https://github.com/mavaali/cricket-mcp/blob/main/src/db/schema.ts) exactly — same table names, same column names, same types. This is what allows all 26 cricket-mcp tools to work unchanged against OneLake.

After writing, the notebook runs `OPTIMIZE` with V-Order compression across all four tables.

### 4. Player Enrichment via DataFactory MCP

DataFactory MCP creates a Dataflow Gen2 that ingests player profile data from an external source:

- M code fetches [`player_meta.csv`](https://raw.githubusercontent.com/mavaali/cricket-mcp/main/data/player_meta.csv) from GitHub via `Web.Contents` + `Csv.Document`
- Source: [cricketdata R package](https://github.com/robjhyndman/cricketdata) → ESPNCricinfo (16K players)
- Columns: cricsheet_id, espn_id, name, country, batting_style, bowling_style, playing_role, DOB
- Writes to `player_enrichment` table via Lakehouse DataDestination

The `players` table joins with `player_enrichment` at query time to add batting/bowling style attributes.

### 5. Incremental Updates via DataFactory MCP

Cricsheet publishes new matches daily. A second dataflow fetches `recently_played_7_json.zip`, parses the small batch (~50 matches), and appends to the base tables. Schedulable for daily runs.

### 6. Pipeline Orchestration

```
CricketPipeline
│
├── Notebook Activity: CricketETL
│   (PySpark: download full ZIP → write 4 base tables)
│
└── Dataflow Activity: PlayerEnrichment
    (M: fetch GitHub CSV → write player_enrichment)
```

Created via DataFactory MCP's `create_pipeline` + `update_pipeline_definition`.

### 7. Semantic Model (TMDL) via Power BI MCP

The semantic model adds a "virtual star schema" over the 4 physical tables:

**Relationships:**
- `deliveries.match_id` → `matches.match_id`
- `deliveries` → `innings` (composite key)
- `deliveries.batter` → `players.player_id` (active)
- `deliveries.bowler` → `players.player_id` (inactive, via `USERELATIONSHIP`)

**Cricket-specific DAX measures:**
- Batting Average = Runs / Dismissals (not innings)
- Balls Faced excludes wides
- Bowler Runs exclude byes and legbyes
- Bowling Wickets exclude run outs
- Strike Rate, Economy Rate, Boundary %, Dot Ball %

### 8. Power BI Reports

- Player career comparison dashboards
- Format-specific breakdowns (Test vs ODI vs T20)
- Batter vs bowler matchup explorer
- Venue analysis, tournament leaderboards
- "GOAT" weighted scoring model

### 9. OneLake Backend for cricket-mcp

```bash
# Local mode (default)
npx tsx src/index.ts serve

# OneLake mode
npx tsx src/index.ts serve --backend onelake \
  --workspace "Cricket Data Factory" \
  --lakehouse "CricketLakehouse"
```

DuckDB `delta` + `azure` extensions with Entra ID auth. All 26 tools work unchanged — same SQL, same tables, different storage.

## MCP Server Usage Map

| # | Action | MCP Server | Tool(s) |
|---|---|---|---|
| 1 | Get Fabric best practices | Fabric MCP | `publicapis_bestpractices_get` |
| 2 | Create CricketLakehouse | Fabric MCP | `onelake item create` |
| 3 | Create staging directory | Fabric MCP | `onelake directory create` |
| 4 | Upload notebook | Fabric MCP | `onelake upload file` |
| 5 | Auth to Fabric | DataFactory MCP | `authenticate_interactive` |
| 6 | Find workspace, connections | DataFactory MCP | `list_workspaces`, `list_connections` |
| 7 | Deploy notebook code | Fabric REST API | `updateDefinition` (base64 ipynb) |
| 8 | Create Livy session | Fabric Livy API | `POST .../sessions` on lakehouse |
| 9 | Execute cells interactively | Fabric Livy API | `scripts/run_livy.py` (submit + poll per cell) |
| 10 | Verify tables | Fabric MCP | `onelake table list`, `onelake table get` |
| 11 | Create player enrichment dataflow | DataFactory MCP | `create_dataflow`, `save_dataflow_definition` |
| 12 | Preview M query results | DataFactory MCP | `execute_query` |
| 13 | Refresh dataflow | DataFactory MCP | `refresh_dataflow_background` |
| 14 | Create incremental dataflow | DataFactory MCP | `create_dataflow`, `save_dataflow_definition` |
| 15 | Create pipeline | DataFactory MCP | `create_pipeline`, `update_pipeline_definition` |
| 16 | Create semantic model | Power BI Modeling MCP | `database_operations`, `table_operations`, `measure_operations`, `relationship_operations` |
| 17 | "Who's the GOAT?" | Power BI MCP | `ExecuteQuery` (DAX) |
| 18 | "Kohli vs Hazlewood?" | cricket-mcp | `get_matchup` |

## Data Sources

- **[Cricsheet](https://cricsheet.org/)** — 21K+ matches, ball-by-ball JSON, free and open
- **[cricketdata R package](https://github.com/robjhyndman/cricketdata)** → Player metadata from ESPNCricinfo (16K players)

## Related

- [cricket-mcp](https://github.com/mavaali/cricket-mcp) — The MCP server this project extends
- [Fabric MCP Server](https://github.com/microsoft/mcp/tree/main/servers/Fabric.Mcp.Server) — OneLake operations + Fabric API knowledge
- [Fabric Analytics MCP](https://github.com/santhoshravindran7/Fabric-Analytics-MCP) — Notebook lifecycle, Spark execution, monitoring
- [DataFactory MCP](https://github.com/Microsoft/DataFactory.MCP) — Dataflow Gen2 + Pipeline management
- [Power BI MCP](https://learn.microsoft.com/en-us/power-bi/developer/mcp/mcp-servers-overview) — DAX query execution
- [Power BI Modeling MCP](https://github.com/microsoft/powerbi-modeling-mcp) — Semantic model authoring (tables, measures, relationships)
- [From JSON to GOAT](https://tpeplow.substack.com/p/from-json-to-goat-building-a-cricket) — Inspiration for this project

## License

MIT
