# Cricket Analytics — Schema & MCP Tool Compatibility Analysis

## The Core Tension

cricket-mcp has 26 analytical tools with SQL queries written against a specific 4-table schema. A star schema (optimized for Power BI) has different tables, columns, and join patterns. Changing the schema means rewriting every SQL query. This document analyzes the options.

## cricket-mcp's Current Schema (4 tables)

```
players      (player_id, name, batting_style, bowling_style, playing_role, country)
matches      (match_id, match_type, gender, venue, city, teams, toss, outcome, event_name, season, ...)
innings      (match_id, innings_number, batting_team, bowling_team, target_runs, ...)
deliveries   (match_id, innings_number, over, ball, batter, bowler, non_striker, runs_batter,
              runs_extras, runs_total, extras_wides, extras_noballs, wicket_kind, ...)
```

## Star Schema for Power BI (8 tables)

```
fact_deliveries  →  dim_match, dim_innings, dim_player, dim_team, dim_venue, dim_date, dim_event
```

## The Trade-off

| Goal | Requires |
|---|---|
| All 26 cricket-mcp tools work unchanged | cricket-mcp's 4-table schema |
| Showcase DataFactory MCP with many dataflows | Star schema (more tables = more dataflows to create) |
| Power BI DirectLake optimized | Star schema preferred |

**You can't maximize all three simultaneously.**

---

## Option 1: cricket-mcp's 4-Table Schema (MCP Tools Win)

```
PySpark Notebook → Lakehouse (4 tables: players, matches, innings, deliveries)
                        ↓                              ↓
                  cricket-mcp (unchanged)        Power BI semantic model
                                                 (adds star-schema-like
                                                  relationships + DAX on top)
```

- All 26 tools work unchanged
- Power BI works fine — semantic models can add relationships + DAX over any schema, DirectLake still works
- DataFactory MCP does: player CSV dataflow, pipeline orchestration, incremental updates

| Criteria | Rating |
|---|---|
| MCP tool compatibility | ✅ Full (26/26 unchanged) |
| Power BI optimization | ⚠️ Good (DirectLake works, schema not ideal for DAX) |
| DataFactory MCP showcase | ❌ Modest (1 dataflow, 1 pipeline) |
| Rewrite effort | None |

---

## Option 2: Both Schemas (Everything Wins) ⭐ Recommended

```
PySpark Notebook → Lakehouse:
                     ├── 4 tables (players, matches, innings, deliveries)  ← cricket-mcp reads these
                     └── 8 tables (star schema: fact_ + dim_)              ← Power BI reads these
                                      ↑
                              DataFactory MCP Dataflows
                              (transform 4-table → star schema)
```

This is a **standard medallion-like pattern**: staging → analytical → presentation.

### How it works

| Layer | Tables | Written by | Read by |
|---|---|---|---|
| Analytical | `players`, `matches`, `innings`, `deliveries` | PySpark Notebook | cricket-mcp (DuckDB via OneLake) |
| Presentation | `fact_deliveries`, `dim_match`, `dim_innings`, `dim_player`, `dim_team`, `dim_venue`, `dim_date`, `dim_event` | DataFactory MCP Dataflows | Power BI (DirectLake) |

### DataFactory MCP showcase

| Dataflow | Source → Transform → Target |
|---|---|
| `DF_DimMatch` | `matches` → extract metadata, deduplicate → `dim_match` |
| `DF_DimPlayer` | `players` → distinct registry → `dim_player` |
| `DF_DimVenue` | `matches` → extract venue/city → distinct → `dim_venue` |
| `DF_DimEvent` | `matches` → extract tournament/season/stage → `dim_event` |
| `DF_DimDate` | `matches` → generate calendar from date range → `dim_date` |
| `DF_DimTeam` | `matches` + `innings` → extract teams, classify international vs club → `dim_team` |
| `DF_DimInnings` | `innings` → add surrogate keys → `dim_innings` |
| `DF_FactDeliveries` | `deliveries` → surrogate keys, join dim keys → `fact_deliveries` |
| `DF_PlayerEnrichment` | `Web.Contents` (GitHub CSV) → type transforms → merge into `dim_player` |
| `DF_IncrementalUpdate` | `Web.Contents` (Cricsheet daily ZIP) → append to base tables |

**10 dataflows** using: `create_dataflow`, `save_dataflow_definition`, `add_connection_to_dataflow`, `refresh_dataflow_background`, `execute_query`, DataDestination config, `AllowCombine` for multi-source.

### Pipeline

```
CricketPipeline (create_pipeline + update_pipeline_definition)
│
├── Notebook Activity: CricketETL_Staging
│   (PySpark: download ZIP → flatten JSON → write 4 base tables)
│
├── Parallel Dataflow Activities:
│   ├── DF_DimMatch
│   ├── DF_DimPlayer
│   ├── DF_DimVenue
│   ├── DF_DimEvent
│   ├── DF_DimDate
│   ├── DF_DimTeam
│   └── DF_DimInnings
│
├── Sequential (depends on dimensions):
│   └── DF_FactDeliveries
│
└── Final:
    └── DF_PlayerEnrichment
```

| Criteria | Rating |
|---|---|
| MCP tool compatibility | ✅ Full (26/26 unchanged against base tables) |
| Power BI optimization | ✅ Optimal (star schema, DirectLake) |
| DataFactory MCP showcase | ✅ Strong (10 dataflows, multi-stage pipeline) |
| Rewrite effort | None for MCP tools; DataFactory creates the star schema |
| Trade-off | ~2x storage (mitigated by Delta compression, ~1-2 GB total) |

---

## Option 3: Star Schema Only, Rewrite MCP Queries

```
PySpark Notebook → stg_matches_raw (staging)
                        ↓
              DataFactory MCP Dataflows → Lakehouse (8 star schema tables)
                                               ↓
                              ┌─────────────────┴───────────────────┐
                              ↓                                     ↓
                        Power BI                              cricket-mcp
                        (DirectLake + DAX)                    (all 26 SQL queries rewritten)
```

| Criteria | Rating |
|---|---|
| MCP tool compatibility | ⚠️ All 26 queries need porting to new schema |
| Power BI optimization | ✅ Optimal |
| DataFactory MCP showcase | ✅ Strong |
| Rewrite effort | **High** — every tool's SQL must change, cricket logic bugs likely |

---

## Recommendation: Option 2

**Two schemas, one lakehouse, zero MCP rewrites.**

The DataFactory MCP dataflows doing 4-table → star schema are genuine, useful transforms — not make-work. This is a standard dimensional modeling pattern: analytical layer (flat, query-optimized) feeds a presentation layer (star schema, visualization-optimized).

Storage cost is negligible — Delta on OneLake, ZSTD compressed, ~1-2 GB total for both schemas.

DataFactory MCP's role upgrades from "glorified scheduler" to **"dimensional modeling engine"** — reading the analytical tables and producing optimized star schema tables with proper surrogate keys, calendar dimensions, and clean relationships. All authored and managed through MCP tools.
