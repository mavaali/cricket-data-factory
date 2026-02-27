# Cricket Analytics Pipeline — Architecture Options

## Context

We want to build a Microsoft Fabric equivalent of [cricket-mcp](https://github.com/mavaali/cricket-mcp) — a cricket stats MCP server with 26 analytical tools powered by 10.9M ball-by-ball deliveries from [Cricsheet](https://cricsheet.org/). The goal is to bring this data into OneLake, enable Power BI visualization (inspired by [this Substack article](https://tpeplow.substack.com/p/from-json-to-goat-building-a-cricket)), and use the [DataFactory MCP](https://github.com/Microsoft/DataFactory.MCP) for data ingestion.

### Two Consumers with Different Needs

| Consumer | Needs | Optimal engine |
|---|---|---|
| **Cricket MCP tools** (26 parameterized SQL queries, sub-second response, ad-hoc slicing) | Fast columnar SQL with DuckDB-style features (QUALIFY, FILTER, list agg) | DuckDB |
| **Power BI dashboards** (visual analytics, sharing, DAX measures, drill-down) | DirectLake for in-memory speed, DAX for complex calculations | Lakehouse + Power BI semantic model |

---

## Architecture A: Full Fabric-Native

```
Cricsheet JSON → PySpark Notebook → Lakehouse (7 Delta tables)
                                          ↓
                                    SQL Endpoint (T-SQL)
                                          ↓
                            ┌─────────────┴─────────────┐
                            ↓                           ↓
                      Power BI                   New cricket-mcp
                      (DirectLake + DAX)         (rewritten for T-SQL)
```

**Pros:**
- Single source of truth, single platform
- DirectLake = fastest Power BI mode (reads Delta files directly, no import/DirectQuery overhead)
- Everything managed in Fabric

**Cons:**
- Must rewrite 26 cricket-mcp SQL queries from DuckDB → T-SQL (Lakehouse SQL endpoint is read-only T-SQL subset)
- T-SQL lacks DuckDB features: `QUALIFY`, `FILTER(WHERE)`, `list()` aggregation, structural types
- Cricket-specific logic bugs are likely during porting
- SQL endpoint is read-only — no stored procs, no views with complex logic
- **Estimated effort: High** (rewriting + debugging all 26 tools)

---

## Architecture B: DuckDB Reads from OneLake ⭐ Recommended

```
Cricsheet JSON → PySpark Notebook → Lakehouse (7 Delta tables in OneLake)
                                          ↓
                            ┌─────────────┴─────────────┐
                            ↓                           ↓
                      Power BI                   cricket-mcp
                      (DirectLake + DAX)         (DuckDB reads Delta from OneLake)
```

**How it works:**
- DuckDB has a `delta` extension that reads Delta Lake tables natively
- OneLake exposes files via `https://onelake.dfs.fabric.microsoft.com/` (ADLS Gen2 compatible)
- DuckDB's `azure` extension can authenticate with Entra ID and read directly
- cricket-mcp's 26 tools work **unchanged** against the same physical data Power BI uses
- **Single source of truth, two optimized query engines**

**Pros:**
- Zero rewrites to cricket-mcp tools — they just point at OneLake Delta tables instead of local .duckdb file
- DirectLake for Power BI = fastest visualization
- One copy of data in OneLake
- DuckDB's query performance on 10.9M rows is milliseconds (it's literally designed for this)
- Incremental updates only need to happen once (in Lakehouse), both consumers see it immediately

**Cons:**
- DuckDB reading from OneLake adds network latency vs local file (mitigated by DuckDB's aggressive caching)
- Need to manage DuckDB Azure auth (Entra ID token)
- Less "pure Fabric" — DuckDB runs locally
- **Estimated effort: Low** (swap connection string, test)

---

## Architecture C: Power BI IS the Analytics Engine

```
Cricsheet JSON → PySpark Notebook → Lakehouse (7 Delta tables)
                                          ↓
                                    Power BI Semantic Model
                                    (DAX measures = reimplemented 26 tools)
                                          ↓
                            ┌─────────────┴─────────────┐
                            ↓                           ↓
                      Power BI Reports          Power BI MCP tools
                      (dashboards)              (execute_dax for ad-hoc queries)
```

This is what the [Substack article](https://tpeplow.substack.com/p/from-json-to-goat-building-a-cricket) did. All analytics through DAX.

**Pros:**
- Pure Fabric + Power BI stack
- DAX is actually powerful for cricket analytics — `CALCULATE` + filter context maps well to "Kohli's average against left-arm pace in ODIs while chasing"
- Power BI MCP tools let you run DAX queries from VS Code
- Shareable semantic model — anyone in the org can query

**Cons:**
- Must reimplement 26 tools as DAX measure libraries (significant but doable)
- DAX has a steep learning curve for some patterns (recursive CTEs, complex window functions)
- Some cricket-mcp tools are hard in DAX: `get_what_if` (counterfactual exclusion), `get_emerging_players` (trend detection vs baseline), `get_partnerships` (consecutive ball sequences)
- DirectLake has a fallback-to-DirectQuery behavior on complex queries that can be slow
- **Estimated effort: Medium-High** (DAX is powerful but different paradigm)

---

## Architecture D: Keep Both, Sync Periodically

```
Cricsheet JSON → cricket-mcp (local DuckDB, 26 tools) ← for MCP queries
                    ↓ (export as Parquet)
              DataFactory MCP Pipeline → Lakehouse → Power BI (DirectLake)
```

**Pros:** Simplest, no rewrites

**Cons:** Two copies of data, sync complexity, not really a "Fabric-native" solution

---

## Architecture E: Eventhouse (Dark Horse)

```
Cricsheet JSON → PySpark Notebook → Eventhouse (KQL Database)
                                          ↓
                            ┌─────────────┴─────────────┐
                            ↓                           ↓
                      Power BI                   cricket-mcp
                      (DirectQuery to KQL)       (adapted to KQL HTTP API)
```

**Why consider it:**
- KQL is genuinely excellent for this exact workload (time-series aggregations, ad-hoc slicing, MVEXPAND for nested data)
- Eventhouse has a free tier in Fabric
- Sub-second on 10.9M rows
- Real-time ingestion API for incremental updates

**Why probably not:**
- No DirectLake (Power BI uses DirectQuery → slower)
- Must rewrite 26 tools from SQL to KQL
- Overkill for batch-updated cricket data
- **Estimated effort: High**

---

## Comparison Matrix

| | A: Full Fabric | **B: DuckDB + OneLake** | C: DAX-only | D: Sync | E: Eventhouse |
|---|---|---|---|---|---|
| Power BI speed | DirectLake | DirectLake | DirectLake | DirectLake | DirectQuery |
| Cricket MCP tools | Rewrite (T-SQL) | **Unchanged** | Rewrite (DAX) | Unchanged | Rewrite (KQL) |
| Single source of truth | Yes | **Yes** | Yes | No | Yes |
| Rewrite effort | High | **Low** | Medium-High | None | High |
| Pure Fabric? | Yes | Mostly | Yes | No | Yes |
| Incremental updates | Once | **Once** | Once | Twice | Once |

---

## Recommendation: Architecture B

**Lakehouse IS the right storage layer** — Delta format, DirectLake support, OneLake as universal store.

**Power BI IS the right visualization layer** — DirectLake mode is unmatched for interactive dashboards.

**DuckDB reading Delta from OneLake** is the key insight — it gives us the proven cricket-mcp tools without rewrites, pointing at the same physical data Power BI uses.

The DataFactory MCP role: orchestrating the PySpark notebook, managing the player CSV dataflow, handling connections and pipelines.

Instead of trying to replicate cricket-mcp's 26 tools as DAX measures, we **adapt cricket-mcp to read from OneLake and keep its SQL queries intact**. Then build Power BI dashboards with a focused set of DAX measures for the visual layer.

### Schema (all architectures)

- 1 fact table: `fact_deliveries` — 1 row per ball (10.9M+ rows)
- 7 dimension tables: `dim_match`, `dim_innings`, `dim_player`, `dim_team`, `dim_venue`, `dim_date`, `dim_event`
- Player enrichment via Dataflow Gen2 (batting/bowling style from [cricketdata R package](https://github.com/robjhyndman/cricketdata) → ESPNCricinfo)
- Key columns on fact table: over_number, ball_number, non_striker_id, wicket_kind, wicket_player_out_id, fielder_id, runs_batter, runs_extras, extras breakdown (wide/noball/bye/legbye), batting_team, bowling_team

### What This Enables

All 26 cricket-mcp tools (matchups, phase stats, situational analysis, GOAT queries, etc.) plus Power BI dashboards — single copy of data, two optimized query engines, minimal rework.
