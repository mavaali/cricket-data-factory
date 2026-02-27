# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MARKDOWN ********************

# # CricketETL — Cricsheet to Lakehouse
# 
# Downloads ball-by-ball cricket data from [Cricsheet](https://cricsheet.org/) and writes 4 Delta tables
# matching the [cricket-mcp](https://github.com/mavaali/cricket-mcp) schema:
# 
# | Table | ~Rows | Description |
# |---|---|---|
# | `players` | 14K | Player registry (Cricsheet ID → name) |
# | `matches` | 21K | Match metadata (format, teams, venue, outcome) |
# | `innings` | 50K | Innings-level data (batting/bowling team, target) |
# | `deliveries` | 10.9M | Ball-by-ball (batter, bowler, runs, extras, wickets) |

# CELL ********************

# PARAMETERS
CRICSHEET_URL = "https://cricsheet.org/downloads/all_json.zip"
LAKEHOUSE_PATH = "Tables"

# CELL ********************

import json
import os
import zipfile
import urllib.request
import tempfile
from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Start time: {datetime.now().isoformat()}")

# CELL ********************

# MARKDOWN ********************

# ## Step 1: Download and extract Cricsheet data

# CELL ********************

# Download the ZIP file
tmp_dir = tempfile.mkdtemp()
zip_path = os.path.join(tmp_dir, "all_json.zip")
extract_dir = os.path.join(tmp_dir, "json_files")

print(f"Downloading from {CRICSHEET_URL}...")
urllib.request.urlretrieve(CRICSHEET_URL, zip_path)
zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
print(f"Download complete: {zip_size_mb:.1f} MB")

# Extract
os.makedirs(extract_dir, exist_ok=True)
with zipfile.ZipFile(zip_path, 'r') as zf:
    json_files = [f for f in zf.namelist() if f.endswith('.json')]
    zf.extractall(extract_dir, members=json_files)

print(f"Extracted {len(json_files)} JSON files")

# CELL ********************

# MARKDOWN ********************

# ## Step 2: Parse JSON files into structured rows
# 
# Each JSON file = 1 match. We parse into 4 sets of rows:
# - players (from registry.people)
# - matches (from info section)
# - innings (from innings section)
# - deliveries (from innings → overs → deliveries)

# CELL ********************

# Accumulators for all tables
all_players = {}  # {player_id: name} — deduplicated across all matches
all_matches = []
all_innings = []
all_deliveries = []

error_files = []
processed = 0

for json_file in json_files:
    file_path = os.path.join(extract_dir, json_file)
    # Match ID from filename (e.g., "1234567.json" → "1234567")
    match_id = os.path.splitext(os.path.basename(json_file))[0]
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        info = data.get('info', {})
        meta = data.get('meta', {})
        
        # --- PLAYERS (from registry) ---
        registry = info.get('registry', {}).get('people', {})
        for name, player_id in registry.items():
            if player_id not in all_players:
                all_players[player_id] = name
        
        # --- MATCH ---
        teams = info.get('teams', [])
        outcome = info.get('outcome', {})
        outcome_by = outcome.get('by', {})
        event = info.get('event', {})
        toss = info.get('toss', {})
        dates = info.get('dates', [])
        
        match_row = {
            'match_id': match_id,
            'data_version': meta.get('data_version'),
            'match_type': info.get('match_type'),
            'match_type_number': info.get('match_type_number'),
            'gender': info.get('gender'),
            'team_type': info.get('team_type'),
            'overs_per_side': info.get('overs'),
            'balls_per_over': info.get('balls_per_over', 6),
            'venue': info.get('venue'),
            'city': info.get('city'),
            'date_start': dates[0] if dates else None,
            'date_end': dates[-1] if dates else None,
            'team1': teams[0] if len(teams) > 0 else None,
            'team2': teams[1] if len(teams) > 1 else None,
            'toss_winner': toss.get('winner'),
            'toss_decision': toss.get('decision'),
            'outcome_winner': outcome.get('winner'),
            'outcome_result': outcome.get('result'),
            'outcome_method': outcome.get('method'),
            'outcome_by_runs': outcome_by.get('runs'),
            'outcome_by_wickets': outcome_by.get('wickets'),
            'outcome_by_innings': outcome_by.get('innings'),
            'player_of_match': ','.join(info.get('player_of_match', [])),
            'event_name': event.get('name'),
            'event_match_number': event.get('match_number'),
            'event_group': str(event.get('group', '')) if event.get('group') is not None else None,
            'event_stage': event.get('stage'),
            'season': info.get('season'),
        }
        all_matches.append(match_row)
        
        # --- INNINGS ---
        for innings_idx, innings_data in enumerate(data.get('innings', [])):
            innings_number = innings_idx + 1
            batting_team = innings_data.get('team')
            # Bowling team is the other team
            bowling_team = None
            if batting_team and len(teams) == 2:
                bowling_team = teams[1] if batting_team == teams[0] else teams[0]
            
            target = innings_data.get('target', {})
            
            innings_row = {
                'match_id': match_id,
                'innings_number': innings_number,
                'batting_team': batting_team,
                'bowling_team': bowling_team,
                'target_runs': target.get('runs'),
                'target_overs': float(target['overs']) if 'overs' in target else None,
                'declared': innings_data.get('declared', False),
                'forfeited': innings_data.get('forfeited', False),
                'is_super_over': innings_data.get('super_over', False),
            }
            all_innings.append(innings_row)
            
            # --- DELIVERIES ---
            if innings_data.get('forfeited'):
                continue  # No deliveries in forfeited innings
            
            for over_data in innings_data.get('overs', []):
                over_number = over_data.get('over', 0)
                
                for ball_idx, delivery in enumerate(over_data.get('deliveries', [])):
                    runs = delivery.get('runs', {})
                    extras = delivery.get('extras', {})
                    wickets = delivery.get('wickets', [])
                    
                    # First wicket (most common — >99.99% have 0 or 1)
                    wicket = wickets[0] if wickets else {}
                    fielders = wicket.get('fielders', [])
                    
                    # Resolve player IDs from registry
                    batter_name = delivery.get('batter')
                    bowler_name = delivery.get('bowler')
                    non_striker_name = delivery.get('non_striker')
                    player_out_name = wicket.get('player_out')
                    
                    delivery_row = {
                        'match_id': match_id,
                        'innings_number': innings_number,
                        'over_number': over_number,
                        'ball_number': ball_idx + 1,
                        'batter': batter_name,
                        'batter_id': registry.get(batter_name),
                        'bowler': bowler_name,
                        'bowler_id': registry.get(bowler_name),
                        'non_striker': non_striker_name,
                        'non_striker_id': registry.get(non_striker_name),
                        'runs_batter': runs.get('batter', 0),
                        'runs_extras': runs.get('extras', 0),
                        'runs_total': runs.get('total', 0),
                        'runs_non_boundary': runs.get('non_boundary', False),
                        'extras_wides': extras.get('wides', 0),
                        'extras_noballs': extras.get('noballs', 0),
                        'extras_byes': extras.get('byes', 0),
                        'extras_legbyes': extras.get('legbyes', 0),
                        'extras_penalty': extras.get('penalty', 0),
                        'is_wicket': len(wickets) > 0,
                        'wicket_kind': wicket.get('kind'),
                        'wicket_player_out': player_out_name,
                        'wicket_player_out_id': registry.get(player_out_name) if player_out_name else None,
                        'wicket_fielder1': fielders[0].get('name') if fielders else None,
                        'wicket_fielder2': fielders[1].get('name') if len(fielders) > 1 else None,
                        'batting_team': batting_team,
                        'bowling_team': bowling_team,
                    }
                    all_deliveries.append(delivery_row)
        
        processed += 1
        if processed % 5000 == 0:
            print(f"Processed {processed}/{len(json_files)} matches ({len(all_deliveries):,} deliveries)")
    
    except Exception as e:
        error_files.append((json_file, str(e)))

print(f"\n=== Parsing Complete ===")
print(f"  Matches:    {len(all_matches):,}")
print(f"  Innings:    {len(all_innings):,}")
print(f"  Deliveries: {len(all_deliveries):,}")
print(f"  Players:    {len(all_players):,}")
print(f"  Errors:     {len(error_files)}")
if error_files:
    print(f"  First 5 errors:")
    for ef, err in error_files[:5]:
        print(f"    {ef}: {err}")

# CELL ********************

# MARKDOWN ********************

# ## Step 3: Create DataFrames and write Delta tables
# 
# Player name ↔ ID mapping uses Cricsheet's registry (8-char hex IDs).
# These are stable across all matches — the same player always has the same ID.

# CELL ********************

# --- PLAYERS TABLE ---
# Schema matches cricket-mcp: player_id (registry hex ID), player_name
# batting_style, bowling_style, playing_role, country are NULL initially
# (populated later by the PlayerEnrichment dataflow)
players_schema = StructType([
    StructField("player_id", StringType()),
    StructField("player_name", StringType()),
    StructField("batting_style", StringType()),
    StructField("bowling_style", StringType()),
    StructField("playing_role", StringType()),
    StructField("country", StringType()),
])

player_rows = [{"player_id": pid, "player_name": name, "batting_style": None, "bowling_style": None, "playing_role": None, "country": None} for pid, name in all_players.items()]
players_df = spark.createDataFrame(player_rows, schema=players_schema)

print(f"Players: {players_df.count():,} rows")
players_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("players")
print("✓ players table written")

# CELL ********************

# --- MATCHES TABLE ---
matches_schema = StructType([
    StructField("match_id", StringType()),
    StructField("data_version", StringType()),
    StructField("match_type", StringType()),
    StructField("match_type_number", IntegerType()),
    StructField("gender", StringType()),
    StructField("team_type", StringType()),
    StructField("overs_per_side", IntegerType()),
    StructField("balls_per_over", IntegerType()),
    StructField("venue", StringType()),
    StructField("city", StringType()),
    StructField("date_start", StringType()),
    StructField("date_end", StringType()),
    StructField("team1", StringType()),
    StructField("team2", StringType()),
    StructField("toss_winner", StringType()),
    StructField("toss_decision", StringType()),
    StructField("outcome_winner", StringType()),
    StructField("outcome_result", StringType()),
    StructField("outcome_method", StringType()),
    StructField("outcome_by_runs", IntegerType()),
    StructField("outcome_by_wickets", IntegerType()),
    StructField("outcome_by_innings", IntegerType()),
    StructField("player_of_match", StringType()),
    StructField("event_name", StringType()),
    StructField("event_match_number", IntegerType()),
    StructField("event_group", StringType()),
    StructField("event_stage", StringType()),
    StructField("season", StringType()),
])

matches_df = spark.createDataFrame(all_matches, schema=matches_schema)

print(f"Matches: {matches_df.count():,} rows")
matches_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("matches")
print("✓ matches table written")

# CELL ********************

# --- INNINGS TABLE ---
innings_schema = StructType([
    StructField("match_id", StringType()),
    StructField("innings_number", IntegerType()),
    StructField("batting_team", StringType()),
    StructField("bowling_team", StringType()),
    StructField("target_runs", IntegerType()),
    StructField("target_overs", FloatType()),
    StructField("declared", BooleanType()),
    StructField("forfeited", BooleanType()),
    StructField("is_super_over", BooleanType()),
])

innings_df = spark.createDataFrame(all_innings, schema=innings_schema)

print(f"Innings: {innings_df.count():,} rows")
innings_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("innings")
print("✓ innings table written")

# CELL ********************

# --- DELIVERIES TABLE ---
deliveries_schema = StructType([
    StructField("match_id", StringType()),
    StructField("innings_number", IntegerType()),
    StructField("over_number", IntegerType()),
    StructField("ball_number", IntegerType()),
    StructField("batter", StringType()),
    StructField("batter_id", StringType()),
    StructField("bowler", StringType()),
    StructField("bowler_id", StringType()),
    StructField("non_striker", StringType()),
    StructField("non_striker_id", StringType()),
    StructField("runs_batter", IntegerType()),
    StructField("runs_extras", IntegerType()),
    StructField("runs_total", IntegerType()),
    StructField("runs_non_boundary", BooleanType()),
    StructField("extras_wides", IntegerType()),
    StructField("extras_noballs", IntegerType()),
    StructField("extras_byes", IntegerType()),
    StructField("extras_legbyes", IntegerType()),
    StructField("extras_penalty", IntegerType()),
    StructField("is_wicket", BooleanType()),
    StructField("wicket_kind", StringType()),
    StructField("wicket_player_out", StringType()),
    StructField("wicket_player_out_id", StringType()),
    StructField("wicket_fielder1", StringType()),
    StructField("wicket_fielder2", StringType()),
    StructField("batting_team", StringType()),
    StructField("bowling_team", StringType()),
])

# Create in batches to avoid driver memory issues
BATCH_SIZE = 2_000_000
total_deliveries = len(all_deliveries)
print(f"Writing {total_deliveries:,} deliveries in batches of {BATCH_SIZE:,}...")

for i in range(0, total_deliveries, BATCH_SIZE):
    batch = all_deliveries[i:i + BATCH_SIZE]
    batch_df = spark.createDataFrame(batch, schema=deliveries_schema)
    
    mode = "overwrite" if i == 0 else "append"
    batch_df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable("deliveries")
    
    print(f"  Batch {i // BATCH_SIZE + 1}: wrote {len(batch):,} rows ({i + len(batch):,}/{total_deliveries:,})")

print("✓ deliveries table written")

# CELL ********************

# MARKDOWN ********************

# ## Step 4: Optimize tables with V-Order

# CELL ********************

tables = ["players", "matches", "innings", "deliveries"]

for table in tables:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table} VORDER")
    
    # Get table stats
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0][0]
    print(f"  {table}: {count:,} rows")

print("\n=== ETL Complete ===")
print(f"End time: {datetime.now().isoformat()}")

# CELL ********************

# MARKDOWN ********************

# ## Step 5: Validation queries

# CELL ********************

# Quick validation
print("=== Validation ===\n")

# Match type distribution
print("Match types:")
spark.sql("""
    SELECT match_type, COUNT(*) as matches 
    FROM matches 
    GROUP BY match_type 
    ORDER BY matches DESC
""").show()

# Delivery count by format
print("Deliveries by format:")
spark.sql("""
    SELECT m.match_type, COUNT(*) as deliveries
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    GROUP BY m.match_type
    ORDER BY deliveries DESC
""").show()

# Top 10 batters by runs
print("Top 10 batters by total runs:")
spark.sql("""
    SELECT d.batter, SUM(d.runs_batter) as total_runs, 
           COUNT(CASE WHEN d.extras_wides = 0 THEN 1 END) as balls_faced
    FROM deliveries d
    WHERE d.extras_wides = 0 OR d.runs_batter > 0
    GROUP BY d.batter
    ORDER BY total_runs DESC
    LIMIT 10
""").show(truncate=False)

# Wicket kind distribution
print("Wicket types:")
spark.sql("""
    SELECT wicket_kind, COUNT(*) as count
    FROM deliveries
    WHERE is_wicket = true
    GROUP BY wicket_kind
    ORDER BY count DESC
""").show()

# CELL ********************

# Clean up temp files
import shutil
shutil.rmtree(tmp_dir, ignore_errors=True)
print(f"Cleaned up temporary directory: {tmp_dir}")
