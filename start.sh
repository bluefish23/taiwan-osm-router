#!/bin/bash
set -e

DB_FILE="${DB_PATH:-/data/taiwan_osm.db}"
PBF_FILE="/data/taiwan.pbf"
PBF_URL="https://download.geofabrik.de/asia/taiwan-latest.osm.pbf"

# Validate existing DB — delete if corrupted
if [ -f "$DB_FILE" ]; then
  if ! python -c "import sqlite3; c=sqlite3.connect('$DB_FILE'); c.execute('SELECT count(*) FROM osm_edges'); print('DB OK')" 2>/dev/null; then
    echo "[start.sh] DB exists but is corrupted, removing..."
    rm -f "$DB_FILE"
  fi
fi

if [ ! -f "$DB_FILE" ]; then
  echo "[start.sh] DB not found, building from PBF..."

  if [ ! -f "$PBF_FILE" ]; then
    echo "[start.sh] Downloading Taiwan PBF..."
    curl -L -o "$PBF_FILE" "$PBF_URL"
    echo "[start.sh] PBF downloaded: $(du -h $PBF_FILE | cut -f1)"
  fi

  echo "[start.sh] Building graph database (this may take several minutes)..."
  python build_osm_graph.py --pbf "$PBF_FILE" --db "$DB_FILE"
  echo "[start.sh] DB built: $(du -h $DB_FILE | cut -f1)"

  rm -f "$PBF_FILE"
  echo "[start.sh] PBF cleaned up"
fi

echo "[start.sh] Starting uvicorn..."
exec uvicorn osm_api:app --host 0.0.0.0 --port 8000
