# Taiwan OSM Dynamic Routing System

Live: **https://taiwan-osm-router.fly.dev/app**

Real-time navigation system for Taiwan built on OpenStreetMap data, integrating TDX traffic data (vehicle speed detection, incident reports) and CWA weather data (weather stations, rainfall) for dynamic route planning via A* algorithm.

## Features

- **A* routing engine** with 811K nodes / 1.38M pre-loaded edges (motorway-tertiary)
- **Three routing modes**: fastest / balanced / safest
- **Real-time data integration**: TDX traffic speed, incidents, CWA weather — auto-synced every 5 minutes
- **Dynamic cost adjustment**: congestion, accidents, construction, closures, rain, wind, visibility
- **Address/landmark search**: hybrid geocoding via TGOS + Nominatim
- **GPS positioning**: browser geolocation with live tracking
- **Overlay visualization**: congestion heatmap, incident markers, weather circles
- **Separate user/admin interfaces**: navigation app + management dashboard

## Architecture

```
index.html  ──►  osm_api.py  ──►  osm_router.py
(Navigation)     (FastAPI)        (A* Engine)
admin.html  ──┘       │
                       ├──►  realtime_sync.py  ──►  TDX API + CWA API
                       └──►  taiwan_osm.db (SQLite)
```

## File Structure

| File | Purpose |
|------|---------|
| `osm_router.py` | A* path search engine + dynamic cost calculation |
| `osm_api.py` | FastAPI REST API server (15 endpoints) |
| `realtime_sync.py` | TDX/CWA background sync engine |
| `index.html` | User navigation UI (full-screen map + floating panels) |
| `admin.html` | Admin dashboard (network stats, event/weather management) |
| `build_osm_graph.py` | OSM PBF → SQLite graph builder (run once) |
| `Dockerfile` | Container image for deployment |
| `fly.toml` | Fly.io deployment configuration |
| `start.sh` | Entrypoint: DB validation + auto PBF download/build + uvicorn |

## Deployment (Fly.io)

The app runs on Fly.io with a persistent volume for the SQLite database.

```bash
# First-time setup
fly launch --no-deploy
fly volumes create osm_data --size 5 --region nrt
fly secrets set TDX_CLIENT_ID=xxx TDX_CLIENT_SECRET=xxx CWB_API_KEY=xxx

# Deploy
fly deploy

# The start.sh script auto-downloads taiwan.osm.pbf and builds the graph
# if taiwan_osm.db is not found on the volume.
```

**Machine specs**: shared-cpu-2x (2GB RAM), 5GB volume, Tokyo (nrt) region

## Local Development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
cp env.example .env   # Fill in TDX / CWA API keys

# 3. Build road network (only once, ~3 min)
python build_osm_graph.py --pbf taiwan.osm.pbf --db taiwan_osm.db

# 4. Start server
uvicorn osm_api:app --host 127.0.0.1 --port 8000

# 5. Open browser
#    Navigation: http://127.0.0.1:8000/app
#    Admin:      http://127.0.0.1:8000/admin
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Service health check |
| GET | `/stats` | System statistics (graph, sync status) |
| GET | `/nearest?lat=&lon=` | Nearest road network node |
| POST | `/route` | Route planning (segments + GeoJSON + analysis) |
| GET | `/events` | List active events |
| POST | `/events` | Add manual event |
| DELETE | `/events/{id}` | Delete event |
| DELETE | `/events` | Clear all events + weather |
| GET | `/weather` | List active weather |
| POST | `/weather` | Add manual weather |
| POST | `/dynamic/recompute` | Recompute dynamic costs |
| GET | `/sync/status` | Sync engine status |
| POST | `/sync/trigger` | Trigger manual sync |
| GET | `/geocode?q=` | Address/landmark geocoding |
| GET | `/app` | Serve navigation UI |
| GET | `/admin` | Serve admin dashboard |

## Routing Engine

- **Graph loading**: pre-loads motorway→tertiary into memory at startup (namedtuple edges for ~60% memory savings)
- **Road speeds**: motorway=110, trunk=90, primary=60, secondary=50, tertiary=40, residential=30 km/h
- **Cost formula**: `edge_cost = base_cost × (time_weight + risk_score × risk_weight)`
- **Event multipliers**: accident=1.80, construction=1.55, closure=999, congestion=1.35
- **Weather multipliers**: rain×0.40, wind×0.20, visibility×0.35, warning×0.30
- **A* heuristic**: haversine distance / 120 km/h (admissible, guarantees optimal path)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TDX_CLIENT_ID` | TDX OAuth2 Client ID | — |
| `TDX_CLIENT_SECRET` | TDX OAuth2 Client Secret | — |
| `CWB_API_KEY` | CWA Weather API Key | — |
| `TGOS_API_KEY` | TGOS Geocoding API Key (optional) | — |
| `DB_PATH` | SQLite database path | `taiwan_osm.db` |
| `AUTO_SYNC_INTERVAL` | Sync interval in seconds, 0=disable | `300` |

## Database

| Table | Purpose | Scale |
|-------|---------|-------|
| `osm_nodes` | Road network nodes | ~3.76M |
| `osm_edges` | Edges with dynamic fields | ~7.65M |
| `dynamic_events` | Traffic events | ~300 realtime |
| `dynamic_weather` | Weather conditions | ~50-80 realtime |
| `vd_positions` | VD detector position cache | ~1,361 |

## Performance Optimizations

- **namedtuple edges** instead of dicts (~60% memory reduction)
- **Cursor iteration** instead of fetchall() during graph loading
- **Cached node/edge counts** via background thread (avoids COUNT(*) on millions of rows)
- **Targeted recompute**: only resets edges with modified dynamic values
- **WAL checkpoint** after recompute to prevent unbounded WAL growth
- **TDX auth backoff**: 5-minute cooldown on authentication failure
- **VD position cache**: static detector positions cached in DB, avoids repeated API calls

## Data Sources

- [OpenStreetMap](https://www.openstreetmap.org/) — road network (via Geofabrik taiwan.osm.pbf)
- [TDX](https://tdx.transportdata.tw/) — real-time traffic speed, incidents
- [CWA](https://opendata.cwa.gov.tw/) — weather stations, rainfall
- [TGOS](https://api.tgos.tw/) — address geocoding (optional)
- [Nominatim](https://nominatim.openstreetmap.org/) — POI/landmark search
