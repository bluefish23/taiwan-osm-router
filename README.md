# Taiwan OSM Dynamic Navigation System

Real-time turn-by-turn navigation system for Taiwan, built on OpenStreetMap road network data. Integrates live traffic data from TDX (vehicle speed detectors, incident reports) and weather data from CWA (weather stations, rain gauges) to dynamically adjust route costs using A* pathfinding.

## Key Features

- **Turn-by-turn GPS navigation** with off-route detection, automatic rerouting, and arrival detection
- **A* routing engine** over 7.6M road edges with three modes: fastest / balanced / safest
- **Live data integration** from TDX traffic (19 cities) and CWA weather (auto-synced every 5 min)
- **Dynamic cost model** adjusting for congestion, accidents, closures, rain, wind, and visibility
- **Map overlays** for congestion heatmap, incident zones, and weather coverage
- **Address search** via hybrid TGOS + Nominatim geocoding
- **Role-based access** separating public navigation from admin management
- **Cloudflare Tunnel** support for instant public access without port forwarding

## Architecture

```
 Browser                    Server                      External
┌──────────┐  REST   ┌──────────────┐              ┌──────────┐
│index.html├────────►│  osm_api.py  │◄────────────►│  TDX API │
│Navigation│  JSON   │   FastAPI    │  realtime_   │  Traffic  │
└──────────┘         │  + Auth      │  sync.py     └──────────┘
                     │              │                    
┌──────────┐  Admin  │  require_    │              ┌──────────┐
│admin.html├────────►│  admin()     │◄────────────►│  CWA API │
│Dashboard │ +Token  │              │              │  Weather  │
└──────────┘         └──────┬───────┘              └──────────┘
                            │
                     ┌──────┴───────┐
                     │osm_router.py │
                     │  A* Engine   │
                     │  + Graph Lock│
                     └──────┬───────┘
                            │
                     ┌──────┴───────┐
                     │taiwan_osm.db │
                     │  SQLite WAL  │
                     └──────────────┘
```

## Quick Start

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp env.example .env   # Fill in TDX / CWA API keys

# Build road network (one-time, ~3 min)
python build_osm_graph.py --pbf taiwan.osm.pbf --db taiwan_osm.db

# Start server
uvicorn osm_api:app --host 127.0.0.1 --port 8000

# Open browser
#   Navigation: http://127.0.0.1:8000/app
#   Admin:      http://127.0.0.1:8000/admin
```

### Public Access via Cloudflare Tunnel

```bash
# Run with tunnel (requires cloudflared installed)
run_with_tunnel.bat

# Or manually:
cloudflared tunnel --url http://127.0.0.1:8000
```

### Fly.io Deployment

```bash
fly launch --no-deploy
fly volumes create osm_data --size 5 --region nrt
fly secrets set TDX_CLIENT_ID=xxx TDX_CLIENT_SECRET=xxx CWB_API_KEY=xxx ADMIN_TOKEN=xxx
fly deploy
# start.sh auto-downloads PBF and builds the graph if DB is missing
```

## File Structure

| File | Purpose |
|------|---------|
| `osm_router.py` | A* routing engine + dynamic cost calculation + thread-safe graph |
| `osm_api.py` | FastAPI REST API (17 endpoints) + role-based auth |
| `realtime_sync.py` | TDX/CWA background sync engine |
| `index.html` | Navigation UI — map, search, routing, GPS nav, overlays |
| `admin.html` | Admin dashboard — stats, event/weather management, sync control |
| `build_osm_graph.py` | OSM PBF → SQLite graph builder (offline, run once) |
| `env.example` | Environment variable template |
| `Dockerfile` | Container image |
| `fly.toml` | Fly.io deployment config |
| `start.sh` | Entrypoint: DB validation + auto PBF download/build + uvicorn |

## API Endpoints

### Public (no auth required)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Frontend navigation UI |
| GET | `/status` | Service health check |
| GET | `/stats` | System statistics |
| GET | `/nearest?lat=&lon=` | Nearest road network node |
| POST | `/route` | Route planning (segments + GeoJSON + analysis) |
| GET | `/events` | List active traffic events |
| GET | `/weather` | List active weather data |
| GET | `/geocode?q=` | Address/landmark geocoding |
| GET | `/sync/status` | Sync engine status |

### Admin (localhost or Bearer token required)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin` | Admin dashboard UI |
| POST | `/events` | Add manual event |
| DELETE | `/events/{id}` | Delete event |
| DELETE | `/events` | Clear all events + weather |
| POST | `/weather` | Add manual weather |
| POST | `/dynamic/recompute` | Recompute dynamic edge costs |
| POST | `/sync/trigger` | Trigger manual data sync |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TDX_CLIENT_ID` | TDX OAuth2 Client ID | — |
| `TDX_CLIENT_SECRET` | TDX OAuth2 Client Secret | — |
| `CWB_API_KEY` | CWA Weather API Key | — |
| `ADMIN_TOKEN` | Admin auth token for remote access | — |
| `TGOS_API_KEY` | TGOS Geocoding API Key (optional) | — |
| `DB_PATH` | SQLite database path | `taiwan_osm.db` |
| `AUTO_SYNC_INTERVAL` | Sync interval seconds, 0=disable | `300` |
| `ALLOWED_ORIGINS` | CORS origins (comma-separated) | `*` |

## Data Sources

- [OpenStreetMap](https://www.openstreetmap.org/) — road network via [Geofabrik](https://download.geofabrik.de/asia/taiwan.html) taiwan.osm.pbf
- [TDX](https://tdx.transportdata.tw/) — real-time vehicle speed detectors + traffic incident news
- [CWA](https://opendata.cwa.gov.tw/) — automatic weather stations + rain gauge stations
- [TGOS](https://api.tgos.tw/) — Taiwan address geocoding (optional)
- [Nominatim](https://nominatim.openstreetmap.org/) — POI/landmark search

## License

MIT
