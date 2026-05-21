# System Features — Taiwan OSM Dynamic Navigation

Detailed technical documentation of all system capabilities, modules, and design decisions.

---

## 1. Routing Engine (`osm_router.py`)

### 1.1 Graph Structure

- **Data source**: OpenStreetMap Taiwan PBF processed by `build_osm_graph.py`
- **Scale**: ~3.76M nodes, ~7.65M directed edges stored in SQLite
- **In-memory graph**: Pre-loads motorway through tertiary edges at startup (~1.38M edges) using `namedtuple` for ~60% memory savings vs dicts
- **Short-range fallback**: Routes under 15 km query the full road network (including residential, service roads) from SQLite on demand
- **Connected components**: Each node has a `component_id`; routing only between nodes in the same component to avoid dead-end failures

### 1.2 A* Pathfinding

- **Heuristic**: Haversine distance / 120 km/h (admissible — never overestimates, guarantees optimal path)
- **Iteration limit**: 2,000,000 expansions to prevent runaway searches on disconnected subgraphs
- **Thread safety**: `threading.RLock` on the graph — concurrent route requests are serialized; dynamic cost updates wait for in-flight routes to finish

### 1.3 Three Routing Modes

| Mode | time_weight | risk_weight | Use Case |
|------|-------------|-------------|----------|
| `fastest` | 1.0 | 0.10 | Minimize travel time, tolerate moderate risk |
| `balanced` | 1.0 | 0.40 | Default — trade-off between speed and safety |
| `safest` | 1.0 | 0.90 | Avoid hazardous areas even if significantly slower |

**Cost formula**: `edge_cost = adjusted_time × (time_weight + risk × risk_weight) + turn_penalty + signal_delay`  
Where:
- `adjusted_time = (distance_km / (speed_kmh × time_speed_factor)) × 60` (minutes)
- `time_speed_factor` = time-of-day speed multiplier (see §1.7)
- `risk = dynamic_risk_score + night_risk` (see §1.6)
- `turn_penalty` = bearing-change penalty (see §1.8)
- `signal_delay` = 0.33 min per signal node (see §1.9)

### 1.4 Road Speed Model

| Highway Type | Speed (km/h) |
|-------------|---------------|
| motorway | 110 |
| trunk | 90 |
| primary | 60 |
| secondary | 50 |
| tertiary | 40 |
| residential | 30 |
| living_street | 20 |
| service | 20 |

### 1.5 Dynamic Cost Adjustments

#### Event Multipliers

| Event Type | Cost Multiplier | Description |
|-----------|-----------------|-------------|
| accident | 1.80 | Traffic accident zone |
| construction | 1.55 | Road construction |
| closure | 999 | Road completely closed (effectively infinite cost) |
| congestion | 1.35 | Traffic congestion (from VD speed data) |
| manual | 1.25 | User-defined event |
| landslide_warning | 1.50 | Mountain road landslide risk (rain ≥ 200mm) |
| landslide_high | 3.00 | High landslide risk (rain ≥ 350mm) |
| landslide_closure | 999 | Landslide road closure (rain ≥ 600mm) |

#### Weather Multipliers

Weather impact is additive: `weather_mult = 1.0 + 0.40×rain + 0.20×wind + 0.35×visibility + 0.30×warning`

Each factor is normalized to 0–1 range:
- **Rain**: raw mm/h ÷ 80 (capped at 1.0)
- **Wind**: (speed_m/s − 5) ÷ 25 (floor 0, cap 1.0)
- **Visibility**: categorical — 0.0 (clear), 0.3, 0.6, 1.0 (dense fog)
- **Warning**: CWA warning level 0–1

### 1.6 Night Driving Risk (Research-backed)

Time-of-day risk factor added to `risk_score` in A* cost calculation. Based on NHTSA data showing nighttime accounts for only 25% of miles driven but 50% of fatal crashes (fatality rate 3–9× daytime).

| Time Period (UTC+8) | Risk Addition | Research Basis |
|---------------------|---------------|----------------|
| 07:00–17:00 (day) | +0.00 | Baseline |
| 17:00–19:00 (dusk) | +0.05 | NHTSA: 1.5× accident risk |
| 19:00–00:00 (night) | +0.15 | NSC: 3× fatality rate |
| 00:00–04:00 (late night) | +0.25 | NHTSA 2008: 4–9× with fatigue |
| 04:00–06:00 (dawn) | +0.15 | Similar to evening |
| 06:00–07:00 (early morning) | +0.05 | Transition period |

**References**: [NHTSA Nighttime Glare Study 2008](https://www.nhtsa.gov/sites/nhtsa.gov/files/811043.pdf), [National Safety Council — Driving at Night](https://www.nsc.org/road/safety-topics/driving-at-night)

### 1.7 Time-Dependent Speed Profiles (Research-backed)

Speed multiplier applied to `base_time` based on road type × time of day. Rush hour reduces urban road effective speed by 30–40%. Based on TDVRP research showing time-dependent routing eliminates 99% of late arrivals.

| Time Period | motorway | trunk | primary | secondary | tertiary | residential |
|------------|----------|-------|---------|-----------|----------|-------------|
| 00:00–06:00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| 07:00–09:00 (AM peak) | 0.85 | 0.80 | 0.70 | 0.65 | 0.62 | 0.60 |
| 09:00–17:00 (midday) | 0.92 | 0.88 | 0.82 | 0.78 | 0.75 | 0.75 |
| 17:00–19:00 (PM peak) | 0.85 | 0.80 | 0.70 | 0.65 | 0.62 | 0.60 |
| 19:00–22:00 (evening) | 0.95 | 0.93 | 0.90 | 0.88 | 0.85 | 0.88 |

Higher-class roads (motorway/trunk) experience less peak degradation due to controlled access and higher capacity.

**References**: [Vehicle Routing with Time-Dependent Travel Times](https://www.researchgate.net/publication/360332685), [Time Dependent Travel Speed Routing: Torino](https://www.sciencedirect.com/science/article/pii/S2352146514001872)

### 1.8 Turn Penalties (Research-backed)

A* pathfinding tracks the incoming edge bearing and computes the angle difference to each outgoing edge. Turn type is classified by angle, and a time penalty (in minutes) is added to the edge cost.

| Angle Difference | Turn Type | Penalty (minutes) | Research Basis |
|-----------------|-----------|-------------------|----------------|
| < 20° | Straight | 0.00 | No delay |
| 20°–60° | Slight turn | 0.05 (~3 sec) | Transport Geography |
| 60°–130° | Left/right turn | 0.17 (~10 sec) | McGill: 5–15 sec |
| 130°–170° | Sharp turn | 0.25 (~15 sec) | McGill: 15–35 sec |
| > 170° | U-turn | 0.42 (~25 sec) | HCM estimate |

Turn penalties are significant in urban routing — McGill University research shows turn delays account for 15–25% of total urban trip time.

**References**: [Intersection Turn Delay Modelling (McGill)](https://tram.mcgill.ca/Research/Publications/Turn%20Delay%20Modelling.pdf), [Turn Penalties at Intersections (Transport Geography)](https://transportgeography.org/contents/methods/network-data-models/turn-penalty-intersection/)

### 1.9 Signal Density Delay (Research-backed)

Traffic signal nodes from OSM (`highway=traffic_signals`, `stop`, `crossing`) are identified during graph build and stored in `osm_nodes.is_signal`. During A* routing, each signal node adds a fixed delay of 0.33 minutes (~20 seconds).

- **Exempt**: motorway, motorway_link, trunk, trunk_link (no signals on controlled-access roads)
- **Impact**: Urban arterials typically have 2–6 signals/km, adding 40–120 sec/km to travel time

Signal delay accounts for 20–40% of urban travel time (HCM). The 20-second average represents the midpoint of HCM's 15–45 second per-signal delay range.

**References**: [Delay Function for Signalized Intersections (ResearchGate)](https://www.researchgate.net/publication/245292022), [Highway Capacity Manual Ch.19 — Signal Delay](https://nap.nationalacademies.org/resource/26432/Highway_Capacity_Manual_Edition_7.1_Chapters.pdf)

### 1.10 Landslide / Debris Flow Risk (Research-backed, Taiwan-specific)

Taiwan experiences frequent rainfall-induced landslides due to steep terrain and typhoon activity. Landslide area increased from 170 km² (2004) to 506 km² (2010 post-Typhoon Morakot).

During CWA weather sync, accumulated rainfall is estimated from hourly data. Mountain area stations (lat ≥ 23.0°N) with heavy rainfall trigger landslide events:

| Accumulated Rainfall | Risk Level | Cost Multiplier | Action |
|---------------------|------------|-----------------|--------|
| < 200 mm | Low | — | Normal routing |
| ≥ 200 mm | Warning | 1.50 | Route penalized |
| ≥ 350 mm | High | 3.00 | Strong avoidance |
| ≥ 600 mm | Critical | 999 (closure) | Road effectively blocked |

Thresholds based on Taiwan Soil and Water Conservation Bureau risk index classifications and academic landslide susceptibility models.

**References**: [Risk-based Landslide Monitoring in Taiwan (Tandfonline 2017)](https://www.tandfonline.com/doi/full/10.1080/19475705.2017.1345797), [Landslide Responses to Typhoon Events 2019–2023 (MDPI)](https://www.mdpi.com/2071-1050/17/21/9673), [Quantifying Travel Time Impacts of Slope Failures (MDPI)](https://www.mdpi.com/2071-1050/17/20/9170)

### 1.11 SQL Injection Protection

The `highway` field in event creation is validated against a whitelist of 15 valid OSM highway types. All database queries use parameterized `?` placeholders — no string interpolation in SQL.

---

## 2. REST API (`osm_api.py`)

### 2.1 Endpoint Summary

17 endpoints total: 9 public, 8 admin-protected.

#### Public Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Serves navigation frontend (`index.html`) |
| `GET /status` | Health check — returns service name, version, graph status |
| `GET /stats` | System statistics — edge/node counts, event/weather counts, sync status |
| `GET /nearest?lat=&lon=` | Finds nearest road network node to given coordinates |
| `POST /route` | Route planning — accepts start/end coords or node IDs, returns step-by-step segments, GeoJSON polyline, distance/time/analysis |
| `GET /events` | Lists all active traffic events (both realtime and manual) |
| `GET /weather` | Lists all active weather records |
| `GET /geocode?q=` | Geocoding — searches TGOS (addresses) then Nominatim (POIs) |
| `GET /sync/status` | Sync engine status — running state, counts, last error, timing |

#### Admin Endpoints (require auth)

| Endpoint | Description |
|----------|-------------|
| `GET /admin` | Serves admin dashboard (`admin.html`) |
| `POST /events` | Create manual traffic event |
| `DELETE /events/{id}` | Delete specific event |
| `DELETE /events` | Clear all events and weather data |
| `POST /weather` | Create manual weather record |
| `POST /dynamic/recompute` | Recompute all dynamic edge costs from current events/weather |
| `POST /sync/trigger` | Manually trigger TDX/CWA data sync |

### 2.2 Authentication & Authorization

- **Localhost bypass**: Requests from `127.0.0.1`, `::1`, or `localhost` skip token check — admin UI works without token when accessed locally
- **Remote access**: Requires `Authorization: Bearer <ADMIN_TOKEN>` header
- **Token comparison**: Uses `secrets.compare_digest()` for timing-attack-safe comparison
- **Public users** (e.g., via Cloudflare Tunnel): Can only access navigation features and read-only event/weather data. All write/management endpoints return `403 Forbidden`

### 2.3 Geocoding

Dual-source geocoding with in-memory cache:
1. **TGOS** (optional): Taiwan government address geocoding API — best for street addresses
2. **Nominatim**: OpenStreetMap geocoding — best for POIs, landmarks, place names
3. **Cache**: Thread-safe LRU cache (500 entries, 5-minute TTL) to avoid repeated API calls

### 2.4 Input Validation

- **Coordinate bounds**: lat 21.5–26.5, lon 118–122.5 (Taiwan bounding box)
- **Highway whitelist**: Regex pattern on `EventReq.highway` field — only valid OSM highway types accepted
- **Node validation**: `start_node` / `end_node` checked against loaded graph before routing
- **Error sanitization**: Internal exceptions logged server-side; clients receive generic error messages without stack traces or file paths

### 2.5 CORS

Configurable via `ALLOWED_ORIGINS` environment variable. Defaults to `*` for development; can be restricted to specific domains in production.

### 2.6 Concurrency

- **Graph lock**: `threading.RLock` in `OSMRouter` protects the in-memory graph during concurrent route queries and dynamic cost updates
- **Cache lock**: `threading.Lock` on geocode cache prevents race conditions
- **SQLite**: WAL mode + `busy_timeout=30000ms` + `timeout=30s` for safe concurrent read/write access

---

## 3. Real-time Data Sync (`realtime_sync.py`)

### 3.1 TDX Traffic Data

#### Vehicle Detector (VD) Speed Data
- Queries 19 cities sequentially with 1.5-second inter-request delay to avoid 429 rate limits
- **Static positions**: VD detector locations cached in `vd_positions` table (~1,361 detectors). Fetched once, reused on subsequent syncs
- **Live speed matching**: Joins live speed readings with cached positions via VDID
- **Congestion detection**: When measured speed < 70% of free-flow speed for that road class, creates a congestion event with severity proportional to speed reduction
- **Top 300**: Only the 300 most severe congestion events are kept per sync cycle

#### Traffic Incident News
- Fetches from TDX News/Highway endpoint
- Maps `NewsCategory` to event types: accident, construction, closure, congestion
- Entries without lat/lon coordinates are skipped

#### Authentication
- OAuth2 `client_credentials` flow with automatic token refresh
- Exponential backoff retry on 429 (base 1.5s, max 3 retries)
- 5-minute cooldown on authentication failure to avoid API lockout

### 3.2 CWA Weather Data

#### Automatic Weather Stations (O-A0001-001)
- Wind speed and visibility data from ~700+ stations
- Normalized to 0–1 scale for the dynamic cost model

#### Rain Gauge Stations (O-A0002-001)
- Hourly rainfall data from ~400+ stations
- Normalized: raw mm/h ÷ 80 (capped at 1.0)

#### SSL Handling
- CWA server has a known SSL certificate issue (Missing Subject Key Identifier)
- SSL verification is disabled specifically for CWA requests only; TDX and all other connections use full SSL verification

### 3.3 Source Separation

| Source | Behavior |
|--------|----------|
| `realtime` | Automatically created by sync engine. Cleared before each sync cycle |
| `manual` | Created by admin via API. Preserved across sync cycles — never auto-deleted |

### 3.4 Sync Cycle

1. Clear all `source='realtime'` events and weather
2. Fetch TDX VD speed data → create congestion events
3. Fetch TDX news → create incident events
4. Fetch CWA weather + rain data → create weather records
5. Recompute dynamic edge costs on the entire graph
6. WAL checkpoint to prevent unbounded WAL growth
7. Report counts and timing

Default interval: 300 seconds (5 minutes), matching TDX data update frequency. Configurable via `AUTO_SYNC_INTERVAL`.

---

## 4. Navigation Frontend (`index.html`)

Single-page application using Leaflet.js. All CSS/JS embedded in one HTML file.

### 4.1 Map Interface

- **Leaflet.js** with OpenStreetMap tile layer
- **Tap or click** to set start/end points directly on map
- **Draggable markers** for fine-tuning positions
- **Responsive design** with floating panels

### 4.2 Address Search

- Autocomplete search boxes for start and end locations
- Queries `/geocode` endpoint with debouncing
- Suggestion dropdown with click-to-select
- XSS protection: all dynamic HTML content sanitized via `escHtml()`

### 4.3 Route Display

- GeoJSON polyline rendered on map with color coding
- Route summary: total distance (km), estimated time (min), number of segments
- Step-by-step segment list with road names and distances
- Three mode buttons: fastest / balanced / safest

### 4.4 GPS Navigation Mode

Activated after route calculation by pressing the "Start Driving" button.

#### Turn-by-Turn Instructions
- Calculates bearing between consecutive edges
- Detects 7 turn types: straight, slight left/right, left/right, sharp left/right, U-turn
- Displays turn direction icon + distance to next turn in a floating HUD
- Current road name shown in the HUD

#### Off-Route Detection
- Measures distance from GPS position to nearest route edge
- Threshold: 80 meters
- Requires 3 consecutive off-route readings before triggering reroute (avoids GPS noise false positives)
- Automatic reroute: calls `/route` from current GPS position to original destination

#### Route Progress
- Tracks which edge the driver is currently on
- Passed route segments change to grey color
- Remaining route stays in original color

#### Arrival Detection
- When GPS position is within 50 meters of destination, navigation ends
- Success notification displayed

#### Event-Triggered Reroute
- Every 60 seconds, overlays refresh and compare event hash
- If new events appear on the remaining route, automatic reroute is triggered
- Prevents driving into newly reported incidents

#### GPS Marker
- Blue dot shows current position only during active navigation
- Hidden when not in driving mode to reduce visual clutter

### 4.5 Overlay Layers

Three independent toggleable layers, auto-refreshed every 60 seconds:

| Layer | Visualization | Data Source |
|-------|---------------|-------------|
| **Congestion** | Color-coded circle markers (green→yellow→red by severity) | TDX VD speed data |
| **Events** | Semi-transparent impact circles with color by type | TDX incidents + manual events |
| **Weather** | Large semi-transparent circles (blue=rain, green=wind, grey=fog) | CWA stations |

Layer counts displayed in the overlay control panel.

### 4.6 Sync Status Panel

- Shows sync running state, last sync time, duration
- Displays TDX speed count, incident count, CWA station count
- Manual sync trigger button (admin only from localhost)

---

## 5. Admin Dashboard (`admin.html`)

Separate management interface, accessible at `/admin`.

### 5.1 Access Control

- **Localhost**: No token needed — admin page loads directly
- **Remote**: Requires entering admin token in the top-bar input field. All API calls include `Authorization: Bearer <token>` header

### 5.2 Features

- **System stats**: Node/edge counts, event/weather counts, database info
- **Manual event creation**: Add traffic events with type, severity, location, radius, highway filter
- **Manual weather creation**: Add weather records with rain/wind/visibility/warning levels
- **Event listing and deletion**: View all events, delete individually or clear all
- **Dynamic cost recompute**: Trigger recalculation of all edge costs
- **Sync control**: View sync status, trigger manual sync

---

## 6. Database Schema

SQLite with WAL journal mode for concurrent read/write access.

### Tables

| Table | Columns | Scale | Purpose |
|-------|---------|-------|---------|
| `osm_nodes` | node_id, lat, lon, component_id, is_signal | ~3.76M | Road network nodes (is_signal=1 for traffic signals/stop signs) |
| `osm_edges` | node_a, node_b, lat_a, lon_a, lat_b, lon_b, dist_km, highway, speed_kmh, dynamic_mult, closure_flag, risk_score | ~7.65M | Directed edges with dynamic fields |
| `dynamic_events` | id, event_type, severity, highway, lat, lon, radius_km, description, active, created_at, source | ~300 realtime | Traffic events |
| `dynamic_weather` | id, lat, lon, radius_km, rain_level, wind_level, visibility_level, warning_level, active, created_at, source | ~50-80 realtime | Weather conditions |
| `vd_positions` | vd_id, lat, lon, road_class, city | ~1,361 | VD detector position cache |

### Indexes

- `idx_edges_latlon` on `osm_edges(lat_a, lon_a)` — critical for spatial lookups during dynamic cost recompute
- `idx_nodes_latlon` on `osm_nodes(lat, lon)` — nearest-node queries
- `idx_nodes_component` on `osm_nodes(component_id)` — component-based routing

---

## 7. Security

### 7.1 Implemented Protections

| Category | Protection |
|----------|-----------|
| **SQL Injection** | All queries use parameterized `?` placeholders. Highway field validated against whitelist |
| **XSS** | `escHtml()` sanitizes all dynamic content in `innerHTML` (search suggestions, route steps, map popups) |
| **Auth** | `require_admin` dependency on all write endpoints. `secrets.compare_digest()` for timing-safe token comparison |
| **CORS** | Configurable allowed origins. Restricted HTTP methods (GET, POST, DELETE only) |
| **Error Leakage** | Internal errors logged server-side; clients receive generic messages. Database path not exposed in stats |
| **Concurrency** | `RLock` on graph, `Lock` on geocode cache, SQLite WAL + busy timeout |
| **Input Validation** | Coordinate bounds check, highway whitelist regex, node existence verification |

### 7.2 Permission Model

| Role | Access | Endpoints |
|------|--------|-----------|
| **Public** | Tunnel URL or any remote | Navigation, read events/weather, geocode, route, stats |
| **Admin** | Localhost or Bearer token | All public + event/weather CRUD, sync trigger, recompute, admin UI |

---

## 8. Performance

| Optimization | Detail |
|-------------|--------|
| **namedtuple edges** | ~60% memory reduction vs dict-based graph |
| **Cursor iteration** | Streams edges during graph load instead of `fetchall()` |
| **Cached counts** | Background thread caches node/edge COUNT(*) to avoid slow full-table scans |
| **Targeted recompute** | Only resets edges with modified dynamic values, not entire graph |
| **WAL checkpoint** | Prevents unbounded WAL growth after recompute |
| **VD position cache** | Static detector positions stored in DB; avoids re-fetching from TDX API |
| **TDX auth backoff** | 5-minute cooldown on auth failure prevents API lockout |
| **Geocode cache** | 500-entry LRU with 5-min TTL reduces external API calls |
| **Pre-loaded main graph** | Motorway→tertiary in memory; only short-range routes query full DB |
| **Signal node set** | `is_signal` nodes pre-loaded into memory set for O(1) lookup during A* |
| **Time factors cached per-route** | Night risk and time-of-day speed factor computed once per route call, not per edge |
| **Turn bearing reuse** | Parent edge bearing cached in `came_from`; avoids recomputation during neighbor expansion |

---

## 9. Deployment Options

### Local (Development)

```
uvicorn osm_api:app --host 127.0.0.1 --port 8000
```

### Local + Public Access (Cloudflare Tunnel)

```
cloudflared tunnel --url http://127.0.0.1:8000
```
Generates a random `*.trycloudflare.com` URL. No account required. Admin endpoints are automatically protected (non-localhost → requires token).

### Cloud (Fly.io)

- Docker container on `shared-cpu-1x` with 2GB RAM
- 5GB persistent volume for SQLite database
- Tokyo (nrt) region for low latency to Taiwan
- Auto-builds graph from PBF on first deploy if DB missing
- Secrets managed via `fly secrets set`
