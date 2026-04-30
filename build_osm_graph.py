#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_osm_graph.py
OSM PBF → SQLite 路網資料庫建置工具

用法：
    python build_osm_graph.py --pbf taiwan.pbf --db taiwan_osm.db

輸出資料表：
    osm_nodes(node_id, lat, lon)
    osm_edges(edge_id, way_id, from_node, to_node, highway, name,
              oneway, length_km, speed_kmh, lat_a, lon_a, lat_b, lon_b)
"""
from __future__ import annotations

import argparse
import collections
import math
import pickle
import sqlite3
import struct
import sys
import time
import zlib
from pathlib import Path

# ── 道路類型預設速度（km/h） ──────────────────────────────────────────
HIGHWAY_SPEEDS: dict[str, float] = {
    "motorway":      110, "motorway_link":  70,
    "trunk":          90, "trunk_link":     60,
    "primary":        60, "primary_link":   45,
    "secondary":      50, "secondary_link": 40,
    "tertiary":       40, "tertiary_link":  30,
    "unclassified":   30, "residential":    30,
    "living_street":  10, "service":        20,
    "road":           30,
}
KEEP_HIGHWAY = set(HIGHWAY_SPEEDS.keys())


# ── PBF Parser ─────────────────────────────────────────────────────────
def _read_varint(data: bytes, pos: int):
    v = 0; shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        v |= (b & 0x7F) << shift; shift += 7
        if not (b & 0x80): break
    return v, pos

def _parse_msg(data: bytes) -> dict:
    if not isinstance(data, (bytes, bytearray)): data = bytes(data)
    pos = 0; f: dict = collections.defaultdict(list)
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        if tag == 0: break
        fld = tag >> 3; wire = tag & 7
        try:
            if wire == 0:   val, pos = _read_varint(data, pos)
            elif wire == 2:
                l, pos = _read_varint(data, pos)
                val = data[pos:pos+l]; pos += l
            elif wire == 1: val = data[pos:pos+8]; pos += 8
            elif wire == 5: val = data[pos:pos+4]; pos += 4
            else: break
        except Exception: break
        f[fld].append(val)
    return f

def _parse_packed_int(data: bytes) -> list[int]:
    pos = 0; v = []
    while pos < len(data): val, pos = _read_varint(data, pos); v.append(val)
    return v

def _parse_packed_sint(data: bytes) -> list[int]:
    pos = 0; v = []
    while pos < len(data):
        raw, pos = _read_varint(data, pos)
        v.append((raw >> 1) ^ -(raw & 1))
    return v

def _read_blob(bb: bytes) -> bytes:
    f = _parse_msg(bb)
    if 1 in f: return bytes(f[1][0])
    if 3 in f: return zlib.decompress(bytes(f[3][0]))
    return b''

def _iter_blocks(pbf_path: str):
    """Yield (block_type, decompressed_content) for each OSMData block."""
    with open(pbf_path, "rb") as fp:
        while True:
            raw = fp.read(4)
            if len(raw) < 4: break
            hs = struct.unpack(">I", raw)[0]
            hb = fp.read(hs)
            hf = _parse_msg(hb)
            ds = hf.get(3, [0])[0]
            bb = fp.read(ds)
            btype = bytes(hf.get(1, [b""])[0]).decode()
            if btype != "OSMData": continue
            yield _read_blob(bb)


# ── Pass 1: Ways ───────────────────────────────────────────────────────
def pass1_ways(pbf_path: str, verbose: bool = True) -> tuple[dict, set]:
    """Extract all highway ways and collect the set of needed node IDs."""
    highway_ways: dict = {}
    needed_nodes: set = set()
    block_i = 0

    for content in _iter_blocks(pbf_path):
        block_i += 1
        pb = _parse_msg(content)
        strings = [bytes(s).decode("utf-8", "replace")
                   for s in _parse_msg(bytes(pb.get(1, [b""])[0])).get(1, [])]

        for pg_bytes in pb.get(2, []):
            pg = _parse_msg(bytes(pg_bytes))
            for wb in pg.get(3, []):
                w = _parse_msg(bytes(wb))
                wid = w.get(1, [None])[0]
                if wid is None: continue
                keys = _parse_packed_int(bytes(w[2][0])) if 2 in w else []
                vals = _parse_packed_int(bytes(w[3][0])) if 3 in w else []
                tags = {strings[k]: strings[v] for k, v in zip(keys, vals)
                        if k < len(strings) and v < len(strings)}
                hw = tags.get("highway", "")
                if hw not in KEEP_HIGHWAY: continue
                refs_raw = w.get(8, [None])[0]
                if refs_raw is None: continue
                refs: list[int] = []
                delta = 0
                for dv in _parse_packed_sint(bytes(refs_raw)):
                    delta += dv; refs.append(delta)
                if len(refs) < 2: continue
                highway_ways[wid] = {
                    "refs":     refs,
                    "highway":  hw,
                    "name":     tags.get("name", tags.get("name:zh", "")),
                    "oneway":   tags.get("oneway", "no"),
                    "maxspeed": tags.get("maxspeed", ""),
                }
                needed_nodes.update(refs)

        if verbose and block_i % 500 == 0:
            print(f"  [P1] block={block_i} ways={len(highway_ways):,} "
                  f"needed_nodes={len(needed_nodes):,}", flush=True)

    return highway_ways, needed_nodes


# ── Pass 2: Node coordinates ───────────────────────────────────────────
def pass2_nodes(pbf_path: str, needed_nodes: set, verbose: bool = True) -> dict:
    """Collect lat/lon for every node in needed_nodes."""
    node_coords: dict[int, tuple[float, float]] = {}
    block_i = 0

    for content in _iter_blocks(pbf_path):
        block_i += 1
        pb = _parse_msg(content)
        gran    = pb.get(17, [100])[0]
        lat_off = pb.get(19, [0])[0]
        lon_off = pb.get(20, [0])[0]

        for pg_bytes in pb.get(2, []):
            pg = _parse_msg(bytes(pg_bytes))
            if 2 not in pg: continue
            dn = _parse_msg(bytes(pg[2][0]))
            ir = dn.get(1, [None])[0]
            lr = dn.get(8, [None])[0]
            or_ = dn.get(9, [None])[0]
            if None in (ir, lr, or_): continue
            ids_d  = _parse_packed_sint(bytes(ir))
            lats_d = _parse_packed_sint(bytes(lr))
            lons_d = _parse_packed_sint(bytes(or_))
            acc_id = acc_lat = acc_lon = 0
            for did, dlat, dlon in zip(ids_d, lats_d, lons_d):
                acc_id += did; acc_lat += dlat; acc_lon += dlon
                if acc_id in needed_nodes:
                    node_coords[acc_id] = (
                        (lat_off + acc_lat * gran) / 1e9,
                        (lon_off + acc_lon * gran) / 1e9,
                    )

        if verbose and block_i % 500 == 0:
            print(f"  [P2] block={block_i} "
                  f"coords={len(node_coords):,}/{len(needed_nodes):,}", flush=True)

    return node_coords


# ── Utility ────────────────────────────────────────────────────────────
def haversine_km(la, loa, lb, lob) -> float:
    R = 6371.0088
    p1, p2 = math.radians(la), math.radians(lb)
    dp, dl = math.radians(lb - la), math.radians(lob - loa)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(max(0.0, a)))

def parse_maxspeed(s: str) -> float | None:
    try:
        s = str(s).strip().lower()
        nums = "".join(c for c in s if c.isdigit() or c == ".")
        v = float(nums) if nums else None
        if v and "mph" in s: v *= 1.609
        return v if v and 5 < v < 140 else None
    except Exception:
        return None


# ── Pass 3: Build SQLite ───────────────────────────────────────────────
def pass3_db(db_path: str, highway_ways: dict, node_coords: dict,
             verbose: bool = True) -> dict:
    """Write osm_nodes and osm_edges to SQLite, return stats dict."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-1024000")
    conn.execute("PRAGMA temp_store=MEMORY")

    conn.execute("DROP TABLE IF EXISTS osm_nodes")
    conn.execute("DROP TABLE IF EXISTS osm_edges")
    conn.execute("CREATE TABLE osm_nodes(node_id INTEGER PRIMARY KEY, lat REAL NOT NULL, lon REAL NOT NULL)")
    conn.execute("""CREATE TABLE osm_edges(
        edge_id    TEXT PRIMARY KEY,
        way_id     INTEGER NOT NULL,
        from_node  INTEGER NOT NULL,
        to_node    INTEGER NOT NULL,
        highway    TEXT,
        name       TEXT,
        oneway     INTEGER DEFAULT 0,
        length_km  REAL NOT NULL,
        speed_kmh  REAL NOT NULL,
        lat_a REAL, lon_a REAL,
        lat_b REAL, lon_b REAL
    )""")

    if verbose: print(f"  [P3] inserting {len(node_coords):,} nodes...", flush=True)
    BATCH = 200_000
    buf = []
    for nid, (la, lo) in node_coords.items():
        buf.append((nid, la, lo))
        if len(buf) >= BATCH:
            conn.executemany("INSERT INTO osm_nodes VALUES(?,?,?)", buf)
            conn.commit(); buf = []
    if buf:
        conn.executemany("INSERT INTO osm_nodes VALUES(?,?,?)", buf)
        conn.commit()

    if verbose: print("  [P3] building edges...", flush=True)
    edge_buf = []; total_e = 0; skipped = 0
    for wid, w in highway_ways.items():
        refs   = w["refs"]; hw = w["highway"]
        ow     = w["oneway"]
        oneway = 1 if ow in ("yes", "1", "true") else 0
        rev_only = ow == "-1"
        speed  = parse_maxspeed(w["maxspeed"]) or HIGHWAY_SPEEDS.get(hw, 30)
        name   = w["name"]

        for i in range(len(refs) - 1):
            a, b = refs[i], refs[i + 1]
            ca = node_coords.get(a); cb = node_coords.get(b)
            if ca is None or cb is None: skipped += 1; continue
            dist = haversine_km(ca[0], ca[1], cb[0], cb[1])
            if dist < 0.0005: continue
            ws = str(wid)
            if not rev_only:
                edge_buf.append((f"{a}_{b}_{ws}", wid, a, b, hw, name,
                                 oneway, round(dist, 6), speed,
                                 ca[0], ca[1], cb[0], cb[1]))
                total_e += 1
            if not oneway or rev_only:
                edge_buf.append((f"{b}_{a}_{ws}", wid, b, a, hw, name,
                                 oneway, round(dist, 6), speed,
                                 cb[0], cb[1], ca[0], ca[1]))
                total_e += 1

            if len(edge_buf) >= BATCH:
                conn.executemany(
                    "INSERT OR IGNORE INTO osm_edges VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    edge_buf)
                conn.commit(); edge_buf = []
                if verbose:
                    print(f"  [P3] edges={total_e:,}", flush=True)

    if edge_buf:
        conn.executemany(
            "INSERT OR IGNORE INTO osm_edges VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            edge_buf)
        conn.commit()

    if verbose: print("  [P3] creating indexes...", flush=True)
    conn.execute("CREATE INDEX idx_ef  ON osm_edges(from_node)")
    conn.execute("CREATE INDEX idx_et  ON osm_edges(to_node)")
    conn.execute("CREATE INDEX idx_nll ON osm_nodes(lat, lon)")
    conn.execute("CREATE INDEX idx_ehw ON osm_edges(highway)")
    conn.commit()

    cur = conn.cursor()
    stats = {
        "nodes": cur.execute("SELECT COUNT(*) FROM osm_nodes").fetchone()[0],
        "edges": cur.execute("SELECT COUNT(*) FROM osm_edges").fetchone()[0],
        "skipped": skipped,
    }
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit(); conn.close()
    return stats


# ── Main ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="OSM PBF → SQLite 路網建置")
    ap.add_argument("--pbf", required=True, help="OSM PBF 檔案路徑")
    ap.add_argument("--db",  default="taiwan_osm.db", help="輸出 SQLite 路徑")
    ap.add_argument("--cache-dir", default="/tmp", help="中間快取目錄")
    args = ap.parse_args()

    pbf = args.pbf
    if not Path(pbf).exists():
        sys.exit(f"找不到 PBF 檔案：{pbf}")

    cache_ways  = Path(args.cache_dir) / "osm_ways.pkl"
    cache_nodes = Path(args.cache_dir) / "osm_nodes.pkl"
    t0 = time.time()

    if cache_ways.exists():
        print("載入快取 ways...", flush=True)
        with open(cache_ways, "rb") as f:
            highway_ways, needed_nodes = pickle.load(f)
    else:
        print("Pass 1: 解析 highway ways...", flush=True)
        highway_ways, needed_nodes = pass1_ways(pbf)
        with open(cache_ways, "wb") as f:
            pickle.dump((highway_ways, needed_nodes), f, protocol=4)
    print(f"  ways={len(highway_ways):,}  needed_nodes={len(needed_nodes):,}  "
          f"t={time.time()-t0:.0f}s", flush=True)

    if cache_nodes.exists():
        print("載入快取 node coords...", flush=True)
        with open(cache_nodes, "rb") as f:
            node_coords = pickle.load(f)
    else:
        print("Pass 2: 收集節點座標...", flush=True)
        node_coords = pass2_nodes(pbf, needed_nodes)
        with open(cache_nodes, "wb") as f:
            pickle.dump(node_coords, f, protocol=4)
    print(f"  node_coords={len(node_coords):,}  t={time.time()-t0:.0f}s", flush=True)

    print("Pass 3: 寫入 SQLite...", flush=True)
    stats = pass3_db(args.db, highway_ways, node_coords)
    print(f"\n建圖完成！")
    print(f"  nodes   : {stats['nodes']:,}")
    print(f"  edges   : {stats['edges']:,}")
    print(f"  skipped : {stats['skipped']:,}")
    print(f"  db      : {Path(args.db).resolve()}")
    print(f"  time    : {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
