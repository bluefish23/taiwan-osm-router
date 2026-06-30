#!/usr/bin/env python3
"""Test event impact on routing."""
import sys, io, json, urllib.request
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def route(slat, slon, elat, elon, mode='balanced'):
    body = json.dumps({'start_lat':slat,'start_lon':slon,'end_lat':elat,'end_lon':elon,'mode':mode}).encode()
    req = urllib.request.Request('http://127.0.0.1:8000/route', data=body, headers={'Content-Type':'application/json'})
    return json.loads(urllib.request.urlopen(req).read())

def events():
    return json.loads(urllib.request.urlopen('http://127.0.0.1:8000/events').read())

evts = events()
print(f"=== 目前事件 ({len(evts)} 個) ===")
for e in evts:
    print(f"  [{e['event_type']}] sev={e.get('severity','')} r={e.get('radius_km','')}km - {e.get('description','')}")

print()
r1 = route(25.0478, 121.5170, 25.0777, 121.2327)
print("=== 路線1: 台北→桃機 (有事件) ===")
print(f"  距離: {r1['total_km']:.2f} km | 時間: {r1['total_min']:.2f} min | 成本: {r1['total_cost']:.2f} | 邊: {r1['num_edges']}")
print(f"  命中事件: {len(r1.get('matched_events',[]))}")
for ev in r1.get('matched_events',[]):
    print(f"    - {ev['event_type']}: {ev.get('description','')}")

print()
r2 = route(25.0478, 121.5170, 24.8015, 120.9716)
print("=== 路線2: 台北→新竹 (有事件) ===")
print(f"  距離: {r2['total_km']:.2f} km | 時間: {r2['total_min']:.2f} min | 成本: {r2['total_cost']:.2f} | 邊: {r2['num_edges']}")
print(f"  命中事件: {len(r2.get('matched_events',[]))}")
for ev in r2.get('matched_events',[]):
    print(f"    - {ev['event_type']}: {ev.get('description','')}")

print()
print("=" * 55)
print("=== 前後比較 ===")
print("=" * 55)
print(f"路線1 基準: 41.62km / 43.12min / cost 56.13")
print(f"路線1 事件: {r1['total_km']:.2f}km / {r1['total_min']:.2f}min / cost {r1['total_cost']:.2f}")
d1km = r1['total_km'] - 41.62
d1min = r1['total_min'] - 43.12
d1cost = r1['total_cost'] - 56.13
print(f"  -> 距離: {d1km:+.2f}km, 時間: {d1min:+.2f}min, 成本: {d1cost:+.2f} ({d1cost/56.13*100:+.0f}%)")
print()
print(f"路線2 基準: 86.60km / 65.64min / cost 87.69")
print(f"路線2 事件: {r2['total_km']:.2f}km / {r2['total_min']:.2f}min / cost {r2['total_cost']:.2f}")
d2km = r2['total_km'] - 86.60
d2min = r2['total_min'] - 65.64
d2cost = r2['total_cost'] - 87.69
print(f"  -> 距離: {d2km:+.2f}km, 時間: {d2min:+.2f}min, 成本: {d2cost:+.2f} ({d2cost/87.69*100:+.0f}%)")

print()
print("=== 測試 safest 模式 (風險放大) ===")
r3 = route(25.0478, 121.5170, 24.8015, 120.9716, mode='safest')
print(f"路線2 safest: {r3['total_km']:.2f}km / {r3['total_min']:.2f}min / cost {r3['total_cost']:.2f}")
print(f"  命中事件: {len(r3.get('matched_events',[]))}")
for ev in r3.get('matched_events',[]):
    print(f"    - {ev['event_type']}: {ev.get('description','')}")
