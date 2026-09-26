#!/usr/bin/env python3
"""Stream-profile GH Archive .json.gz files without loading them fully into memory."""
import argparse, gzip, json
from collections import Counter, defaultdict
from pathlib import Path

def profile(path):
    path=Path(path); types=Counter(); top=Counter(); payload=defaultdict(Counter)
    ids=set(); repos=set(); actors=set(); n=0; first=last=None
    with gzip.open(path,"rt",encoding="utf-8") as fh:
        for line in fh:
            if not line.strip(): continue
            e=json.loads(line); n+=1
            if e.get("id") is not None: ids.add(str(e["id"]))
            types[e.get("type","UNKNOWN")]+=1
            for k in e: top[k]+=1
            r=e.get("repo") or {}; a=e.get("actor") or {}
            if r.get("id") is not None: repos.add(str(r["id"]))
            if a.get("id") is not None: actors.add(str(a["id"]))
            ts=e.get("created_at")
            if ts:
                first=ts if first is None or ts<first else first
                last=ts if last is None or ts>last else last
            for k in (e.get("payload") or {}): payload[e.get("type","UNKNOWN")][k]+=1
    print(f"\nFILE: {path.name}")
    print(f"Compressed size: {path.stat().st_size/(1024**2):.2f} MB")
    print(f"Events: {n:,}")
    print(f"Unique event IDs: {len(ids):,}")
    print(f"Unique repositories: {len(repos):,}")
    print(f"Unique actors: {len(actors):,}")
    print(f"Time range: {first} -> {last}")
    print("\nEVENT TYPES")
    for k,v in types.most_common(): print(f"{k:40s}{v:12,d}")
    print("\nTOP-LEVEL KEYS")
    for k,v in top.most_common(): print(f"{k:25s}{v:12,d}")
    print("\nPAYLOAD KEYS BY EVENT TYPE")
    for et in sorted(payload):
        print(f"\n[{et}]")
        for k,v in payload[et].most_common(): print(f"  {k:35s}{v:12,d}")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("files",nargs="+")
    args=ap.parse_args()
    for f in args.files: profile(f)
