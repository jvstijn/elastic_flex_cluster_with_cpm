#!/usr/bin/env python3
"""Explain why a cluster has no dedicated pipeline.

A dedicated pipeline only ever comes from a routing suggestion (or a manual
stream lock). The cpm-routing-advisor watcher decides that, and it stays silent
about the streams it drops. This replays its logic read-only against the live
data and reports, per cluster, whether it should get a dedicated pipeline and if
not, which condition it fell over.

Nothing is written. Safe to run against production.

Only stdlib is used.

Examples:
  ./cpm_diagnose_dedicated.py --host https://es-central-01:9200 --insecure
  ./cpm_diagnose_dedicated.py --host https://localhost:9200 --insecure --streams 25
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
import urllib.error
import urllib.request

MONITORING = "monitoring:.monitoring-es-8-*"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default="https://localhost:9200")
    p.add_argument("--user", default="elastic")
    p.add_argument("--password", default="", help="defaults to $ELASTIC_PASSWORD")
    p.add_argument("--monitoring-index", default=MONITORING,
                   help="must match cpm_monitoring_index of this environment")
    p.add_argument("--streams", type=int, default=15,
                   help="how many streams to list in the ranking")
    p.add_argument("--insecure", action="store_true")
    return p.parse_args()


def make_opener(insecure: bool):
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))


class ES:
    def __init__(self, args):
        self.base = args.host.rstrip("/")
        self.op = make_opener(args.insecure)
        pw = args.password or os.environ.get("ELASTIC_PASSWORD", "")
        if not pw:
            sys.exit("Geen wachtwoord: gebruik --password of zet ELASTIC_PASSWORD.")
        self.auth = base64.b64encode(f"{args.user}:{pw}".encode()).decode()

    def post(self, path, body=None):
        req = urllib.request.Request(self.base + path,
                                     data=json.dumps(body).encode() if body else None,
                                     method="POST" if body else "GET")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Basic " + self.auth)
        try:
            with self.op.open(req, timeout=90) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            return {"error": e.read().decode()[:300], "status": e.code}

    def search(self, index, body):
        return self.post(f"/{index}/_search", body)


def parse_backing_index(iname: str):
    """Same parsing the advisor does; returns (type, dataset, namespace) or None."""
    if not iname.startswith(".ds-"):
        return None
    after = iname[4:]
    if not (after.startswith("logs-") or after.startswith("metrics-")
            or after.startswith("traces-")):
        return None
    idx = after
    p = idx.rfind("-")
    if p <= 0:
        return None
    idx = idx[:p]                      # generatie eraf
    p = idx.rfind("-")
    if p <= 0:
        return None
    idx = idx[:p]                      # datum eraf
    fd = idx.find("-")
    if fd <= 0:
        return None
    stream_type = idx[:fd]
    if stream_type not in ("logs", "metrics", "traces"):
        return None
    rest = idx[fd + 1:]
    nd = rest.rfind("-")
    if nd <= 0:                        # let op: de advisor slaat dit over,
        return None                    # de state-manager vult 'default' aan
    return stream_type, rest[:nd], rest[nd + 1:]


def topic_excluded(topic: str, patterns) -> bool:
    for pat in patterns:
        if "*" not in pat:
            if topic == pat:
                return True
            continue
        star = pat.index("*")
        pre, suf = pat[:star], pat[star + 1:]
        if not pre:
            if topic.endswith(suf):
                return True
        elif not suf:
            if topic.startswith(pre):
                return True
        elif topic.startswith(pre) and topic.endswith(suf):
            return True
    return False


def collect(es: ES, args):
    out = {}
    out["registry"] = [h["_source"] for h in
                       es.search("cpm-cluster-registry", {"size": 64}).get("hits", {}).get("hits", [])]
    sc = es.search("cpm-scores", {"size": 1, "sort": [{"scored_at": "desc"}]})
    hits = sc.get("hits", {}).get("hits", [])
    out["scores"] = hits[0]["_source"] if hits else None
    out["state"] = [h["_source"] for h in
                    es.search("cpm-pipeline-state", {"size": 500}).get("hits", {}).get("hits", [])]
    out["suggestions"] = [h["_source"] for h in
                          es.search("cpm-routing-suggestions", {"size": 100}).get("hits", {}).get("hits", [])]
    out["exclusions"] = [h["_source"].get("topic_pattern") for h in
                         es.search("cpm-stream-exclusions", {"size": 200}).get("hits", {}).get("hits", [])
                         if h["_source"].get("topic_pattern")]
    locks = es.search("cpm-routing-config", {"size": 200}).get("hits", {}).get("hits", [])
    out["locks"] = [h["_source"] for h in locks if h["_source"].get("locked")]
    return out


def index_rates(es: ES, index: str):
    body = {
        "size": 0,
        "query": {"bool": {"filter": [
            {"match_phrase": {"event.dataset": "elasticsearch.index"}},
            {"range": {"@timestamp": {"gte": "now-2h"}}},
            {"bool": {"minimum_should_match": 1, "should": [
                {"wildcard": {"elasticsearch.index.name": ".ds-logs-*"}},
                {"wildcard": {"elasticsearch.index.name": ".ds-metrics-*"}},
                {"wildcard": {"elasticsearch.index.name": ".ds-traces-*"}},
            ]}},
        ]}},
        "aggs": {"by_cluster": {
            "terms": {"field": "elasticsearch.cluster.id", "size": 32},
            "aggs": {"by_index": {
                "terms": {"field": "elasticsearch.index.name", "size": 2048},
                "aggs": {"over_time": {
                    "date_histogram": {"field": "@timestamp", "fixed_interval": "1h"},
                    "aggs": {"max_total": {"max": {
                        "field": "elasticsearch.index.total.indexing.index_total"}}},
                }}},
            }}},
    }
    return es.search(index, body)


def main() -> int:
    args = parse_args()
    es = ES(args)
    d = collect(es, args)

    if not d["scores"]:
        print("GEEN cpm-scores document. De advisor heeft dan geen clusterlijst en")
        print("stelt niets voor. Draai eerst cpm-scoring; blijven de scores 0, dan")
        print("ontbreken de ML-forecasts.")
        return 1

    score_map = {}                      # uuid -> (cluster_id, score)
    for c in d["scores"]["clusters"]:
        score_map[c["cluster_uuid"]] = (c["cluster_id"], float(c["total_score"]))

    reg_by_id = {r.get("cluster_id"): r for r in d["registry"]}
    name_of = {r.get("cluster_id"): r.get("cluster_name") or r.get("cluster_id")
                for r in d["registry"]}
    ded_by_cluster = {s["cluster_id"]: s for s in d["state"]
                      if s.get("pipeline_type") == "dedicated"}
    cat_clusters = {s["cluster_id"] for s in d["state"]
                    if s.get("pipeline_type") == "catchall"}

    locked_topics = set()
    for l in d["locks"]:
        if not l.get("cluster_id"):
            continue
        locked_topics.add("%s-%s-%s" % (l.get("data_stream_type", "logs"),
                                        l.get("dataset", ""),
                                        l.get("namespace", "default")))

    # ---- rates, exact zoals de advisor ze berekent
    agg = index_rates(es, args.monitoring_index)
    if "aggregations" not in agg:
        print("Monitoring niet leesbaar via", args.monitoring_index)
        print(json.dumps(agg)[:400])
        return 1

    rates = {}                          # (uuid, key) -> rate
    single_bucket = 0
    no_growth = 0
    for cb in agg["aggregations"]["by_cluster"]["buckets"]:
        uuid = cb["key"]
        for ib in cb["by_index"]["buckets"]:
            parsed = parse_backing_index(ib["key"])
            if not parsed:
                continue
            st, ds, ns = parsed
            tb = ib["over_time"]["buckets"]
            delta = 0
            if len(tb) >= 2:
                v0 = tb[0]["max_total"]["value"]
                v1 = tb[-1]["max_total"]["value"]
                if v0 is not None and v1 is not None and v1 - v0 > 0:
                    delta = int(v1 - v0)
                else:
                    no_growth += 1
            else:
                single_bucket += 1
            k = (uuid, "%s|%s|%s" % (st, ds, ns))
            rates[k] = rates.get(k, 0) + delta

    streams = []
    dropped_zero = dropped_excl = dropped_lock = 0
    for (uuid, key), rate in rates.items():
        st, ds, ns = key.split("|")
        topic = "%s-%s-%s" % (st, ds, ns)
        if rate <= 0:
            dropped_zero += 1
            continue
        if topic_excluded(topic, d["exclusions"]):
            dropped_excl += 1
            continue
        if topic in locked_topics:
            dropped_lock += 1
            continue
        streams.append({"topic": topic, "rate": rate, "uuid": uuid})
    streams.sort(key=lambda s: -s["rate"])

    n = len(score_map)
    top = streams[:min(n, len(streams))]

    clusters = sorted(({"uuid": u, "cluster_id": v[0], "score": v[1]}
                       for u, v in score_map.items()), key=lambda c: c["score"])

    # ---- greedy toewijzing naspelen
    used, vacating, skipped, sugg = set(), {}, [], []
    for s in top:
        if s["uuid"] not in score_map:
            continue
        src_score = score_map[s["uuid"]][1]
        cand = [c for c in clusters if c["uuid"] not in used]
        if not cand:
            break
        best_score = min(c["score"] for c in cand)
        best = next((c for c in cand if c["score"] == best_score and c["uuid"] == s["uuid"]),
                    next(c for c in cand if c["score"] == best_score))
        used.add(best["uuid"])
        if best["uuid"] == s["uuid"]:
            skipped.append((s, best, src_score, "lokaal"))
        elif best["score"] < src_score:
            vacating[s["uuid"]] = vacating.get(s["uuid"], 0) + s["rate"]
            sugg.append((s, best, "verplaatsing"))
        else:
            skipped.append((s, best, src_score, "doel zwaarder"))

    for s, best, src_score, why in skipped:
        if why == "lokaal":
            sugg.append((s, best, "lokaal dedicated"))
        elif best["uuid"] in vacating and vacating[best["uuid"]] > s["rate"]:
            sugg.append((s, best, "swap"))

    # ---- rapport
    print("=" * 78)
    print("STREAMS met een meetbare rate (venster now-2h, twee uurbuckets)")
    print("=" * 78)
    if not streams:
        print("  GEEN. Zonder rate stelt de advisor niets voor en ontstaat er nooit")
        print("  een dedicated pipeline. Zie de tellers onderaan.")
    for i, s in enumerate(streams[:args.streams]):
        cid = score_map.get(s["uuid"], ("?", 0))[0]
        cid = name_of.get(cid, cid)
        mark = "  <- top-N" if i < len(top) else ""
        print("  %2d. %-42s %10d evt  op %s%s" % (i + 1, s["topic"], s["rate"], cid, mark))
    if len(streams) > args.streams:
        print("  ... en nog %d" % (len(streams) - args.streams))
    print()
    print("  clusters: %d   streams met rate>0: %d   top-N = min(beide) = %d"
          % (n, len(streams), len(top)))
    print("  overgeslagen: rate 0 -> %d, uitgesloten -> %d, gelockt -> %d"
          % (dropped_zero, dropped_excl, dropped_lock))
    print("  reeksen met maar 1 uurbucket: %d   zonder groei: %d"
          % (single_bucket, no_growth))

    spread = max(c["score"] for c in clusters) - min(c["score"] for c in clusters)
    print()
    if all(c["score"] == 0.0 for c in clusters):
        print("  LET OP: alle scores zijn 0. De ML-forecasts leveren niets, dus de")
        print("  advisor kan geen zwaar van licht onderscheiden. Controleer of de")
        print("  datafeeds draaien en of ze de juiste monitoring-index lezen.")
    elif spread < 0.5:
        print("  LET OP: de scores liggen binnen %.2f punt van elkaar. De advisor" % spread)
        print("  verplaatst alleen naar een LICHTER cluster (tgtScore < srcScore).")
        print("  Bij vrijwel gelijke scores gaat die vlieger nooit op, en blijft")
        print("  alleen de tie-break over die een stream op zijn eigen cluster laat.")
        print("  Een cluster dat geen enkele drukke stream host krijgt dan niets.")

    print()
    print("=" * 78)
    print("PER CLUSTER")
    print("=" * 78)
    print("  %-22s %7s  %-9s %-9s %s" % ("cluster", "score", "catchall", "dedicated", "oordeel"))
    would_get = {}
    for s, best, why in sugg:
        would_get.setdefault(best["cluster_id"], (s["topic"], why))

    for c in clusters:
        cid = c["cluster_id"]
        reg = reg_by_id.get(cid, {})
        has_ded = cid in ded_by_cluster
        verdict = []
        if has_ded:
            verdict.append("ok: " + ded_by_cluster[cid].get("topic", "?"))
        elif cid in would_get:
            t, why = would_get[cid]
            verdict.append("advisor stelt voor (%s: %s), nog niet in state" % (why, t))
        else:
            if len(top) < n:
                verdict.append("te weinig streams met rate>0 (%d voor %d clusters)"
                               % (len(top), n))
            elif c["uuid"] in used:
                verdict.append("als doel gekozen maar niet zwaarder->lichter, en geen swap")
            else:
                verdict.append("nooit als doel gekozen")
            if not any(s["uuid"] == c["uuid"] for s in top):
                verdict.append("host geen enkele top-N stream")
            if reg.get("active") is False:
                verdict.append("cluster staat op active=false")
            if not reg.get("ingest_hosts"):
                verdict.append("ingest_hosts ontbreekt in de registry")
        print("  %-22s %7.2f  %-9s %-9s %s"
              % (name_of.get(cid, cid), c["score"], "ja" if cid in cat_clusters else "nee",
                 "ja" if has_ded else "NEE", "; ".join(verdict)))

    print()
    print("=" * 78)
    print("HUIDIGE SUGGESTIES in cpm-routing-suggestions: %d" % len(d["suggestions"]))
    print("=" * 78)
    for s in d["suggestions"]:
        print("  %s-%s-%s -> %s" % (s.get("data_stream_type"), s.get("dataset"),
                                    s.get("namespace"), s.get("suggested_cluster_id")))
    if d["suggestions"] and len(ded_by_cluster) < len(d["suggestions"]):
        print()
        print("  Er zijn meer suggesties dan dedicated pipelines. De state-manager")
        print("  neemt per cluster maar één dedicated (seenClusters) en slaat een")
        print("  suggestie stil over als suggested_cluster_id niet als actief cluster")
        print("  in cpm-cluster-registry staat.")

    if d["exclusions"]:
        print()
        print("uitsluitingen actief:", ", ".join(sorted(d["exclusions"])))
    if locked_topics:
        print("stream locks actief:", ", ".join(sorted(locked_topics)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
