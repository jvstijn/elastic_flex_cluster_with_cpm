#!/usr/bin/env python3
"""Generate stack-monitoring test data for the coverage checker.

Writes one synthetic `event.dataset: elasticsearch.index` document per data
stream listed below into the `.monitoring-es-8-mb` data stream (the same place
Metricbeat writes), attributed to the active `central-cluster`, with an
`@timestamp` a few minutes ago (well inside the 24h window the coverage script
uses). Four datasets get a docs.count between 80000 and 100000; the rest get a
small count.

This makes `scripts/check_index_pipeline_coverage.py` report every dataset.
Only stdlib is used.

  ./gen_monitoring_testdata.py --insecure
"""
from __future__ import annotations

import argparse
import base64
import datetime
import json
import random
import ssl
import sys
import urllib.error
import urllib.request

MON_DS = ".monitoring-es-8-mb"

# Synthetic marker in the backing-index name (year 2000 / gen 000001) so this
# generator can safely delete_by_query its own previous docs and never touch
# real monitoring data. The date/gen are cosmetic for the coverage script.
MARKER_SUFFIX = "2000.01.01-000001"

# The three active clusters in cpm-cluster-registry (name -> cluster uuid).
CLUSTERS = [
    ("central-cluster", "CDs5L0stRsiSlgV2EPWjmg"),
    ("remote-a", "qwnBfYSwQ02Usrr8p9G-ZQ"),
    ("remote-b", "16AcRGCJSNmTqKkIHxYGvQ"),
]
CLUSTER_BY_NAME = {name: uuid for name, uuid in CLUSTERS}

# The four datasets that get a large docs.count (80000-100000), placed
# deliberately: one on central, one on remote-a, two on remote-b.
BIG_ASSIGN = {
    "logs-system.auth-default": "central-cluster",
    "metrics-endpoint.metadata_current_default": "remote-a",
    "logs-winlog.winlog-default": "remote-b",
    "logs-endpoint.events.process-default": "remote-b",
}

DATASETS = """
metrics-endpoint.metadata_current_default
logs-winlog.winlog-tst
logs-winlog.winlog-prd
logs-winlog.winlog-ont
logs-winlog.winlog-default
logs-winlog.winlog-acc
logs-winlog.terminalservices-tst
logs-winlog.terminalservices-prd
logs-winlog.terminalservices-ont
logs-winlog.terminalservices-default
logs-winlog.terminalservices-acc
logs-winlog.defender-tst
logs-winlog.defender-prd
logs-winlog.defender-ont
logs-winlog.defender-default
logs-winlog.defender-acc
logs-windows.windows_defender-tst
logs-windows.windows_defender-prd
logs-windows.windows_defender-ont
logs-windows.windows_defender-default
logs-windows.windows_defender-acc
logs-windows.powershell_operational-tst
logs-windows.powershell_operational-prd
logs-windows.powershell_operational-ont
logs-windows.powershell_operational-default
logs-windows.powershell_operational-acc
logs-windows.powershell-tst
logs-windows.powershell-prd
logs-windows.powershell-ont
logs-windows.powershell-default
logs-windows.powershell-acc
logs-windows.forwarded-tst
logs-windows.forwarded-prd
logs-windows.forwarded-ont
logs-windows.forwarded-default
logs-windows.forwarded-acc
logs-vmware
logs-system.system-tst
logs-system.system-prd
logs-system.system-ont
logs-system.system-default
logs-system.system-acc
logs-system.syslog-tst
logs-system.syslog-prd
logs-system.syslog-ont
logs-system.syslog-default
logs-system.syslog-acc
logs-system.security-tst
logs-system.security-prd
logs-system.security-ont
logs-system.security-default
logs-system.security-acc
logs-system.auth-tst
logs-system.auth-prd
logs-system.auth-ont
logs-system.auth-default
logs-system.auth-acc
logs-system.application-tst
logs-system.application-prd
logs-system.application-ont
logs-system.application-default
logs-system.application-acc
logs-postgresql.log-tst
logs-postgresql.log-prd
logs-postgresql.log-ont
logs-postgresql.log-default
logs-postgresql.log-acc
logs-osquery_manager.result-tst
logs-osquery_manager.result-prd
logs-osquery_manager.result-ont
logs-osquery_manager.result-default
logs-osquery_manager.result-acc
logs-osquery_manager.action.responses-tst
logs-osquery_manager.action.responses-prd
logs-osquery_manager.action.responses-ont
logs-osquery_manager.action.responses-default
logs-osquery_manager.action.responses-acc
logs-nexpose-tst
logs-nexpose-prd
logs-nexpose-ont
logs-nexpose-default
logs-nexpose-acc
logs-netwerk-services
logs-logstash.log-tst
logs-logstash.log-prd
logs-logstash.log-ont
logs-logstash.log-default
logs-logstash.log-acc
logs-logmanagement
logs-linlog.postfix-tst
logs-linlog.postfix-prd
logs-linlog.postfix-ont
logs-linlog.postfix-default
logs-linlog.postfix-acc
logs-linlog.cams3-tst
logs-linlog.cams3-prd
logs-linlog.cams3-ont
logs-linlog.cams3-default
logs-linlog.cams3-acc
logs-kubernetes.audit_logs-tst
logs-kubernetes.audit_logs-prd
logs-kubernetes.audit_logs-ont
logs-kubernetes.audit_logs-default
logs-kubernetes.audit_logs-acc
logs-infoblox_nios.log-tst
logs-infoblox_nios.log-prd
logs-infoblox_nios.log-ont
logs-infoblox_nios.log-default
logs-infoblox_nios.log-acc
logs-generic-tst
logs-generic-prd
logs-generic-ont
logs-generic-default
logs-generic-acc
logs-fortinet_fortimanager.log-tst
logs-fortinet_fortimanager.log-prd
logs-fortinet_fortimanager.log-ont
logs-fortinet_fortimanager.log-default
logs-fortinet_fortimanager.log-acc
logs-fortinet_fortigate.log-tst
logs-fortinet_fortigate.log-prd
logs-fortinet_fortigate.log-ont
logs-fortinet_fortigate.log-default
logs-fortinet_fortigate.log-acc
logs-fireeye.nx-tst
logs-fireeye.nx-prd
logs-fireeye.nx-ont
logs-fireeye.nx-default
logs-fireeye.nx-acc
logs-fim.event-tst
logs-fim.event-prd
logs-fim.event-ont
logs-fim.event-default
logs-fim.event-acc
logs-filestream.squid-tst
logs-filestream.squid-prd
logs-filestream.squid-ont
logs-filestream.squid-default
logs-filestream.squid-acc
logs-filestream.sap.securitycontrol-tst
logs-filestream.sap.securitycontrol-prd
logs-filestream.sap.securitycontrol-ont
logs-filestream.sap.securitycontrol-default
logs-filestream.sap.securitycontrol-acc
logs-filestream.generic-tst
logs-filestream.generic-purpleteaming
logs-filestream.generic-prd
logs-filestream.generic-ont
logs-filestream.generic-default
logs-filestream.generic-acc
logs-filestream.conjur-tst
logs-filestream.conjur-prd
logs-filestream.conjur-ont
logs-filestream.conjur-default
logs-filestream.conjur-acc
logs-extranet
logs-etw.winlog-tst
logs-etw.winlog-prd
logs-etw.winlog-ont
logs-etw.winlog-default
logs-etw.winlog-acc
logs-entityanalytics_ad.user-tst
logs-entityanalytics_ad.user-prd
logs-entityanalytics_ad.user-ont
logs-entityanalytics_ad.user-default
logs-entityanalytics_ad.user-acc
logs-entityanalytics_ad.entity-tst
logs-entityanalytics_ad.entity-prd
logs-entityanalytics_ad.entity-ont
logs-entityanalytics_ad.entity-default
logs-entityanalytics_ad.entity-acc
logs-entityanalytics_ad.device-tst
logs-entityanalytics_ad.device-prd
logs-entityanalytics_ad.device-ont
logs-entityanalytics_ad.device-default
logs-entityanalytics_ad.device-acc
logs-endpoint.events.security-tst
logs-endpoint.events.security-prd
logs-endpoint.events.security-ont
logs-endpoint.events.security-default
logs-endpoint.events.security-acc
logs-endpoint.events.security
logs-endpoint.events.registry-tst
logs-endpoint.events.registry-prd
logs-endpoint.events.registry-ont
logs-endpoint.events.registry-default
logs-endpoint.events.registry-acc
logs-endpoint.events.process-tst
logs-endpoint.events.process-prd
logs-endpoint.events.process-ont
logs-endpoint.events.process-default
logs-endpoint.events.process-acc
logs-endpoint.events.process
logs-endpoint.events.network-tst
logs-endpoint.events.network-prd
logs-endpoint.events.network-ont
logs-endpoint.events.network-default
logs-endpoint.events.network-acc
logs-endpoint.events.network
logs-endpoint.events.library-tst
logs-endpoint.events.library-prd
logs-endpoint.events.library-ont
logs-endpoint.events.library-default
logs-endpoint.events.library-acc
logs-endpoint.events.library
logs-endpoint.events.file-tst
logs-endpoint.events.file-prd
logs-endpoint.events.file-ont
logs-endpoint.events.file-default
logs-endpoint.events.file-dafault
logs-endpoint.events.file-acc
logs-endpoint.events.file
logs-endpoint.events.api-tst
logs-endpoint.events.api-prd
logs-endpoint.events.api-ont
logs-endpoint.events.api-default
logs-endpoint.events.api-acc
logs-endpoint.alerts-tst
logs-endpoint.alerts-prd
logs-endpoint.alerts-ont
logs-endpoint.alerts-default
logs-endpoint.alerts-acc
logs-elastic_agent.status_change-tst
logs-elastic_agent.status_change-prd
logs-elastic_agent.status_change-ont
logs-elastic_agent.status_change-default
logs-elastic_agent.status_change-acc
logs-elastic_agent.osquerybeat-tst
logs-elastic_agent.osquerybeat-prd
logs-elastic_agent.osquerybeat-ont
logs-elastic_agent.osquerybeat-default
logs-elastic_agent.osquerybeat-acc
logs-elastic_agent.osquerybeat
logs-elastic_agent.metricbeat-tst
logs-elastic_agent.metricbeat-prd
logs-elastic_agent.metricbeat-ont
logs-elastic_agent.metricbeat-fleetserver
logs-elastic_agent.metricbeat-default
logs-elastic_agent.metricbeat-acc
logs-elastic_agent.metricbeat
logs-elastic_agent.heartbeat-tst
logs-elastic_agent.heartbeat-prd
logs-elastic_agent.heartbeat-ont
logs-elastic_agent.heartbeat-default
logs-elastic_agent.heartbeat-acc
logs-elastic_agent.fleet_server-tst
logs-elastic_agent.fleet_server-prd
logs-elastic_agent.fleet_server-ont
logs-elastic_agent.fleet_server-fleetserver
logs-elastic_agent.fleet_server-default
logs-elastic_agent.fleet_server-acc
logs-elastic_agent.filebeat-tst
logs-elastic_agent.filebeat-prd
logs-elastic_agent.filebeat-ont
logs-elastic_agent.filebeat-default
logs-elastic_agent.filebeat-acc
logs-elastic_agent.filebeat
logs-elastic_agent.endpoint_security-tst
logs-elastic_agent.endpoint_security-prd
logs-elastic_agent.endpoint_security-ont
logs-elastic_agent.endpoint_security-default
logs-elastic_agent.endpoint_security-acc
logs-elastic_agent.endpoint_security
logs-elastic_agent.auditbeat-tst
logs-elastic_agent.auditbeat-prd
logs-elastic_agent.auditbeat-ont
logs-elastic_agent.auditbeat-default
logs-elastic_agent.auditbeat-acc
logs-elastic_agent-tst
logs-elastic_agent-prd
logs-elastic_agent-ont
logs-elastic_agent-fleetserver
logs-elastic_agent-default
logs-elastic_agent-acc
logs-elastic_agent
logs-docker.container_logs-tst
logs-docker.container_logs-prd
logs-docker.container_logs-ont
logs-docker.container_logs-default
logs-docker.container_logs-acc
logs-database
logs-cyberarkpas.audit-tst
logs-cyberarkpas.audit-prd
logs-cyberarkpas.audit-ont
logs-cyberarkpas.audit-default
logs-cyberarkpas.audit-acc
logs-checkpoint.firewall-tst
logs-checkpoint.firewall-prd
logs-checkpoint.firewall-ont
logs-checkpoint.firewall-default
logs-checkpoint.firewall-acc
logs-auditd_manager.auditd-tst
logs-auditd_manager.auditd-prd
logs-auditd_manager.auditd-ont
logs-auditd_manager.auditd-default
logs-auditd_manager.auditd-acc
logs-auditd.log-tst
logs-auditd.log-prd
logs-auditd.log-ont
logs-auditd.log-default
logs-auditd.log-acc
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="https://localhost:9200")
    p.add_argument("--user", default="elastic")
    p.add_argument("--password", default="changeme-elastic")
    p.add_argument("--ca-cert")
    p.add_argument("--insecure", action="store_true")
    return p.parse_args()


def make_call(host, user, pw, ca_cert, insecure):
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    elif insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
    base = host.rstrip("/")

    def call(method, path, body=None, raw=False):
        data = (body if raw else json.dumps(body).encode()) if body is not None else None
        if raw and isinstance(data, str):
            data = data.encode()
        req = urllib.request.Request(
            base + path, data=data, method=method,
            headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, context=ctx) as resp:
            return json.loads(resp.read().decode())

    return call


def main() -> int:
    args = parse_args()
    call = make_call(args.host, args.user, args.password, args.ca_cert, args.insecure)
    rnd = random.Random(20260630)

    datasets = []
    seen = set()
    for line in DATASETS.splitlines():
        name = line.strip()
        if name and name not in seen:
            seen.add(name)
            datasets.append(name)

    # idempotent self-cleanup: drop this generator's previous synthetic docs
    dbq = {"query": {"wildcard": {"elasticsearch.index.name": f".ds-*-{MARKER_SUFFIX}"}}}
    d = call("POST", f"/{MON_DS}*/_delete_by_query?refresh=true&conflicts=proceed", dbq)
    print(f"cleaned previous synthetic docs: {d.get('deleted', 0)}")

    now = datetime.datetime.now(datetime.timezone.utc)
    big_counts = {}
    dist = {name: 0 for name, _ in CLUSTERS}

    lines = []
    for i, ds in enumerate(datasets):
        ts = now - datetime.timedelta(seconds=300 + (i % 1800))  # 5-35 min ago, < 24h
        if ds in BIG_ASSIGN:
            cname = BIG_ASSIGN[ds]
            count = rnd.randint(80000, 100000)
            big_counts[ds] = (cname, count)
        else:
            cname = CLUSTERS[i % len(CLUSTERS)][0]  # round-robin
            count = rnd.randint(50, 2000)
        cid = CLUSTER_BY_NAME[cname]
        dist[cname] += 1
        idx_name = f".ds-{ds}-{MARKER_SUFFIX}"
        doc = {
            "@timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "event": {"dataset": "elasticsearch.index"},
            "elasticsearch": {
                "cluster": {"id": cid, "name": cname},
                "index": {"name": idx_name, "total": {"docs": {"count": count}}},
            },
        }
        lines.append(json.dumps({"create": {"_index": MON_DS}}))
        lines.append(json.dumps(doc))

    body = "\n".join(lines) + "\n"
    res = call("POST", "/_bulk?refresh=true", body, raw=True)
    errors = res.get("errors")
    n = len(res.get("items", []))
    print(f"datasets written: {n}   bulk errors: {errors}")
    if errors:
        for it in res["items"]:
            err = it.get("create", {}).get("error")
            if err:
                print("  ERROR:", json.dumps(err)[:300])
                break
        return 1
    print("distribution over active clusters:")
    for name, _ in CLUSTERS:
        print(f"  {name:16} {dist[name]} datasets")
    print(f"big datasets ({len(big_counts)}):")
    for ds, (cname, c) in big_counts.items():
        print(f"  {ds}: {c} docs on {cname}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
