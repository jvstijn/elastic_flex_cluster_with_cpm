#!/usr/bin/env python3
"""Patch cpm-state-manager watcher: home_cluster_id routing semantics."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
J2 = ROOT / "ansible/roles/elastic_cpm/templates/watcher_cpm-state-manager.json.j2"

OLD_EXISTING_TAIL = """    int dotNs = topic.lastIndexOf('.');
    if (dotNs > 0 && topic.length() - dotNs == 3) {
      String topicNn = topic.substring(dotNs + 1);
      Map regCluster = (Map)registryMap.get(cid);
      if (regCluster != null) {
        String cname = (String)regCluster.get('cluster_name');
        if (cname != null && cname.startsWith('cluster') && cname.length() >= 9) {
          String expectNn = cname.substring(7);
          if (expectNn.length() == 1) expectNn = '0' + expectNn;
          if (!topicNn.equals(expectNn)) {
            Map del = new HashMap();
            del.put('state_id', hit._id);
            stateDeletions.add(del);
            continue;
          }
        }
      }
    }

    String cu = src.cluster_uuid != null ? (String)src.cluster_uuid : '';
    boolean onInactive = inactiveClusterIds.contains(cid)
      || (!cu.isEmpty() && inactiveClusterIds.contains(cu));
    if (onInactive) {
      orphanedTopics.add(topic);
      continue;
    }

    if ('dedicated'.equals(ptype)) continue;

    if (!stateCatchallTopics.containsKey(cid)) {
      stateCatchallTopics.put(cid, new ArrayList());
    }
    ((List)stateCatchallTopics.get(cid)).add(topic);
    stateAllTopics.add(topic);"""

NEW_EXISTING_TAIL = r"""    if ('dedicated'.equals(ptype)) {
      if (!managedTopics.contains(topic) && !seenClusters.contains(cid)) {
        Map dCluster = (Map)registryMap.get(cid);
        if (dCluster != null) {
          String ddc = (String)dCluster.get('dc');
          if (ddc == null || ddc.isEmpty()) ddc = 'default';
          seenClusters.add(cid);
          lockedDedicatedClusters.add(cid);
          managedTopics.add(topic);
          Map dst = new HashMap();
          dst.put('data_stream_type', streamType);
          dst.put('dataset', dataset);
          dst.put('namespace', ns);
          dst.put('pipeline_type', 'dedicated');
          dst.put('pipeline_id', ddc + '_cpm-dedicated-' + cid);
          dst.put('cluster_id', cid);
          dst.put('dc', ddc);
          dst.put('topic', topic);
          dst.put('updated_at', ts);
          stateEntries.add(dst);
        }
      }
      continue;
    }

    String home = src.home_cluster_id != null ? (String)src.home_cluster_id : cid;
    topicHomeCluster.put(topic, home);

    boolean homeInactive = inactiveClusterIds.contains(home);
    if (!homeInactive) {
      for (def regHit : ctx.payload.registry.hits.hits) {
        Map regSrc = regHit._source;
        String regCid = (String)regSrc.cluster_id;
        String regCu = regSrc.cluster_uuid != null ? (String)regSrc.cluster_uuid : '';
        if (!home.equals(regCid) && !home.equals(regCu)) continue;
        if (regSrc.active != null && !(boolean)regSrc.active) {
          homeInactive = true;
        }
        break;
      }
    }

    if (homeInactive) {
      orphanedTopics.add(topic);
      stateAllTopics.add(topic);
      continue;
    }

    if (!stateCatchallTopics.containsKey(home)) {
      stateCatchallTopics.put(home, new ArrayList());
    }
    List homeTopics = (List)stateCatchallTopics.get(home);
    if (!homeTopics.contains(topic)) homeTopics.add(topic);
    stateAllTopics.add(topic);"""

OLD_ORPHAN = """// Re-home catchall topics from deactivated clusters onto the first active cluster
if (!orphanedTopics.isEmpty() && !registryMap.isEmpty()) {
  String defaultCid = null;
  for (def hit : ctx.payload.registry.hits.hits) {
    Map src = hit._source;
    if (src.active != null && !(boolean)src.active) continue;
    defaultCid = (String)src.cluster_id;
    break;
  }
  if (defaultCid != null) {
    if (!clusterTopics.containsKey(defaultCid)) {
      clusterTopics.put(defaultCid, new ArrayList());
    }
    List dt = (List)clusterTopics.get(defaultCid);
    for (int oi = 0; oi < orphanedTopics.size(); oi++) {
      String topic = (String)orphanedTopics.get(oi);
      if (!dt.contains(topic)) dt.add(topic);
    }
  }
}"""

NEW_ORPHAN = r"""// Re-home catchall topics whose home cluster is unavailable
if (!orphanedTopics.isEmpty() && !registryMap.isEmpty()) {
  String defaultCid = null;
  for (def hit : ctx.payload.registry.hits.hits) {
    Map src = hit._source;
    if (src.active != null && !(boolean)src.active) continue;
    defaultCid = (String)src.cluster_id;
    break;
  }
  if (defaultCid != null) {
    if (!clusterTopics.containsKey(defaultCid)) {
      clusterTopics.put(defaultCid, new ArrayList());
    }
    List dt = (List)clusterTopics.get(defaultCid);
    for (int oi = 0; oi < orphanedTopics.size(); oi++) {
      String topic = (String)orphanedTopics.get(oi);
      String home = (String)topicHomeCluster.get(topic);
      boolean homeActive = false;
      if (home != null && registryMap.containsKey(home)) {
        homeActive = true;
      }
      if (homeActive) continue;
      if (!dt.contains(topic)) dt.add(topic);
    }
  }
}"""

OLD_CATCHALL_ST = """    st.put('cluster_id', cid);
    st.put('dc', dc);
    st.put('topic', topic);
    st.put('updated_at', ts);
    stateEntries.add(st);"""

NEW_CATCHALL_ST = r"""    st.put('cluster_id', cid);
    st.put('dc', dc);
    st.put('topic', topic);
    String homeCid = (String)topicHomeCluster.get(topic);
    if (homeCid != null && !homeCid.isEmpty()) {
      st.put('home_cluster_id', homeCid);
      if (!cid.equals(homeCid)) {
        st.put('previous_cluster_id', homeCid);
      }
    }
    st.put('updated_at', ts);
    stateEntries.add(st);"""


def encode_painless(src: str) -> str:
    return json.dumps(src)[1:-1]  # escape for JSON string


def decode_painless(escaped: str) -> str:
    return json.loads(f'"{escaped}"')


def main() -> int:
    text = J2.read_text()
    m = re.search(r'"source": "((?:\\.|[^"\\])*)"\s*,\s*\n\s*"lang": "painless"', text)
    if not m:
        print("Could not find painless source", file=sys.stderr)
        return 1
    src = decode_painless(m.group(1))

    if "Map topicHomeCluster = new HashMap();" not in src:
        src = src.replace(
            "List orphanedTopics = new ArrayList();\n\nif (ctx.payload.existing_state",
            "List orphanedTopics = new ArrayList();\nMap topicHomeCluster = new HashMap();\n\nif (ctx.payload.existing_state",
            1,
        )

    if OLD_EXISTING_TAIL not in src:
        if "topicHomeCluster.put(topic, home)" in src:
            print("existing-state already patched")
        else:
            print("existing-state block not found", file=sys.stderr)
            return 1
    else:
        src = src.replace(OLD_EXISTING_TAIL, NEW_EXISTING_TAIL, 1)

    if OLD_ORPHAN not in src:
        if "homeActive" in src:
            print("orphan block already patched")
        else:
            print("orphan block not found", file=sys.stderr)
            return 1
    else:
        src = src.replace(OLD_ORPHAN, NEW_ORPHAN, 1)

    if OLD_CATCHALL_ST not in src:
        if "home_cluster_id" in src:
            print("catchall st already patched")
        else:
            print("catchall st block not found", file=sys.stderr)
            return 1
    else:
        src = src.replace(OLD_CATCHALL_ST, NEW_CATCHALL_ST, 1)

    # Monitoring: record home cluster at first discovery
    old_mon_put = (
        "    if (cl != null) {\n"
        "      Map dm = new HashMap();\n"
        "      dm.put('cluster_id', (String)cl.get('cluster_id'));\n"
        "      dm.put('namespace', ns);\n"
        "      dm.put('data_stream_type', streamType);\n"
        "      dm.put('dataset', dataset);\n"
        "      datasetClusterMap.put(streamType + '|' + dataset + '|' + ns, dm);\n"
        "    }"
    )
    new_mon_put = (
        "    if (cl != null) {\n"
        "      String monHome = (String)cl.get('cluster_id');\n"
        "      topicHomeCluster.put(topic, monHome);\n"
        "      Map dm = new HashMap();\n"
        "      dm.put('cluster_id', monHome);\n"
        "      dm.put('namespace', ns);\n"
        "      dm.put('data_stream_type', streamType);\n"
        "      dm.put('dataset', dataset);\n"
        "      datasetClusterMap.put(streamType + '|' + dataset + '|' + ns, dm);\n"
        "    }"
    )
    if old_mon_put not in src:
        print("monitoring put block not found", file=sys.stderr)
        return 1
    src = src.replace(old_mon_put, new_mon_put, 1)

    # Merge: route new discoveries to home cluster when active
    old_merge_loop = (
        "  String topic = streamType + '-' + dataset + '-' + ns;\n"
        "  if ('filebeat'.equals(streamType)) topic = 'filebeat';\n"
        "  if (!clusterTopics.containsKey(cid)) {\n"
        "    clusterTopics.put(cid, new ArrayList());\n"
        "  }\n"
        "  ((List)clusterTopics.get(cid)).add(topic);\n"
        "}"
    )
    new_merge_loop = (
        "  String topic = streamType + '-' + dataset + '-' + ns;\n"
        "  if ('filebeat'.equals(streamType)) topic = 'filebeat';\n"
        "  if (stateAllTopics.contains(topic)) continue;\n"
        "  String home = topicHomeCluster.get(topic) != null ? (String)topicHomeCluster.get(topic) : cid;\n"
        "  topicHomeCluster.put(topic, home);\n"
        "  boolean homeInactive = inactiveClusterIds.contains(home);\n"
        "  if (!homeInactive && !registryMap.containsKey(home)) {\n"
        "    homeInactive = true;\n"
        "  }\n"
        "  if (homeInactive) {\n"
        "    if (!orphanedTopics.contains(topic)) orphanedTopics.add(topic);\n"
        "    stateAllTopics.add(topic);\n"
        "    continue;\n"
        "  }\n"
        "  if (!clusterTopics.containsKey(home)) {\n"
        "    clusterTopics.put(home, new ArrayList());\n"
        "  }\n"
        "  List ht = (List)clusterTopics.get(home);\n"
        "  if (!ht.contains(topic)) ht.add(topic);\n"
        "  stateAllTopics.add(topic);\n"
        "}"
    )
    if old_merge_loop not in src:
        print("merge loop not found", file=sys.stderr)
        return 1
    src = src.replace(old_merge_loop, new_merge_loop, 1)

    new_escaped = encode_painless(src)
    text = text[: m.start(1)] + new_escaped + text[m.end(1) :]
    J2.write_text(text)
    print("patched", J2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
