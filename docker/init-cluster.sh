#!/bin/sh
set -eu

NODE_ID_2=11cdbc8d-6b87-4dad-9fc1-d550c5659830
NODE_ID_3=e4fbc6fe-87f0-4ac8-810e-fd8deee6ed89
NODE_ID_4=1f13956d-a64d-4073-9b41-8d5648059e9c
NODE_ID_5=ee820b61-a10d-4ed2-92c6-aebcd97740fb
NODE_ID_6=cc1b33e5-a223-4cea-9fa0-219ba3d9305e
NODE_ID_7=540187b7-6645-4227-8a02-751976069a03
NODE_ID_8=9f91de67-a94c-4134-b489-5ff8fb97f9b5
NODE_ID_9=a7bd05e5-4933-47f2-afe9-04081bdfe296
NODE_ID_10=f51336c9-247d-48d8-801f-8134545bcc9c

BOOTSTRAP=http://node1:8080

wait_for() {
  url=$1
  echo "waiting for $url ..."
  until curl -sf "$url" >/dev/null 2>&1; do
    sleep 2
  done
  echo "$url is up"
}

wait_for "$BOOTSTRAP/api/cluster/state"

echo "starting cluster on node1 ..."
curl -sf -X POST "$BOOTSTRAP/api/cluster/start" -H "Content-Type: application/json" >/dev/null
echo "cluster started"

i=2
while [ "$i" -le 10 ]; do
  eval node_id="\$NODE_ID_$i"
  target="http://node$i:8080"

  wait_for "$target/api/cluster/state"

  echo "adding node$i ($node_id) to cluster ..."
  curl -sf -X POST "$BOOTSTRAP/api/cluster/add-node" \
    -H "Content-Type: application/json" \
    -d "{\"baseUrl\":\"$target\",\"nodeId\":\"$node_id\"}" >/dev/null
  echo "node$i added"

  sleep 2
  i=$((i + 1))
done

# A node only serves reads/writes once it has seen ClusterInit itself, and only
# picks up the replication factor once it has processed a ReplicationFactorChange.
# Both travel by best-effort gossip, which a node added after the fact can miss
# entirely (gossip fans out at broadcast time and isn't replayed for late joiners).
# Calling both endpoints on every node directly makes this deterministic instead
# of depending on gossip having converged by the time you start using the cluster.
echo "marking every node initialized and setting replication factor to 3 ..."
i=1
while [ "$i" -le 10 ]; do
  target="http://node$i:8080"
  curl -sf -X POST "$target/api/cluster/start" -H "Content-Type: application/json" >/dev/null
  curl -sf -X POST "$target/api/cluster/set-replication-factor" \
    -H "Content-Type: application/json" \
    -d '{"replicationFactor":3}' >/dev/null
  i=$((i + 1))
done

echo "10-node cluster is initialized (replication factor 3)"
