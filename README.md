# DistributedKvStore

A distributed key-value store built from scratch in .NET 9, implementing the core mechanics of systems like DynamoDB and Cassandra:

- **Consistent hashing** for key ownership and data placement
- **Gossip-based membership** (epidemic broadcast, anti-entropy)
- **Replication** with configurable replication factor and last-write-wins conflict resolution
- **Failure detection** via indirect/proxy heartbeats and a SWIM-style suspicion protocol
- **Automatic data rebalancing** on node join, node removal, and replication-factor changes

Each "node" is a standalone ASP.NET Core Web API process with its own SQLite database. Nodes discover and coordinate with each other purely over HTTP — there's no external coordination service (no ZooKeeper/etcd/Raft), and no message broker. This is a learning/reference project, not a production data store.

## Contents

- [Solution structure](#solution-structure)
- [Quick start (Docker)](#quick-start-docker)
- [Running locally with dotnet](#running-locally-with-dotnet)
- [Cluster administration API](#cluster-administration-api)
- [Key-value data API](#key-value-data-api)
- [Using the client library](#using-the-client-library)
- [Architecture notes](#architecture-notes)
- [Known limitations](#known-limitations)

## Solution structure

| Project | Description |
|---|---|
| `DistributedKvStore.Shared` | Shared types: enums (`NodeStatus`, `OperationType`, `GossipTopic`), models (`ClusterNodeInfo`, `ClusterState`, `KeyValueRecord`), DTOs, and the consistent-hashing implementation (`ConsistentHashRing` / `IHashRing`). |
| `DistributedKvStore.Node` | The server. ASP.NET Core Web API — one process is one cluster node, with its own SQLite database via EF Core. |
| `DistributedKvStore.Client` | A thin client library (`IDistributedClient` / `DistributedClient`) that resolves seed nodes, builds its own hash ring from `/api/cluster/state`, and routes reads/writes to the correct node(s), retrying across replicas on failure. |
| `DistributedKvStore.TestApi` | A small ASP.NET Core app wiring up `DistributedKvStore.Client` behind a simple external-facing REST surface — useful for poking at the cluster through one endpoint instead of talking to nodes directly. |

There is no automated test suite — verification is done by running multiple node instances and exercising the HTTP APIs directly.

## Quick start (Docker)

The fastest way to see the whole thing running is `docker-compose.yml` at the repo root, which builds and starts a 10-node cluster plus the `TestApi` front door, and initializes the cluster automatically.

```
docker compose build
docker compose up -d
```

A one-shot `cluster-init` container waits for all 10 nodes to come up, starts the cluster, adds every node, and sets the replication factor to 3. Once it exits, the cluster is ready:

- Nodes are reachable at `http://localhost:5001` .. `http://localhost:5010`
- `DistributedKvStore.TestApi` is reachable at `http://localhost:6000`

```
curl http://localhost:6000/cluster/state

curl -X POST http://localhost:6000/key \
  -H "Content-Type: application/json" \
  -d '{"key":"hello","value":"world"}'

curl http://localhost:6000/key/hello
```

Tear it down with `docker compose down` (add `-v` to also drop the per-node SQLite volumes).

## Running locally with dotnet

Build the whole solution:

```
dotnet build DistributedKvStore.sln
```

`DistributedKvStore.Node/Properties/launchSettings.json` defines three profiles (`Node1`, `Node2`, `Node3`) bound to ports 5001/5002/5003:

```
dotnet run --project DistributedKvStore.Node --launch-profile Node1
dotnet run --project DistributedKvStore.Node --launch-profile Node2
dotnet run --project DistributedKvStore.Node --launch-profile Node3
```

Additional nodes can be launched standalone with explicit config:

```
dotnet run --project DistributedKvStore.Node -- --Node:Id=<guid> --Node:BaseUrl=http://localhost:5004 --Node:DbPath=node4.db
```

Once nodes are up, initialize the cluster and add nodes via the HTTP API (see below), then run `DistributedKvStore.TestApi` against it — configure `Cluster:SeedNodes` in its `appsettings.json` (it defaults to the three local ports above).

There are no lint or test commands configured in this repo.

## Cluster administration API

Exposed by every node's `ClusterController` (`/api/cluster/*`):

| Endpoint | Description |
|---|---|
| `POST /api/cluster/start` | Marks the cluster initialized on this node and gossips it out so every node follows. Reads/writes 503 until a node has seen this. |
| `POST /api/cluster/uninitialize` | The inverse of `start`. |
| `POST /api/cluster/add-node` | Body: `{ "baseUrl": "...", "nodeId": "..." }` (`nodeId` optional). Onboards a new node — it starts `Joining` and pulls its owned hash range plus replica ranges from its ring successor. |
| `POST /api/cluster/remove-node` | Body: `{ "nodeId": "..." }`. Triggers a gossiped removal; peers rebalance the departing node's data before dropping it from their state. |
| `POST /api/cluster/set-replication-factor` | Body: `{ "replicationFactor": N }`. See [replication-factor semantics](#replication-factor-semantics). |
| `GET /api/cluster/state` | Returns the full cluster membership and replication factor as seen by this node. |
| `POST /api/cluster/shutdown` | Fans out a graceful shutdown to every node. |

There is no restart-node endpoint — a node that restarts rejoins from its persisted snapshot (see [Architecture notes](#architecture-notes)) rather than needing to be re-added.

## Key-value data API

Exposed by every node's `DataController` (`/api/data/*`), gated on the node being initialized:

| Endpoint | Description |
|---|---|
| `GET /api/data/{key}` | Read a key. `404` if not found, `503` if the node isn't initialized yet. |
| `POST /api/data` | Body: `{ "key": "...", "value": "..." }`. Write a key. |
| `PUT /api/data` | Body: `{ "key": "...", "value": "..." }`. Update a key. |
| `DELETE /api/data/{key}` | Tombstone-deletes a key. |

Writes/reads issued directly against a node aren't routed for you — you need to hash the key yourself or just always go through the client library / TestApi, which resolve the right node automatically.

`DistributedKvStore.TestApi` wraps both APIs behind a simpler surface on port 6000: `GET/POST/PUT/DELETE /key[/{key}]` for data, and `/cluster/state`, `/cluster/state/node?baseUrl=`, `/cluster/start`, `/cluster/shutdown`, `POST /cluster/nodes`, `DELETE /cluster/nodes/{nodeId}`, `POST /cluster/replication-factor`, `GET /cluster/health?baseUrl=` for cluster admin.

## Using the client library

```csharp
services.AddHttpClient("DistributedKvStore");
services.AddDistributedKvClient(new[] { "http://localhost:5001", "http://localhost:5002", "http://localhost:5003" });
```

```csharp
public class MyService(IDistributedClient client)
{
    public async Task Example()
    {
        await client.PutAsync("hello", "world");
        var value = await client.GetAsync("hello");   // "world"
        await client.UpdateAsync("hello", "updated");
        await client.DeleteAsync("hello");

        var state = await client.GetClusterStateAsync();
    }
}
```

The client resolves seed nodes, builds its own hash ring from `/api/cluster/state`, and routes each read/write to the correct node, retrying across replicas on failure.

## Architecture notes

- **Consistent hashing**: nodes sit on a ring by SHA-256-derived hash position. A key's primary owner is the first node clockwise whose position is >= the key's hash; replicas are the next `replicationFactor` successors after the primary.
- **Replication-factor semantics**: `replicationFactor` (`k`) means *additional* replicas beyond the primary — a range's full holder set is `k + 1` nodes total, not `k`. This convention is consistent across the write path, read path, and rebalancing logic.
- **Gossip**: every message carries a dedup'd `MessageId` and fans out epidemically (3 random peers, re-forwarded by each recipient to 3 more) — not a spanning tree or all-to-all broadcast.
- **Failure detection**: SWIM-style — direct heartbeats, falling back to proxied/indirect heartbeats through other nodes, then a suspicion window with independent re-verification before a node is declared failed.
- **Rebalancing**: data migrates automatically on node join, node removal, and replication-factor changes, all driven by ring-offset arithmetic relative to `replicationFactor`.
- **Storage**: one SQLite database per node via EF Core. User-facing deletes are tombstones; rebalancing deletes (shedding out-of-scope ranges) are hard deletes.
- **Persistence across restarts**: each node snapshots its cluster-state view so a restarted node doesn't need to be manually re-added.

For a much deeper dive into the internals — including known edge cases in the rebalancing offset math — see [CLAUDE.md](CLAUDE.md).

## Known limitations

- No automated test suite; verification is manual, via running a live cluster.
- Replication is fire-and-forget/eventually-consistent, not synchronous quorum-based — a write can return to the caller before all replicas have it.
- No authentication/authorization, TLS, or multi-tenancy — this is a from-scratch protocol implementation for learning purposes, not a hardened production system.
