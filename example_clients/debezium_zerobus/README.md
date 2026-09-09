# Debezium Zerobus: SQL Server CDC into Delta

This example replicates a relational database's change stream directly into Unity Catalog
Delta tables using [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html)
and its native `databricks-zerobus` sink.

There is no Kafka, no Kafka Connect cluster, and no message broker of any kind. Debezium
reads the database's change log and writes each event straight to Zerobus over gRPC, which
writes it into a Delta table. For CDC into the lakehouse, that removes an entire tier of
infrastructure from the usual design.

The whole thing runs on a laptop with `docker compose up`. SQL Server is the source shown
here; the sink is source-agnostic, so the same configuration works for Postgres, MySQL and
Oracle with one connector class changed.

```
┌──────────────────────────┐
│        SQL Server        │  orders, customers
│   change data capture    │  a load generator keeps changing them
└────────────┬─────────────┘
             │  CDC change tables, read over JDBC
             v
┌──────────────────────────┐
│     Debezium Server      │  databricks-zerobus sink
└────────────┬─────────────┘
             │  gRPC, authenticated as a service principal
             v
┌──────────────────────────┐
│      Zerobus Ingest      │  serverless, no broker
└────────────┬─────────────┘
             │
             v
┌──────────────────────────┐
│  Delta in Unity Catalog  │  <catalog>.<schema>.orders and .customers
└──────────────────────────┘
```

---

## Prerequisites

- **Docker** with Compose v2. `docker compose version` should report v2 or later. Docker
  Desktop, Colima, and Docker Engine on Linux all work. Nothing else is installed on the
  host, because SQL Server runs from `mcr.microsoft.com/mssql/server:2022-latest` rather
  than being installed locally.
- **A Databricks workspace** with Unity Catalog, on AWS, Azure or GCP.
- **The Databricks CLI**, authenticated as yourself with
  `databricks auth login --host <workspace-url>`. This is used only by `setup.sh`, not by the
  running pipeline.
- **A catalog you can create a schema in.** That is the only Unity Catalog permission
  needed, and it has to be backed by your own cloud storage rather than Databricks default
  storage, which Zerobus cannot write to.
- **A SQL warehouse** in the workspace. `setup.sh` finds one and starts it if needed.
- **About 4 GB of RAM free** for the containers, most of it SQL Server's.

`setup.sh` does the rest of the Databricks side: the schema, the target tables, the grants,
the service principal, and the Zerobus endpoint.

### A note on architecture

Microsoft publishes SQL Server container images for x86-64 only, so `docker-compose.yml`
pins `platform: linux/amd64` on that one service. On Linux x86-64, on Windows with WSL2 and
on Intel Macs it runs natively and the pin changes nothing. On Apple Silicon it runs
emulated, which needs Rosetta, so Docker Desktop users should turn on the setting called
*Use Rosetta for x86_64/amd64 emulation*. It will also run on arm64 Linux under QEMU, slowly
enough that it is not worth doing.

Debezium Server's image is multi-arch and always runs natively, including the Zerobus SDK's
native library.

On Apple Silicon with Colima rather than Docker Desktop, this is the sequence that works.
Homebrew installs `docker-compose` as a standalone binary, so `docker compose` does not find
it until you link it in as a CLI plugin:

```bash
brew install colima docker docker-compose
mkdir -p ~/.docker/cli-plugins
ln -sfn "$(brew --prefix)/opt/docker-compose/bin/docker-compose" ~/.docker/cli-plugins/docker-compose
colima start --vm-type=vz --vz-rosetta --cpu 4 --memory 8
```

---

## Quick start

### 1. Configure

```bash
cd example_clients/debezium_zerobus
cp .env.example .env
```

Set two values in `.env`. `DATABRICKS_WORKSPACE_URL` is your workspace hostname without
`https://`, and `DATABRICKS_CATALOG` is a catalog you can already create a schema in.
Everything else has a working default.

```bash
databricks catalogs list --profile <your-profile>
```

There is nothing else to look up. Your workspace ID and region are not settings here:
`setup.sh` reads the ID from the workspace itself and the region from the metastore, and
builds the Zerobus endpoint from them.

Two different credentials are involved, which is worth being clear about. The CLI profile you
pass to `setup.sh` is yours, and it is used only to create the schema, tables and grants. The
service principal is what Debezium authenticates to Zerobus with while the pipeline runs. If
you leave `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` blank, `setup.sh` creates that
service principal, mints a secret and writes both back into `.env`, which needs workspace
admin. Fill them in with an existing service principal if you do not have that.

### 2. Prepare the Databricks side

```bash
./setup.sh <your-cli-profile>
```

This creates the schema and target tables in the catalog you named, grants the service
principal what it needs, works out the Zerobus endpoint for your workspace, and renders
`conf/application.properties` from the template. It is idempotent, so re-run it whenever you
change `.env`.

### 3. Start the pipeline

```bash
docker compose up -d
docker compose logs -f debezium
```

Four containers come up: SQL Server, a one-shot initialiser that creates the tables and
turns on CDC, Debezium Server, and a load generator that keeps changing rows so there is
always something to watch.

Look for these lines, in this order. Both target tables get their own stream, because
Zerobus opens one per target table:

```
INFO [com.databricks.zerobus.NativeLoader] Loaded native library from classpath:
     /native/linux-aarch64/libzerobus_jni.so
INFO [io.debezium.server.databricks.zerobus.ZerobusChangeConsumer] Zerobus gRPC sink
     connected: endpoint=<workspace-id>.zerobus.<region>.cloud.databricks.com
INFO [io.debezium.pipeline.ChangeEventSourceCoordinator] Snapshot ended with
     SnapshotResult [status=COMPLETED
INFO [io.debezium.server.databricks.zerobus.ZerobusChangeConsumer] Opening Zerobus JSON
     stream for table '<catalog>.<schema>.orders'
INFO [io.debezium.server.databricks.zerobus.ZerobusChangeConsumer] Opening Zerobus JSON
     stream for table '<catalog>.<schema>.customers'
```

### 4. Watch the data arrive

```sql
SELECT * FROM <catalog>.<schema>.orders ORDER BY order_id DESC LIMIT 20;

-- Deletes arrive as a row with __deleted = 'true'
SELECT __deleted, count(*) FROM <catalog>.<schema>.orders GROUP BY __deleted;

-- End-to-end lag, from the source commit to now
SELECT round((unix_millis(current_timestamp()) - max(unix_millis(updated_at))) / 1000.0, 1)
       AS lag_seconds
FROM <catalog>.<schema>.orders;
```

The load generator inserts and updates on every round and deletes every fifth, so all of
insert, update and delete show up within roughly fifteen seconds of starting.

The first query returns something like this. Notice `order_id` 278 twice, once as `PAID` and
again as `SHIPPED` a tenth of a second later, which is the insert and then the update to the
same source row:

```
 order_id | order_status | amount   | order_date | updated_at               | __deleted
----------+--------------+----------+------------+--------------------------+-----------
 278      | SHIPPED      | 18764.38 | 2026-09-03 | 2026-09-03T23:15:40.578Z | false
 278      | PAID         | 18764.38 | 2026-09-03 | 2026-09-03T23:15:40.480Z | false
 277      | SHIPPED      | 13642.41 | 2026-09-03 | 2026-09-03T23:15:37.257Z | false
 276      | CANCELLED    | 24413.59 | 2026-09-03 | 2026-09-03T23:15:33.800Z | false
```

Re-running a `SELECT` is the simplest way to see progress. For a view that updates on its own,
import [`notebooks/watch_stream.py`](notebooks/watch_stream.py) into the workspace and run it.
It does `spark.readStream.table()` against both target tables and displays them live, which
works because Zerobus only ever appends, updates and deletes included.

### 5. Stop

```bash
docker compose down -v      # -v also discards the database and Debezium's offsets
```

---

## Configuration reference

Only the first two need your input. Everything else either ships with a working value or is
worked out for you.

| Variable | Set it yourself | Description |
|---|---|---|
| `DATABRICKS_WORKSPACE_URL` | Yes | Workspace hostname, no `https://` |
| `DATABRICKS_CATALOG` | Yes | An existing catalog you can create a schema in |
| `DATABRICKS_SCHEMA` | No | Schema to create in it, default `zerobus_cdc` |
| `DATABRICKS_CLIENT_ID` | No | Service principal application ID, created if blank |
| `DATABRICKS_CLIENT_SECRET` | No | Its OAuth secret, minted if blank |
| `DATABRICKS_SP_NAME` | No | Name for that service principal if one is created |
| `DATABRICKS_INGEST_ENDPOINT` | No | Built as `<workspace-id>.zerobus.<region>.<suffix>`, where the workspace ID comes from the workspace and needs no looking up |
| `DATABRICKS_REGION` | No | Read from the metastore. Override only if that is wrong. |
| `DATABRICKS_CATALOG_STORAGE_ROOT` | No | Only to create the catalog itself, see Troubleshooting |
| `MSSQL_SA_PASSWORD` | No | Throwaway password for the demo container, already filled in |
| `MSSQL_DATABASE` | No | Source database, default `inventory` |
| `DEBEZIUM_SQL_USER` | No | The SQL Server login Debezium connects with |
| `DEBEZIUM_SQL_PASSWORD` | No | Its password, already filled in |
| `LOADGEN_INTERVAL_SECONDS` | No | Seconds between rounds of changes, default 3 |

---

## What the target tables look like

Each source column becomes a Delta column, so the two target tables mirror
`dbo.orders` and `dbo.customers` in the source database. The full DDL, including the grants
the service principal needs, is in [`create_table.sql`](create_table.sql).

Deletes are the one thing that does not map one to one. Rather than removing the Delta row,
Debezium's `ExtractNewRecordState` transform sets `__deleted` to the string `"true"` on it,
so the history of the row survives. That column has to be declared `STRING` and not
`BOOLEAN`.

Delivery is at-least-once, so deduplicate downstream if you need exactly-once.

## Troubleshooting

**Nothing arrives, and the logs show no errors at all.**
Almost always the router. Debezium names each destination after the source object, and
because this configuration uses the multi-database `database.names` form, that name carries
the database too: `cdc.inventory.dbo.orders`, four parts rather than three. The
`RegexRouter` in `conf/application.*.properties.tmpl` rewrites it into a three-part Unity
Catalog name. If the pattern does not match, nothing is routed, nothing is ingested, and
Debezium reports no error. A healthy-looking log with an empty table is this.

**`Unsupported table kind. Tables created in default storage are not supported. Error Code: 4024`**
Zerobus cannot write to a table in Databricks default storage, so the catalog has to be
backed by your own cloud storage. Point `DATABRICKS_CATALOG` at one that is. `setup.sh`
checks this before Debezium ever starts, so you should see its message rather than this one.

If no such catalog exists yet, set `DATABRICKS_CATALOG_STORAGE_ROOT` to an external location
and `setup.sh` will create the catalog on it, which needs `CREATE CATALOG` on the metastore:

```bash
databricks external-locations list --profile <profile>
```

**`Failed to create stream` with a permission error.**
The service principal needs `SELECT` and `MODIFY` granted **on the table itself**. Inheriting
from the schema is not enough, and `GRANT ALL PRIVILEGES` does not substitute. `setup.sh`
issues the right grants; if you supplied your own service principal, check them.

**`ClassNotFoundException: io.debezium.transforms.ExtractNewRecordState`**
`ENABLE_DEBEZIUM_SCRIPTING=true` is missing from the debezium service's environment.

**CDC is enabled but no changes are ever captured.**
SQL Server Agent is not running. Capture and cleanup are Agent jobs, so without it the
tables report as CDC-enabled and nothing happens. The compose file sets
`MSSQL_AGENT_ENABLED=true` and the healthcheck waits for Agent specifically rather than for
the engine, which is why `db-init` does not start too early.

**SQL Server will not start, or the healthcheck never passes.**
Check that the container has enough memory, since it needs about 2 GB. On Apple Silicon,
confirm Rosetta emulation is enabled, otherwise the amd64 image cannot run at all.

**Deletes never appear.**
In typed mode they arrive as an existing row with `__deleted = 'true'`. If that column is
declared `BOOLEAN` instead of `STRING`, every delete is rejected.

---

## Extending this

**A different database.** Change `debezium.source.connector.class` and the connection
properties. Postgres and MySQL are simpler than SQL Server here, needing neither capture
instances nor SQL Server Agent. The sink configuration does not change at all.

**More source tables.** Add them to `debezium.source.table.include.list` and give each one a
Delta table with matching columns. The existing `RegexRouter` pattern already covers every
table in the `dbo` schema, so it does not need changing.

**Production.** Give the Debezium container a durable volume for its offsets and schema
history, as this compose file does, or it re-snapshots on every restart. Narrow the
`db_owner` grant that `init.sql` takes for convenience. One Debezium Server handles one
database server, since the connector takes a single hostname, so scale out per source
instance; several of them can write the same Delta table concurrently, each with its own
service principal secret.

---

## File reference

| File | Description |
|---|---|
| `docker-compose.yml` | The four services: SQL Server, initialiser, Debezium, load generator |
| `setup.sh` | Databricks-side setup and config rendering |
| `.env.example` | Every setting, with defaults |
| `create_table.sql` | Target Delta tables, plus grants |
| `conf/application.properties.tmpl` | Debezium Server config, rendered by `setup.sh` |
| `sqlserver/init.sql` | Source tables, CDC enablement, Debezium login |
| `sqlserver/loadgen.sh` | Continuous inserts, updates and deletes |
| `notebooks/watch_stream.py` | Streaming read of both target tables, for a live view |

## Reference

- [Zerobus Ingest overview](https://docs.databricks.com/ingestion/zerobus-overview)
- [Zerobus Ingest limits](https://docs.databricks.com/ingestion/zerobus-limits)
- [Debezium Server, Databricks Zerobus sink](https://debezium.io/documentation/reference/operations/debezium-server.html#_databricks_zerobus_ingest)
- [Debezium SQL Server connector](https://debezium.io/documentation/reference/connectors/sqlserver.html)
- [SQL Server change data capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this example is provided
subject to the [Databricks License](https://databricks.com/db-license-source).
