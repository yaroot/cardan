# Cardan

Postgres to kafka capture


# Dependencies

- postgres wal logical
- wal2json

# Usage

- install wal2json
- set wal_level to logical
- create the replication role `create role <name> with login replication password '<secret>'`
- allow replication user in pg_hba.conf
- create the slot `select pg_create_logical_replication_slot('<slot_name>', 'wal2json')`

# Dev

Written in Scala, build with mill.

# The details


