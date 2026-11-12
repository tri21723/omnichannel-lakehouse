# ADR-0002: Local-first monorepo and lakehouse boundaries

- Status: Accepted for initial implementation.
- Context: A personal project needs a reproducible batch demo before other flows.
- Decision: Adopt the top-level directories defined in the plan, including
  `.github` for CI. Use Docker Compose profiles, Iceberg with Lakekeeper REST
  catalog, Spark batch transformations and Trino query federation.
- Alternatives: Hive Metastore adds a different catalog dependency; a warehouse-only
  design does not demonstrate the planned Iceberg snapshots and multi-engine access.
- Trade-off: Many services increase resource and compatibility work. Implement the
  Walmart vertical slice first; streaming, CDC and governance stay separate phases.
- Flink initially writes files to match the architecture; direct Iceberg sink is backlog.
- No infrastructure is provisioned by scaffold setup or CI.
