# ADR-0003: Source ownership, keys and Gold responsibilities

- Status: Proposed defaults for implementation; no existing schemas are migrated.
- Walmart JDBC is the authoritative batch source. Walmart JSON exports support
  reconciliation and replay; do not union both into the same Silver state.
- CDC adoption requires a snapshot-to-log handoff design before enabling concurrent
  writers to Walmart Silver. LSN ordering and soft deletes are the intended policy.
- Source IDs are opaque strings scoped by channel. Composite keys remain canonical;
  generators.common.identity.entity_key provides an optional reversible encoding.
- dp2 owns Silver and Gold dimensions/facts. dp3 owns features/labels. dp4 publishes
  existing Gold to PostgreSQL warehouse. This follows the diagram and resolves the
  conflicting dp4 description in the original plan.
- Feature cutoff uses event time strictly before as_of_ts. Return labels have a
  configurable observation horizon; immature labels are NULL, not negative.
- Monetary values retain original currency; cross-currency totals require an explicit
  FX dataset and rate date. Until then aggregate within currency only.
