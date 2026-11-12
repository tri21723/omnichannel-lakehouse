# ADR-0005: Deterministic datasets and source ownership

- Status: Accepted for batch/load; streaming stays in Phase 7.
- Use Python 3.11, Faker, Pydantic, Typer, YAML, boto3 and Psycopg. Direct dependency
  pins live in generators/requirements.txt. Faker locale and seeds are explicit.
- Dataset identity is a hash of the full normalized config plus contract version.
  Same identity means the same bytes; bump contract version when changing generation.
  generated_at is logical start-of-data time, not wall-clock execution time.
- Each day/channel has independent seeded RNG. dup_rate means extra copies per unique
  order (0.04 => 1,040 physical rows for 1,000 unique orders). Counts are rounded per day.
  Skew and return rates apply to unique orders, not duplicates. Drift day is zero-based.
- Config channel order is normalized. Different configs produce separate run prefixes
  and source IDs. They are separate datasets, not incremental updates to existing orders.
- Paths: <channel>/dt=<date>/run_id=<id>/part-*.jsonl. Consumers use the selected
  manifest, never union all runs blindly. Files are chunked with rows_per_file.
- Orders embed customer, item/product and return payloads. Monetary values are decimal
  strings. Shopee timestamps include +0700; Lazada uses synthetic VND prices and omits
  optional brand. No real exchange-rate conversion is claimed.
- Amazon renames buyer_id to customer_id and adds fulfillment_type at drift day.
- JSONL intentionally contains duplicates. Walmart load uses canonical unique records,
  nested entities and transactional upserts. JSON exports and JDBC are the same orders;
  downstream Silver must choose JDBC for Walmart (ADR-0003), not double-count both.
- Publish S3 manifest last. It is the completion marker; incomplete files without a
  manifest are not ingestible. Existing dataset artifacts must reconcile before reuse.
- Local generation writes into staging then renames to the immutable run directory.
  Streaming late/burst fields are reserved; batch does not implement those behaviors.

Implementation references:
- [Psycopg COPY and transaction behavior](https://www.psycopg.org/psycopg3/docs/basic/copy.html)
- [S3 put_object conditional writes](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html)
- [Pydantic model configuration](https://docs.pydantic.dev/latest/concepts/models/)
