# Phase 10 handoff: AWS with Terraform and Ansible

Phase 9 is complete locally (`v0.9-governance`). This page lists what Phase 10 can rely on and the
constraints it inherits. It does not implement Phase 10 ([plan §Phase 10](plan.md)); no cloud resource
was created and none may be without the owner's explicit request (AGENTS.md §9).

## 1. What runs locally today

| Profile | Services (pinned) | Measured memory | Make entry |
|---|---|---|---|
| `core` | PostgreSQL 16, MinIO | 0.2–0.7 GiB | `up-core` |
| `batch` | Lakekeeper, Spark 3.5.5 master + 2 workers (Iceberg 1.9.2) | ~2.5 GiB under load | `up-batch` |
| `orch` | Airflow 3.3.2 (also the Spark driver; DataHub CLI venv) | 1.2–2.1 GiB | `up-orch` |
| `query` | Trino 484 read-only | ~1.75 GiB | `up-query` |
| `stream` | Kafka 4.3.2 KRaft, kafka-ui, Flink 2.2.1 | Kafka 0.5, Flink 1.4 GiB | `up-stream` |
| `cdc` | Kafka Connect + Debezium 3.6.3 | ~0.5 GiB | `up-cdc` |
| `governance` | DataHub v1.7.0.1 (OpenSearch, GMS, frontend), `datahub-ingest` | ~3.5 GiB | `up-gov` |

Images built from the repo: `omnichannel/spark`, `omnichannel/airflow`, `omnichannel/flink`,
`omnichannel/generator`, `omnichannel/minio` (init), `omnichannel/datahub-ingest`. Every manual
workflow (`.github/workflows/*.yml`) builds them on a clean runner.

## 2. Suggested mapping to AWS (plan §3.3, one EC2, no NAT Gateway)

| Local | AWS (plan) | Notes |
|---|---|---|
| MinIO buckets `raw`, `lakehouse`, `checkpoints` | S3 buckets | path-style + endpoint settings become the AWS defaults; Iceberg via Lakekeeper REST stays |
| PostgreSQL 16 (sources, warehouse, airflow, lakekeeper, datahub) | PostgreSQL in Docker on the EC2 data disk (plan) | RDS would add cost; decide in an ADR |
| Docker Compose profiles | the same compose files on one EC2 (`/mnt/platform-data` EBS) | Ansible role `app` runs `make up-*` per profile |
| Locally built images | ECR repositories | Ansible pulls; tags pinned as in ADR-0001 |
| `.env` generated secrets | SSM Parameter Store / Secrets Manager → `.env` on the host | never in Terraform state or git |
| Loopback-only ports (8081, 8083, 9002, …) | SSH tunnel / SSM port forwarding | do not open UI ports to the internet |

DoD of the plan: `make aws-up` builds the minimal stack (batch + Trino), `make aws-down` removes it.
DataHub has its own Ansible role because of its size; it does not fit next to batch on a small instance.

## 3. Contracts Phase 10 must keep

- **No `terraform apply`/`destroy` and no cloud resource without an explicit request**; only `fmt`,
  `validate`, `plan` (AGENTS.md §7, §9). A budget alert (`aws_budgets_budget`, 5–10 USD) comes first.
- Tags `project`, `owner`, `ttl` on every resource; **no NAT Gateway**; check EBS/EIP/snapshots after
  destroy.
- Pinned versions and ports (AGENTS.md §5, ADR-0001) do not change; the three batch datasets, CDC
  scenarios and ledgers are local evidence and are not migrated as part of IaC.
- PII stays restricted: `raw` and Bronze hold synthetic names/e-mails (20 objects tagged `PII` in
  DataHub); S3 buckets need block-public-access and encryption; Trino keeps `iceberg.security=read_only`.
- Read-only roles (`trino_ro`, `datahub_ro`, MinIO `trino_local`) map to least-privilege IAM.

## 4. Budget notes

The plan targets one EC2 for a short demo then `terraform destroy`. Measured local memory: batch +
Trino fits in ~8 GiB; batch + Airflow + Trino ~10–12 GiB; DataHub alone ~3.5 GiB plus core/Kafka.
Prices change by region and date: use the AWS Pricing Calculator at the time of Phase 10 and record
the chosen instance type and hourly cost in its ADR.

## 5. Existing skeleton

`infra/terraform/{modules,envs/dev}` and `infra/ansible/{inventories,playbooks,roles}` contain only
`.gitkeep`; no Make target `aws-*` exists yet.
