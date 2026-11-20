# Kiến trúc hệ thống

## Hệ thống hiện tại

![Kiến trúc hệ thống](images/architecture-current.png)

Nguồn: [architecture-current.drawio](architecture-current.drawio) (mở bằng draw.io / app.diagrams.net hoặc
extension draw.io của VS Code; logo được nhúng sẵn trong file). Bản SVG vẽ tay tương đương:
[architecture-current.svg](architecture-current.svg).

Chỉ gồm thành phần đã triển khai và kiểm chứng (đến `v0.9-governance`); màu viền là profile Docker
Compose, ba làn batch / CDC / streaming, tên job, bảng, topic và cổng đúng như trong repo. Cập nhật
hình này khi thêm hoặc đổi một thành phần.

## Sơ đồ theo plan gốc (tham khảo)

Sơ đồ vẽ từ plan gốc ([plan](plan.md)), trước khi triển khai; hệ thống hoàn chỉnh là hình ở trên.

![System architecture](architecture.svg)

Nhãn luồng (B/S/C/Q/O/G/I) khớp bảng §4.2 trong [plan](plan.md). Port theo
`AGENTS.md` §5. Nét liền là dữ liệu; nét đứt là điều phối, metadata hoặc provisioning.
Sơ đồ mô tả kiến trúc đích; trạng thái triển khai xem
[implementation status](implementation-status.md).

Logo được nhúng trực tiếp trong SVG để GitHub render được; nhãn hiệu thuộc chủ sở hữu
tương ứng và chỉ dùng để nhận diện công nghệ:

- [Simple Icons](https://simpleicons.org) v13.21.0 (CC0): Airflow, Docker, MinIO,
  PostgreSQL, Spark, Trino, DBeaver, Kafka, Flink, Terraform, Ansible.
- Logo chính thức từ repo dự án (nhúng dạng data URI): DataHub
  (`datahub-project/datahub`), Debezium (`debezium/debezium.github.io`, cắt phần biểu tượng),
  Apache Iceberg (`apache/iceberg`, `iceberg-logo-icon.png`), Lakekeeper
  (`lakekeeper/lakekeeper`).
- Python (bản draw.io): biểu tượng `Logo_Python` của IBM Carbon Icons (Apache-2.0), lấy qua icon
  service của draw.io; bản draw.io dùng lại đúng các logo trên, nhúng dạng data URI.

## Bản rút gọn (Mermaid)

Dùng để chỉnh nhanh khi thay đổi luồng; khi đổi kiến trúc, cập nhật cả
`architecture.svg` để hai bản không lệch nhau.

```mermaid
flowchart LR
    DE([Data Engineer])

    subgraph SRC[Source generators]
        GB[batch<br/>JSONL 5 kênh]
        GL[load<br/>Walmart OLTP]
        GS[stream<br/>late · dup · burst]
    end

    MINIO[(MinIO raw<br/>JSONL + manifest)]
    PG[(PostgreSQL<br/>sources: walmart, warehouse<br/>airflow · lakekeeper · mlflow)]

    subgraph ORCH[Airflow :8081]
        DAGS[dp1_bronze · dp2_silver · dp3_* · dp4_warehouse · dp1_cdc]
    end

    subgraph SPARK[Spark :7077]
        SJ[master + 2 workers]
    end

    subgraph LH[Lakehouse · Iceberg v2]
        LK[Lakekeeper<br/>REST catalog :8181]
        BR[Bronze · dp1]
        SI[Silver · dp2]
        GO[Gold<br/>dim/fact dp2 · feat/lbl dp3]
    end

    subgraph STREAM[Streaming]
        KAFKA[[Kafka :9092]]
        FLINK[Flink :8082<br/>watermark · dedup]
        FOUT[CSV / Parquet]
    end

    DBZ[Debezium<br/>+ kafka-ui :8085]
    TRINO[Trino :8083]
    DBV[DBeaver]
    DH[DataHub :9002]
    IAC[Terraform + Ansible<br/>AWS]

    GB -- B1 --> MINIO
    MINIO -- B2 --> GL
    GL -- B3 --> PG
    MINIO -- B4 --> SJ
    PG -- B5 JDBC --> SJ
    SJ -- B6 --> BR --> SI --> GO
    SJ -. B7 commit .-> LK
    LK -. B8 catalog DB .-> PG
    GO -- B9 dp4 30 ngày --> PG

    GS -- S1 --> KAFKA -- S2 --> FLINK -- S3 --> FOUT --> BR

    PG -- C1 WAL --> DBZ -- C2 --> KAFKA
    KAFKA -- C3 dp1_cdc --> BR

    LH -- Q1 --> TRINO
    PG -- Q2 JDBC --> TRINO
    TRINO -- Q3 --> DBV --> DE

    DE -. O1 .-> ORCH
    DE -. O2 .-> SRC
    ORCH -. O3 spark-submit .-> SPARK
    ORCH -. O4 lineage .-> DH
    ORCH -. O5 metadata .-> PG
    DH -. G1 .-> DE
    DE -. I1 / I2 .-> IAC
```
