"""Laboratório Aurora: dados próprios, Spark/Delta e Python nativos.

Execuções devem ser serializadas por dataset. Nenhum pip/download é usado.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

BASE = "/Workspace/Users/<SEU_USUARIO>/portfolio-estudos"
ROOT = "workspace"
VOLUME = "/Volumes/workspace/estudos_bronze/fontes/aurora"
SCENARIOS = {"base", "base_small", "duplicates", "late", "late_small", "invalid", "conflict", "late_outside", "late_outside_small"}
EVENT_FIELDS = ["event_id", "event_type", "event_time", "library_id", "book_id", "loan_id", "schema_version"]
TABLES = {
    "bronze": "workspace.estudos_bronze.aurora_eventos",
    "events": "workspace.estudos_silver.aurora_eventos_validos",
    "quarantine": "workspace.estudos_silver.aurora_eventos_quarentena",
    "loans": "workspace.estudos_silver.aurora_emprestimos",
    "gold": "workspace.estudos_gold.aurora_circulacao_diaria",
    "stats": "workspace.estudos_ops.aurora_cargas",
    "checks": "workspace.estudos_ops.aurora_checks",
    "releases": "workspace.estudos_ops.aurora_releases",
    "active": "workspace.estudos_ops.aurora_publicacao",
    "tests": "workspace.estudos_ops.aurora_testes_integracao",
    "reports": "workspace.estudos_ai.aurora_boletins",
}


def digest(value):
    data = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(data).hexdigest()


def iso(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Config:
    dataset: str = "aurora_v1_202609"
    batch: str = "base_v1"
    scenario: str = "base"
    release: str = "manual_v1"
    data_reference: str = "2026-09-30T23:59:59Z"
    allow_backfill: bool = False

    def __post_init__(self):
        for value in (self.dataset, self.batch, self.release):
            if not re.fullmatch(r"[a-zA-Z0-9_]{1,100}", value):
                raise ValueError("Dataset, batch e release aceitam somente letras, números e underscore.")
        if self.scenario not in SCENARIOS:
            raise ValueError("Cenário não permitido.")
        parsed = datetime.fromisoformat(self.data_reference.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("A data de referência precisa de timezone explícita.")


def from_widgets(dbutils):
    defaults = {"dataset_version": "aurora_v1_202609", "batch_id": "base_v1", "scenario": "base",
                "release_id": "manual_v1", "data_reference": "2026-09-30T23:59:59Z", "allow_backfill": "false"}
    for name, value in defaults.items():
        dbutils.widgets.text(name, value)
    args = {name: dbutils.widgets.get(name) for name in defaults}
    if args["allow_backfill"].lower() not in {"true", "false"}:
        raise ValueError("allow_backfill deve ser true ou false.")
    return Config(dataset=args["dataset_version"], batch=args["batch_id"], scenario=args["scenario"],
                  release=args["release_id"], data_reference=args["data_reference"],
                  allow_backfill=args["allow_backfill"].lower() == "true")


def synthetic_events(scenario="base"):
    if scenario not in SCENARIOS:
        raise ValueError("Cenário não permitido")
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    events = []
    small = scenario == "base_small"
    for index in range(10 if small else 2500):
        loan = f"L{index:06d}"
        event_time = start + timedelta(days=index % 23, hours=(index // 23) % 24)
        common = {"library_id": f"LIB{index % 3 + 1:02d}", "book_id": f"B{index % 200:03d}",
                  "loan_id": loan, "schema_version": 1}
        def add(kind, time):
            events.append(dict(common, event_id=f"E_{loan}_{kind}", event_type=kind,
                               event_time=iso(time), source_arrived_at=iso(time + timedelta(hours=1))))
        add("loan", event_time)
        if index < (7 if small else 2000):
            add("return", event_time + timedelta(days=1 + index % 7))
        if index < (3 if small else 500):
            add("renewal", event_time + timedelta(hours=12))
    if scenario in {"base", "base_small"}:
        return events
    if scenario == "duplicates":
        return [dict(item) for item in events[:5]]
    if scenario == "conflict":
        return [dict(events[0], book_id="B199")]
    if scenario == "invalid":
        return [dict(events[0], event_id=None),
                dict(events[0], event_id="E_BAD_TIME", event_time="nao-e-data"),
                dict(events[0], event_id="E_BAD_LIBRARY", library_id="LIB99"),
                dict(events[0], event_id="E_BAD_RETURN", event_type="return", event_time="2026-08-31T00:00:00Z")]
    index = {"late": 2200, "late_small": 8, "late_outside": 2201, "late_outside_small": 9}[scenario]
    original = next(item for item in events if item["loan_id"] == f"L{index:06d}" and item["event_type"] == "loan")
    event_time = datetime.fromisoformat(original["event_time"].replace("Z", "+00:00")) + timedelta(days=2)
    delay = 3 if scenario in {"late", "late_small"} else 8
    return [dict(original, event_id=f"E_{original['loan_id']}_return", event_type="return", event_time=iso(event_time),
                 source_arrived_at=iso(event_time + timedelta(days=delay)))]


def write_immutable(path, data):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() != data:
            raise ValueError(f"Arquivo imutável já existe com outro conteúdo: {path}")
        return
    with target.open("xb") as stream:
        stream.write(data)


def ensure_table(spark, table, schema):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING DELTA")


def bootstrap(spark):
    for schema in ["estudos_bronze", "estudos_silver", "estudos_gold", "estudos_ops", "estudos_ai"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema} COMMENT 'Laboratório pessoal Aurora; dados sintéticos'")
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.estudos_bronze.fontes COMMENT 'Fontes sintéticas do portfólio Aurora'")
    definitions = {
        "bronze": "dataset_version STRING, batch_id STRING, ingest_key STRING, source_row BIGINT, raw_json STRING, source_file STRING, source_hash STRING, ingested_at TIMESTAMP",
        "stats": "dataset_version STRING, release_id STRING, batch_id STRING, scenario STRING, bronze_rows BIGINT, accepted_rows BIGINT, duplicate_rows BIGINT, quarantine_rows BIGINT, conflict_rows BIGINT, loan_rows BIGINT, gold_rows BIGINT, data_reference STRING, config_hash STRING, created_at TIMESTAMP",
        "checks": "dataset_version STRING, release_id STRING, rule_id STRING, severity STRING, observed DOUBLE, threshold DOUBLE, status STRING, detail STRING, checked_at TIMESTAMP",
        "active": "dataset_version STRING, release_id STRING, data_reference STRING, published_at TIMESTAMP",
        "releases": "dataset_version STRING, release_id STRING, data_reference STRING, status STRING, gold_hash STRING, created_at TIMESTAMP",
        "tests": "suite_id STRING, test_id STRING, status STRING, expected STRING, observed STRING, created_at TIMESTAMP",
        "reports": "report_key STRING, dataset_version STRING, release_id STRING, library_id STRING, data_reference STRING, revision_hash STRING, report_json STRING, text STRING, created_at TIMESTAMP",
    }
    for key, schema in definitions.items():
        ensure_table(spark, TABLES[key], schema)
    return {"schemas": 5, "volume": "workspace.estudos_bronze.fontes", "downloads": 0}


def generate_landing(config):
    events = synthetic_events(config.scenario)
    data = "".join(json.dumps({"source_row": i, "payload": json.dumps(event, sort_keys=True)}, sort_keys=True) + "\n"
                   for i, event in enumerate(events)).encode()
    folder = f"{VOLUME}/{config.dataset}/{config.batch}"
    path = f"{folder}/eventos.ndjson"
    manifest = {"dataset_version": config.dataset, "batch_id": config.batch, "scenario": config.scenario,
                "file": path, "sha256": digest(data), "rows": len(events), "generator_version": "1.0.0",
                "seed": "deterministic_v1", "source": "synthetic_original", "mode": "historical_replay",
                "arrival_semantics": "source_arrived_at é chegada simulada; ingested_at é a ingestão técnica real"}
    write_immutable(path, data)
    write_immutable(f"{folder}/manifesto.json", json.dumps(manifest, sort_keys=True, indent=2).encode())
    return manifest


def merge_frame(spark, frame, table, keys):
    if not spark.catalog.tableExists(table):
        frame.limit(0).write.format("delta").saveAsTable(table)
    frame.createOrReplaceTempView("aurora_upsert_source")
    condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
    spark.sql(f"MERGE INTO {table} t USING aurora_upsert_source s ON {condition} WHEN NOT MATCHED THEN INSERT *")


def ingest_bronze(spark, config):
    from pyspark.sql import functions as F
    manifest = json.loads(Path(f"{VOLUME}/{config.dataset}/{config.batch}/manifesto.json").read_text())
    assert manifest["dataset_version"] == config.dataset and manifest["batch_id"] == config.batch
    assert digest(Path(manifest["file"]).read_bytes()) == manifest["sha256"], "Hash de origem divergente"
    data = spark.read.schema("source_row LONG, payload STRING").json(manifest["file"])
    assert data.count() == manifest["rows"]
    frame = data.select(F.lit(config.dataset).alias("dataset_version"), F.lit(config.batch).alias("batch_id"),
        F.sha2(F.concat_ws("|", F.lit(config.dataset), F.lit(config.batch), F.col("source_row")), 256).alias("ingest_key"),
        "source_row", F.col("payload").alias("raw_json"), F.lit(manifest["file"]).alias("source_file"),
        F.lit(manifest["sha256"]).alias("source_hash"), F.current_timestamp().alias("ingested_at"))
    merge_frame(spark, frame, TABLES["bronze"], ["ingest_key"])
    return {"batch_rows": manifest["rows"], "bronze_rows": spark.table(TABLES["bronze"]).where(F.col("dataset_version") == config.dataset).count()}


def snapshot(spark, key, config):
    from pyspark.sql import functions as F
    return spark.table(TABLES[key]).where((F.col("dataset_version") == config.dataset) & (F.col("release_id") == config.release))


def transform(spark, config):
    from pyspark.sql import functions as F, Window
    bronze = spark.table(TABLES["bronze"]).where(F.col("dataset_version") == config.dataset)
    total = bronze.count()
    if total > 20000:
        raise ValueError("Esta versão didática está limitada a 20.000 eventos por dataset.")
    previous = snapshot(spark, "stats", config).collect()
    config_hash = digest({"data_reference": config.data_reference, "allow_backfill": config.allow_backfill, "contract": "1.0.0"})
    if previous and (previous[0].bronze_rows != total or previous[0].config_hash != config_hash):
        raise ValueError("Release já existe com outra entrada/referência. Crie uma nova execução/release.")
    schema = "event_id STRING,event_type STRING,event_time STRING,library_id STRING,book_id STRING,loan_id STRING,schema_version STRING,source_arrived_at STRING"
    parsed = bronze.withColumn("parsed", F.from_json("raw_json", schema)).select("*", "parsed.*").drop("parsed")
    parsed = parsed.withColumn("_event_ts", F.expr("try_to_timestamp(event_time)"))
    parsed = parsed.withColumn("_arrival_ts", F.expr("try_to_timestamp(source_arrived_at)"))
    missing = F.lit(False)
    for field in EVENT_FIELDS:
        missing = missing | F.col(field).isNull() | (F.trim(F.col(field)) == "")
    reason = (F.when(missing, F.lit("required_field_missing"))
        .when(~F.col("event_type").isin("loan", "return", "renewal"), "event_type_invalid")
        .when(F.col("schema_version") != "1", "schema_version_invalid")
        .when(F.col("_event_ts").isNull() | F.col("_arrival_ts").isNull(), "timestamp_invalid")
        .when(~F.col("event_time").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"), "timestamp_format_invalid")
        .when(~F.col("library_id").isin("LIB01", "LIB02", "LIB03"), "library_unknown")
        .when(~F.col("book_id").isin(*[f"B{x:03d}" for x in range(200)]), "book_unknown")
        .when(F.col("_arrival_ts") < F.col("_event_ts"), "arrival_before_event"))
    if not config.allow_backfill:
        reason = reason.when(F.col("_arrival_ts") > F.col("_event_ts") + F.expr("INTERVAL 7 DAYS"), "late_outside_window")
    parsed = parsed.withColumn("_basic_reason", reason.otherwise(F.lit(None).cast("string")))
    parsed = parsed.withColumn("semantic_hash", F.sha2(F.to_json(F.struct(*EVENT_FIELDS)), 256))
    conflicts = parsed.groupBy("event_id").agg(F.countDistinct("semantic_hash").alias("_variants"))
    parsed = parsed.join(conflicts, "event_id", "left").withColumn("_conflict", (F.col("_variants") > 1) & F.col("event_id").isNotNull())
    rank = Window.partitionBy("event_id").orderBy("source_hash", "batch_id", "source_row", "ingest_key")
    parsed = parsed.withColumn("_rank", F.row_number().over(rank))
    eligible = parsed.where(F.col("_basic_reason").isNull() & ~F.col("_conflict") & (F.col("_rank") == 1))
    facts = eligible.groupBy("loan_id").agg(
        F.sum(F.when(F.col("event_type") == "loan", 1).otherwise(0)).alias("_starts"),
        F.min(F.when(F.col("event_type") == "loan", F.col("_event_ts"))).alias("_start"),
        F.first(F.when(F.col("event_type") == "loan", F.col("library_id")), ignorenulls=True).alias("_start_library"),
        F.first(F.when(F.col("event_type") == "loan", F.col("book_id")), ignorenulls=True).alias("_start_book"))
    links = eligible.join(facts, "loan_id", "left").select("event_id",
        F.when(F.col("_starts") != 1, "loan_start_missing_or_multiple")
        .when(F.col("_event_ts") < F.col("_start"), "event_before_loan")
        .when((F.col("library_id") != F.col("_start_library")) | (F.col("book_id") != F.col("_start_book")), "loan_entity_mismatch")
        .otherwise(F.lit(None).cast("string")).alias("_relation_reason"))
    classified = parsed.join(links, "event_id", "left").withColumn("reason",
        F.coalesce("_basic_reason", F.when(F.col("_conflict"), F.lit("event_key_conflict")), "_relation_reason"))
    accepted = classified.where(F.col("reason").isNull() & (F.col("_rank") == 1)).select(
        "dataset_version", F.lit(config.release).alias("release_id"), "batch_id", "ingest_key", "event_id", "event_type",
        F.col("_event_ts").alias("event_time"), "library_id", "book_id", "loan_id",
        F.col("schema_version").cast("int").alias("schema_version"), F.col("_arrival_ts").alias("source_arrived_at"),
        "semantic_hash", "source_file", "source_hash", "ingested_at")
    rejected = classified.where(F.col("reason").isNotNull()).select(
        "dataset_version", F.lit(config.release).alias("release_id"), "batch_id", "ingest_key", "event_id", "loan_id",
        "reason", "raw_json", "source_file", "source_hash", "source_row")
    merge_frame(spark, accepted, TABLES["events"], ["dataset_version", "release_id", "ingest_key"])
    merge_frame(spark, rejected, TABLES["quarantine"], ["dataset_version", "release_id", "ingest_key"])
    valid = snapshot(spark, "events", config).where(F.col("event_time") <= F.to_timestamp(F.lit(config.data_reference)))
    loans = valid.groupBy("dataset_version", "release_id", "loan_id").agg(
        F.first("library_id").alias("library_id"), F.first("book_id").alias("book_id"),
        F.min(F.when(F.col("event_type") == "loan", F.col("event_time"))).alias("loan_time"),
        F.min(F.when(F.col("event_type") == "return", F.col("event_time"))).alias("return_time"),
        F.sum(F.when(F.col("event_type") == "renewal", 1).otherwise(0)).cast("long").alias("renewals"))
    loans = loans.where(F.col("loan_time").isNotNull()).withColumn("duration_hours",
        F.when(F.col("return_time").isNotNull(), (F.unix_timestamp("return_time") - F.unix_timestamp("loan_time")) / 3600.0))
    loans = loans.withColumn("is_open", F.col("return_time").isNull()).withColumn("data_reference", F.lit(config.data_reference))
    merge_frame(spark, loans, TABLES["loans"], ["dataset_version", "release_id", "loan_id"])
    loans = snapshot(spark, "loans", config)
    gold = loans.withColumn("loan_date", F.to_date("loan_time")).groupBy("dataset_version", "release_id", "library_id", "loan_date").agg(
        F.count("loan_id").alias("loans"),
        F.sum(F.when(F.col("is_open"), 1).otherwise(0)).cast("long").alias("open_loans"),
        F.sum(F.when(~F.col("is_open"), 1).otherwise(0)).cast("long").alias("closed_loans"),
        F.sum("duration_hours").alias("total_duration_hours"), F.avg("duration_hours").alias("average_duration_hours"))
    gold = gold.withColumn("data_reference", F.lit(config.data_reference))
    merge_frame(spark, gold, TABLES["gold"], ["dataset_version", "release_id", "library_id", "loan_date"])
    counts = classified.agg(
        F.sum(F.when(F.col("reason").isNull() & (F.col("_rank") == 1), 1).otherwise(0)).alias("accepted"),
        F.sum(F.when(F.col("reason").isNull() & (F.col("_rank") > 1), 1).otherwise(0)).alias("duplicates"),
        F.sum(F.when(F.col("reason").isNotNull(), 1).otherwise(0)).alias("rejected"),
        F.sum(F.when(F.col("reason") == "event_key_conflict", 1).otherwise(0)).alias("conflicts")).first()
    stats = {"dataset_version": config.dataset, "release_id": config.release, "batch_id": config.batch, "scenario": config.scenario,
             "bronze_rows": total, "accepted_rows": int(counts.accepted or 0), "duplicate_rows": int(counts.duplicates or 0),
             "quarantine_rows": int(counts.rejected or 0), "conflict_rows": int(counts.conflicts or 0),
             "loan_rows": loans.count(), "gold_rows": snapshot(spark, "gold", config).count(), "data_reference": config.data_reference,
             "config_hash": config_hash, "created_at": datetime.now(timezone.utc)}
    merge_frame(spark, spark.createDataFrame([stats], schema=spark.table(TABLES["stats"]).schema), TABLES["stats"], ["dataset_version", "release_id"])
    return {key: value for key, value in stats.items() if key != "created_at"}


def quality_gate(spark, config, raise_on_failure=True):
    from pyspark.sql import functions as F
    stats = snapshot(spark, "stats", config).first()
    if stats is None:
        raise ValueError("Não existe carga preparada para esta release.")
    gold = snapshot(spark, "gold", config)
    totals = gold.agg(F.sum("loans").alias("loans"), F.sum("open_loans").alias("open"), F.sum("closed_loans").alias("closed")).first()
    negative = snapshot(spark, "loans", config).where(F.col("duration_hours") < 0).count()
    definitions = [
        ("nonempty", "critical", stats.bronze_rows, 1, stats.bronze_rows > 0, "Origem não vazia"),
        ("reconciliation", "critical", stats.accepted_rows + stats.duplicate_rows + stats.quarantine_rows, stats.bronze_rows,
         stats.accepted_rows + stats.duplicate_rows + stats.quarantine_rows == stats.bronze_rows, "Aceitos + duplicados + rejeitados = Bronze"),
        ("quarantine_empty", "critical", stats.quarantine_rows, 0, stats.quarantine_rows == 0, "Registros inválidos bloqueiam a publicação"),
        ("event_conflicts", "critical", stats.conflict_rows, 0, stats.conflict_rows == 0, "Chaves conflitantes exigem investigação"),
        ("gold_reconciliation", "critical", totals.loans or 0, stats.loan_rows, (totals.loans or 0) == stats.loan_rows, "Gold reconcilia com empréstimos"),
        ("open_closed_balance", "critical", (totals.open or 0) + (totals.closed or 0), totals.loans or 0,
         (totals.open or 0) + (totals.closed or 0) == (totals.loans or 0), "Abertos + concluídos = empréstimos"),
        ("negative_duration", "critical", negative, 0, negative == 0, "Duração negativa não é válida"),
        ("duplicates_present", "warning", stats.duplicate_rows, 0, stats.duplicate_rows == 0, "Duplicatas são auditadas e não duplicam o negócio"),
    ]
    rows = [{"dataset_version": config.dataset, "release_id": config.release, "rule_id": rule,
             "severity": severity, "observed": float(observed), "threshold": float(threshold),
             "status": "pass" if passed else "fail", "detail": detail, "checked_at": datetime.now(timezone.utc)}
            for rule, severity, observed, threshold, passed, detail in definitions]
    merge_frame(spark, spark.createDataFrame(rows, schema=spark.table(TABLES["checks"]).schema), TABLES["checks"], ["dataset_version", "release_id", "rule_id"])
    failures = [row["rule_id"] for row in rows if row["severity"] == "critical" and row["status"] == "fail"]
    if failures:
        release = {"dataset_version": config.dataset, "release_id": config.release, "data_reference": config.data_reference,
                   "status": "blocked", "gold_hash": "", "created_at": datetime.now(timezone.utc)}
        merge_frame(spark, spark.createDataFrame([release], schema=spark.table(TABLES["releases"]).schema), TABLES["releases"], ["dataset_version", "release_id"])
        if raise_on_failure:
            raise ValueError("QUALITY_GATE_BLOCKED: " + ", ".join(failures))
    return {"passed": not failures, "failed_rules": failures, "checks": len(rows), "warnings": sum(row["severity"] == "warning" and row["status"] == "fail" for row in rows)}


def gold_hash(spark, config):
    rows = snapshot(spark, "gold", config).drop("dataset_version", "release_id").orderBy("library_id", "loan_date").collect()
    return digest([row.asDict() for row in rows])


def publish(spark, config):
    from pyspark.sql import functions as F
    checks = snapshot(spark, "checks", config).collect()
    if len(checks) != 8 or any(row.severity == "critical" and row.status != "pass" for row in checks):
        raise ValueError("Não publicar uma release sem todos os checks críticos aprovados.")
    value = gold_hash(spark, config)
    release = {"dataset_version": config.dataset, "release_id": config.release, "data_reference": config.data_reference,
               "status": "approved", "gold_hash": value, "created_at": datetime.now(timezone.utc)}
    merge_frame(spark, spark.createDataFrame([release], schema=spark.table(TABLES["releases"]).schema), TABLES["releases"], ["dataset_version", "release_id"])
    pointer = {"dataset_version": config.dataset, "release_id": config.release, "data_reference": config.data_reference,
               "published_at": datetime.now(timezone.utc)}
    spark.createDataFrame([pointer], schema=spark.table(TABLES["active"]).schema).createOrReplaceTempView("aurora_publicacao_source")
    spark.sql(f"MERGE INTO {TABLES['active']} t USING aurora_publicacao_source s ON t.dataset_version=s.dataset_version "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    for key, name in [("gold", "workspace.estudos_gold.aurora_circulacao_atual"), ("loans", "workspace.estudos_gold.aurora_emprestimos_atuais")]:
        spark.sql(f"CREATE OR REPLACE VIEW {name} AS SELECT d.* FROM {TABLES[key]} d JOIN {TABLES['active']} p "
                  "ON d.dataset_version=p.dataset_version AND d.release_id=p.release_id")
    return {"published": True, "release_id": config.release, "gold_hash": value}


def report_document(facts):
    from decimal import Decimal, ROUND_HALF_UP
    required = ["loans", "open_loans", "closed_loans", "total_duration_hours"]
    if any(key not in facts for key in required):
        raise ValueError("Fatos incompletos para o boletim")
    if any(facts[key] < 0 for key in required) or facts["loans"] != facts["open_loans"] + facts["closed_loans"]:
        raise ValueError("Fatos não reconciliados")
    average = (Decimal(str(facts["total_duration_hours"])) / facts["closed_loans"]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if facts["closed_loans"] else None
    average_text = f"{average} horas" if average is not None else "não calculada: não há empréstimos concluídos"
    text = (f"Biblioteca {facts['library_id']} — empréstimos iniciados em setembro de 2026, "
            f"com situação até {facts['data_reference']}. "
            f"Foram {facts['loans']} empréstimos, dos quais {facts['closed_loans']} concluídos e {facts['open_loans']} em aberto. "
            f"A duração média dos concluídos foi {average_text}. "
            "Dados integralmente sintéticos. Estes números descrevem a circulação; não estabelecem causas para as variações.")
    return {"status": "validated", "template_version": "1.0.0", "facts": facts,
            "average_duration_hours": str(average) if average is not None else None, "text": text}


def generate_reports(spark, config):
    from pyspark.sql import functions as F
    active = spark.table(TABLES["active"]).where(F.col("dataset_version") == config.dataset).first()
    if active is None or active.release_id != config.release:
        raise ValueError("Boletim exige a release atualmente aprovada.")
    data = snapshot(spark, "gold", config).groupBy("library_id").agg(
        F.sum("loans").alias("loans"), F.sum("open_loans").alias("open_loans"),
        F.sum("closed_loans").alias("closed_loans"), F.sum("total_duration_hours").alias("total_duration_hours")).collect()
    records = []
    for row in data:
        facts = row.asDict()
        facts["total_duration_hours"] = float(facts["total_duration_hours"] or 0)
        facts["data_reference"] = config.data_reference
        report = report_document(facts)
        revision = digest(report)
        records.append({"report_key": digest({"dataset": config.dataset, "library": row.library_id,
                         "data_reference": config.data_reference, "template": "1.0.0"}), "dataset_version": config.dataset,
                        "release_id": config.release, "library_id": row.library_id, "data_reference": config.data_reference,
                        "revision_hash": revision, "report_json": json.dumps(report, sort_keys=True, ensure_ascii=False),
                        "text": report["text"], "created_at": datetime.now(timezone.utc)})
        write_immutable(f"{VOLUME}/{config.dataset}/boletins/{revision}.json", json.dumps(report, sort_keys=True, ensure_ascii=False, indent=2).encode())
    if records:
        merge_frame(spark, spark.createDataFrame(records, schema=spark.table(TABLES["reports"]).schema), TABLES["reports"], ["report_key", "revision_hash"])
    return {"reports": len(records), "mode": "template_python_native", "external_sends": 0, "downloads": 0}


def summary(spark, config):
    from pyspark.sql import functions as F
    row = snapshot(spark, "gold", config).agg(F.sum("loans").alias("loans"), F.sum("open_loans").alias("open_loans"),
        F.sum("closed_loans").alias("closed_loans"), F.sum("total_duration_hours").alias("total_duration_hours")).first()
    stats = snapshot(spark, "stats", config).first().asDict()
    stats.pop("created_at", None)
    return {"dataset_version": config.dataset, "release_id": config.release, "totals": row.asDict(),
            "stats": stats, "gold_hash": gold_hash(spark, config)}
