"""DuckDB persistence and immutable raw snapshots."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
from pydantic import BaseModel


class SnapshotReference(BaseModel):
    path: str
    content_sha256: str
    retrieved_at: datetime
    source_url: str


class SnapshotStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def write_json(
        self,
        *,
        source: str,
        source_record_id: str,
        payload: Any,
        request: dict[str, Any] | None,
        source_url: str,
    ) -> SnapshotReference:
        retrieved_at = datetime.now(UTC)
        envelope = {
            "source": source,
            "source_record_id": source_record_id,
            "source_url": source_url,
            "retrieved_at": retrieved_at.isoformat(),
            "request": request,
            "payload": payload,
        }
        encoded = json.dumps(envelope, ensure_ascii=False, sort_keys=True, default=str).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        directory = self.root / source / retrieved_at.strftime("%Y/%m/%d")
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{source_record_id}-{digest[:12]}.json"
        if not path.exists():
            path.write_bytes(encoded)
        return SnapshotReference(
            path=str(path),
            content_sha256=digest,
            retrieved_at=retrieved_at,
            source_url=source_url,
        )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS evidence (
  evidence_id VARCHAR PRIMARY KEY, source_name VARCHAR, source_type VARCHAR,
  source_tier INTEGER, source_url VARCHAR, retrieved_at TIMESTAMPTZ,
  data_period VARCHAR, raw_source_id VARCHAR,
  raw_text_or_payload_reference VARCHAR, observed_fact VARCHAR,
  inference VARCHAR, confidence DOUBLE, limitations JSON
);
CREATE TABLE IF NOT EXISTS explicit_demand (
  notice_id VARCHAR PRIMARY KEY, buyer VARCHAR, buyer_country VARCHAR,
  buyer_city VARCHAR, product_category VARCHAR, cpv_codes JSON,
  candidate_hs_codes JSON, demand_type VARCHAR, publication_date DATE,
  deadline DATE, value DECIMAL(38, 6), currency VARCHAR, quantity VARCHAR,
  requirement VARCHAR, procurement_type VARCHAR, qualifications VARCHAR,
  delivery_requirements VARCHAR, source VARCHAR, source_url VARCHAR,
  current_status VARCHAR, evidence_strength VARCHAR, confidence DOUBLE,
  next_action VARCHAR, evidence_ids JSON
);
CREATE TABLE IF NOT EXISTS opportunity_scores (
  opportunity_id VARCHAR PRIMARY KEY, notice_id VARCHAR, account_id VARCHAR,
  buyer_identifiability INTEGER, buyer_contactability INTEGER,
  procurement_recurrence INTEGER, procurement_visibility INTEGER,
  supplier_switchability INTEGER, chinese_supply_advantage INTEGER,
  regulatory_accessibility INTEGER, sales_cycle_accessibility INTEGER,
  repeat_revenue_potential INTEGER, margin_potential INTEGER,
  total_score INTEGER, reasoning JSON, evidence_ids JSON, gate_answers JSON,
  recommendation_status VARCHAR, scored_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS recurrent_accounts (
  company_id VARCHAR PRIMARY KEY, company_name VARCHAR, country VARCHAR,
  website VARCHAR, company_type VARCHAR, products JSON, relevant_hs_codes JSON,
  procurement_evidence JSON, trade_evidence JSON, import_evidence JSON,
  china_sourcing_evidence JSON, months_active INTEGER, estimated_scale VARCHAR,
  buyer_role VARCHAR, contact_status VARCHAR, buying_signals JSON,
  switching_hypothesis VARCHAR, evidence_strength VARCHAR, confidence DOUBLE,
  next_action VARCHAR
);
CREATE TABLE IF NOT EXISTS country_category (
  metric_id VARCHAR PRIMARY KEY, reporter_country VARCHAR, partner_country VARCHAR,
  product_category VARCHAR, hs_cn_code VARCHAR, period VARCHAR, period_type VARCHAR,
  import_value DOUBLE, quantity DOUBLE, quantity_unit VARCHAR, china_share DOUBLE,
  evidence_ids JSON
);
CREATE TABLE IF NOT EXISTS signals (
  signal_id VARCHAR PRIMARY KEY, company_id VARCHAR, signal_type VARCHAR,
  happened VARCHAR, why_it_matters VARCHAR, outreach_angle VARCHAR,
  observed_at DATE, evidence_ids JSON
);
CREATE TABLE IF NOT EXISTS source_records (
  source_name VARCHAR, source_record_id VARCHAR, snapshot_reference VARCHAR,
  content_sha256 VARCHAR, retrieved_at TIMESTAMPTZ,
  PRIMARY KEY(source_name, source_record_id, content_sha256)
);
CREATE TABLE IF NOT EXISTS ingestion_runs (
  run_id VARCHAR PRIMARY KEY, connector VARCHAR, started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ, status VARCHAR, records_fetched INTEGER,
  records_written INTEGER, error VARCHAR, last_success_at TIMESTAMPTZ
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(path))

    def initialize(self) -> None:
        self.connection.execute(SCHEMA_SQL)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "Database":
        self.initialize()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
