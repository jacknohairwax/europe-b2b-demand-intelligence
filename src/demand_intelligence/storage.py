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
    payload_sha256: str | None = None
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
        payload_bytes = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, default=str
        ).encode()
        encoded = json.dumps(envelope, ensure_ascii=False, sort_keys=True, default=str).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        payload_digest = hashlib.sha256(payload_bytes).hexdigest()
        directory = self.root / source / retrieved_at.strftime("%Y/%m/%d")
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{source_record_id}-{digest[:12]}.json"
        if not path.exists():
            path.write_bytes(encoded)
        return SnapshotReference(
            path=str(path),
            content_sha256=digest,
            payload_sha256=payload_digest,
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
  next_action VARCHAR, activity_year INTEGER, recurrent_product_roles JSON
);
ALTER TABLE recurrent_accounts
  ADD COLUMN IF NOT EXISTS activity_year INTEGER;
ALTER TABLE recurrent_accounts
  ADD COLUMN IF NOT EXISTS recurrent_product_roles JSON;
CREATE TABLE IF NOT EXISTS recurrent_account_scopes (
  company_id VARCHAR, activity_year INTEGER, commodity_code VARCHAR,
  commodity_code_level VARCHAR, trade_role VARCHAR, active_months JSON,
  evidence_ids JSON,
  PRIMARY KEY(company_id, activity_year, commodity_code, commodity_code_level, trade_role)
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
CREATE TABLE IF NOT EXISTS ingestion_run_scopes (
  run_id VARCHAR PRIMARY KEY, connector VARCHAR, scope_key VARCHAR,
  scope_parameters JSON, started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ,
  status VARCHAR, completion_reason VARCHAR, is_authoritative BOOLEAN,
  pages_fetched INTEGER, records_seen INTEGER, error VARCHAR
);
CREATE TABLE IF NOT EXISTS ingestion_run_records (
  run_id VARCHAR, source_record_id VARCHAR, payload_sha256 VARCHAR,
  observation_state VARCHAR,
  PRIMARY KEY(run_id, source_record_id)
);
CREATE TABLE IF NOT EXISTS active_scope_leases (
  connector VARCHAR, scope_key VARCHAR, run_id VARCHAR, acquired_at TIMESTAMPTZ,
  PRIMARY KEY(connector, scope_key)
);
CREATE TABLE IF NOT EXISTS source_scope_membership (
  connector VARCHAR, scope_key VARCHAR, source_record_id VARCHAR,
  current_payload_sha256 VARCHAR, previous_payload_sha256 VARCHAR,
  is_present BOOLEAN, first_seen_run_id VARCHAR, last_seen_run_id VARCHAR,
  first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ,
  removed_at TIMESTAMPTZ, removed_by_run_id VARCHAR,
  last_change_type VARCHAR, state_changed_at TIMESTAMPTZ,
  PRIMARY KEY(connector, scope_key, source_record_id)
);
CREATE TABLE IF NOT EXISTS trade_activity (
  activity_id VARCHAR PRIMARY KEY, source_record_id VARCHAR, company_id VARCHAR,
  trader_id BIGINT, company_name VARCHAR, address_lines JSON, postcode VARCHAR,
  commodity_code VARCHAR, hs2_code VARCHAR, hs4_code VARCHAR, hs6_code VARCHAR,
  product_description VARCHAR, year INTEGER, month INTEGER, trade_role VARCHAR,
  value DECIMAL(38, 6), quantity DECIMAL(38, 6), partner_country VARCHAR,
  source_url VARCHAR, evidence_id VARCHAR, raw_payload_reference VARCHAR,
  payload_sha256 VARCHAR, commodity_code_level VARCHAR,
  is_current BOOLEAN DEFAULT TRUE, first_seen_run_id VARCHAR,
  last_seen_run_id VARCHAR, removed_at TIMESTAMPTZ
);
ALTER TABLE trade_activity
  ADD COLUMN IF NOT EXISTS commodity_code_level VARCHAR;
ALTER TABLE trade_activity
  ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
ALTER TABLE trade_activity
  ADD COLUMN IF NOT EXISTS first_seen_run_id VARCHAR;
ALTER TABLE trade_activity
  ADD COLUMN IF NOT EXISTS last_seen_run_id VARCHAR;
ALTER TABLE trade_activity
  ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ;
CREATE TABLE IF NOT EXISTS canonical_companies (
  company_id VARCHAR PRIMARY KEY, preferred_name VARCHAR, normalized_name VARCHAR,
  country VARCHAR, original_names JSON, identifiers JSON, addresses JSON,
  evidence_ids JSON, first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS company_source_roles (
  company_id VARCHAR, source_name VARCHAR, source_record_id VARCHAR,
  original_name VARCHAR, role VARCHAR, evidence_id VARCHAR,
  PRIMARY KEY(company_id, source_name, source_record_id, role)
);
CREATE TABLE IF NOT EXISTS entity_link_decisions (
  decision_id VARCHAR PRIMARY KEY, left_company_id VARCHAR, right_company_id VARCHAR,
  normalized_name VARCHAR, status VARCHAR, match_type VARCHAR, confidence DOUBLE,
  reasoning VARCHAR, evidence_ids JSON, decided_at TIMESTAMPTZ,
  is_current BOOLEAN DEFAULT TRUE
);
ALTER TABLE entity_link_decisions
  ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
CREATE TABLE IF NOT EXISTS procurement_releases (
  source_name VARCHAR, release_id VARCHAR, ocid VARCHAR,
  release_payload_sha256 VARCHAR, package_payload_sha256 VARCHAR,
  package_version VARCHAR, package_uri VARCHAR, snapshot_reference VARCHAR,
  snapshot_content_sha256 VARCHAR, retrieved_at TIMESTAMPTZ,
  publication_date TIMESTAMPTZ, stages JSON, tender_status VARCHAR,
  buyer_id VARCHAR, buyer_name VARCHAR, buyer_location JSON,
  title VARCHAR, description VARCHAR, cpv_codes JSON, deadline TIMESTAMPTZ,
  procurement_type VARCHAR, procurement_category VARCHAR, is_framework BOOLEAN,
  framework_stage VARCHAR, framework_periods JSON,
  value DECIMAL(38, 6), currency VARCHAR, value_basis VARCHAR,
  award_statuses JSON, awarded_suppliers JSON, product_category VARCHAR,
  candidate_hs_codes JSON, taxonomy_mapping_status VARCHAR, source_url VARCHAR,
  evidence_ids JSON, limitations JSON, raw_release JSON,
  is_current BOOLEAN DEFAULT TRUE, version_state VARCHAR DEFAULT 'current',
  superseded_at TIMESTAMPTZ, superseded_by_sha256 VARCHAR,
  PRIMARY KEY(source_name, release_id, release_payload_sha256)
);
ALTER TABLE procurement_releases
  ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
ALTER TABLE procurement_releases
  ADD COLUMN IF NOT EXISTS version_state VARCHAR DEFAULT 'current';
ALTER TABLE procurement_releases
  ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ;
ALTER TABLE procurement_releases
  ADD COLUMN IF NOT EXISTS superseded_by_sha256 VARCHAR;
CREATE TABLE IF NOT EXISTS procurement_release_observations (
  source_name VARCHAR, release_id VARCHAR, observation_id VARCHAR,
  release_payload_sha256 VARCHAR, observed_at TIMESTAMPTZ,
  snapshot_reference VARCHAR, snapshot_content_sha256 VARCHAR,
  PRIMARY KEY(source_name, release_id, observation_id)
);
CREATE TABLE IF NOT EXISTS release_transitions (
  transition_id VARCHAR PRIMARY KEY, source_name VARCHAR, release_id VARCHAR,
  from_payload_sha256 VARCHAR, to_payload_sha256 VARCHAR,
  transition_type VARCHAR, observed_at TIMESTAMPTZ, observation_id VARCHAR
);
CREATE TABLE IF NOT EXISTS opportunity_current_state (
  opportunity_id VARCHAR PRIMARY KEY, source_name VARCHAR, release_id VARCHAR,
  ocid VARCHAR, lifecycle VARCHAR, state_reason VARCHAR, source_status VARCHAR,
  deadline TIMESTAMPTZ, release_payload_sha256 VARCHAR,
  source_observed_at TIMESTAMPTZ, state_changed_at TIMESTAMPTZ,
  evidence_ids JSON
);
CREATE TABLE IF NOT EXISTS opportunity_state_history (
  history_id VARCHAR PRIMARY KEY, opportunity_id VARCHAR, prior_lifecycle VARCHAR,
  lifecycle VARCHAR, state_reason VARCHAR, source_name VARCHAR,
  release_id VARCHAR, release_payload_sha256 VARCHAR,
  source_observed_at TIMESTAMPTZ, recorded_at TIMESTAMPTZ, evidence_ids JSON
);
CREATE TABLE IF NOT EXISTS procurement_awards (
  source_name VARCHAR, release_id VARCHAR, release_payload_sha256 VARCHAR,
  award_index INTEGER, award_id VARCHAR, title VARCHAR, status VARCHAR,
  award_date TIMESTAMPTZ, value DECIMAL(38, 6), currency VARCHAR,
  value_basis VARCHAR, contract_status VARCHAR,
  contract_value DECIMAL(38, 6), contract_currency VARCHAR,
  contract_value_basis VARCHAR, contract_period JSON, suppliers JSON,
  evidence_id VARCHAR, raw_award JSON,
  PRIMARY KEY(source_name, release_id, release_payload_sha256, award_index)
);
CREATE TABLE IF NOT EXISTS procurement_parties (
  source_name VARCHAR, release_id VARCHAR, release_payload_sha256 VARCHAR,
  party_index INTEGER, party_id VARCHAR, name VARCHAR, roles JSON,
  identifiers JSON, location JSON, evidence_id VARCHAR, raw_party JSON,
  PRIMARY KEY(source_name, release_id, release_payload_sha256, party_index)
);
CREATE TABLE IF NOT EXISTS material_change_state (
  namespace VARCHAR, subject_id VARCHAR, state_hash VARCHAR,
  state_payload JSON, is_present BOOLEAN, first_seen_run_id VARCHAR,
  last_seen_run_id VARCHAR, first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  PRIMARY KEY(namespace, subject_id)
);
CREATE TABLE IF NOT EXISTS change_events (
  event_id VARCHAR PRIMARY KEY, run_id VARCHAR, connector VARCHAR,
  scope_key VARCHAR, event_type VARCHAR, subject_type VARCHAR,
  subject_id VARCHAR, field_name VARCHAR, old_value JSON, new_value JSON,
  evidence_ids JSON, confidence DOUBLE, detected_at TIMESTAMPTZ,
  details JSON
);
CREATE TABLE IF NOT EXISTS change_detection_runs (
  run_id VARCHAR PRIMARY KEY, connector VARCHAR, scope_key VARCHAR,
  detected_at TIMESTAMPTZ, completed_authoritative BOOLEAN,
  recurrence_threshold INTEGER, event_count INTEGER
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(path))

    def initialize(self) -> None:
        self.connection.execute(SCHEMA_SQL)
        self.connection.execute(
            """INSERT OR IGNORE INTO procurement_release_observations
               SELECT source_name, release_id,
                      sha256(
                        coalesce(snapshot_content_sha256, '') || '|' ||
                        CAST(retrieved_at AS VARCHAR) || '|' ||
                        release_payload_sha256
                      ),
                      release_payload_sha256, retrieved_at, snapshot_reference,
                      snapshot_content_sha256
               FROM procurement_releases AS release
               WHERE NOT EXISTS (
                 SELECT 1 FROM procurement_release_observations AS observation
                 WHERE observation.source_name = release.source_name
                   AND observation.release_id = release.release_id
                   AND observation.release_payload_sha256 =
                       release.release_payload_sha256
                   AND observation.observed_at = release.retrieved_at
               )"""
        )
        self.connection.execute(
            """UPDATE procurement_releases AS target
               SET is_current = target.release_payload_sha256 = winner.current_sha256,
                   version_state = CASE
                     WHEN target.release_payload_sha256 = winner.current_sha256
                       THEN 'current'
                     ELSE 'replaced'
                   END,
                   superseded_at = CASE
                     WHEN target.release_payload_sha256 = winner.current_sha256
                       THEN NULL
                     ELSE winner.current_retrieved_at
                   END,
                   superseded_by_sha256 = CASE
                     WHEN target.release_payload_sha256 = winner.current_sha256
                       THEN NULL
                     ELSE winner.current_sha256
                   END
               FROM (
                 SELECT source_name, release_id,
                        release_payload_sha256 AS current_sha256,
                        observed_at AS current_retrieved_at
                 FROM (
                   SELECT source_name, release_id, release_payload_sha256,
                          observed_at,
                          row_number() OVER (
                            PARTITION BY source_name, release_id
                            ORDER BY observed_at DESC, observation_id DESC
                          ) AS version_rank
                   FROM procurement_release_observations
                 ) ranked
                 WHERE version_rank = 1
               ) winner
               WHERE target.source_name = winner.source_name
                 AND target.release_id = winner.release_id"""
        )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "Database":
        self.initialize()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
