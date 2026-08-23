# v0 architecture and delivery plan

## Proposed architecture

The harness is a small, batch-oriented Python application. Connectors only
retrieve and snapshot source payloads. Normalizers convert source-specific
payloads into shared models. Pipelines attach evidence, deduplicate on explicit
keys, score validation readiness, persist to DuckDB, and export tables. No
connector may write a conclusion directly.

```text
public source -> connector -> immutable raw snapshot -> normalizer
                                                    -> evidence
                                                    -> opportunity
                                                    -> DuckDB -> CSV/Parquet/report
```

The product taxonomy is external JSON configuration. CPV-to-HS relationships
are explicitly labelled candidate mappings rather than authoritative
crosswalks. The three initial clusters can therefore be replaced without code
changes.

## Database schema

DuckDB contains six master tables:

- `explicit_demand`: one normalized procurement notice/version.
- `recurrent_accounts`: evidence-qualified recurring account candidates.
- `country_category`: period/country/product trade aggregates.
- `evidence`: immutable source-level observations and separate inferences.
- `signals`: account-level observable changes and outreach hypotheses.
- `opportunity_scores`: ten component scores, evidence references, gate
  answers, and recommendation status.

`ingestion_runs` stores connector status, counts, errors, and timestamps.
`source_records` stores source IDs, snapshot references, content hashes, and
retrieval times. Arrays and structured reasoning are JSON columns serialized in
portable exports. Primary IDs are deterministic where source identifiers are
stable, making reruns idempotent.

## Connector plan

1. **TED Search v3 (implemented):** anonymous `POST /v3/notices/search`, page
   and iteration pagination, bounded exponential retry for 429/5xx, raw JSON
   snapshots, response validation, and last-success state. Current field names
   are isolated in one preset to contain API drift.
2. **Find a Tender:** use the official OCDS release API/download where
   available; preserve releases before compiling procedures; model awards and
   framework recurrence separately from active notices.
3. **TED SPARQL:** base queries on the official `OP-TED/ted-open-data-examples`
   repository and the ontology version used by each reviewed example (the live
   service currently documents ePO 5.2); pin reviewed query files locally and
   test bindings rather than rewriting ontology terms speculatively.
4. **Eurostat Comext:** query only requested CN/product/country/period slices;
   retain flags, units, revisions, and China-share calculation inputs.
5. **UK Trade Info:** confirm the current licensed/public delivery mechanism
   and importer disclosure constraints before implementation. Never infer a
   trader identity from an aggregate table.

Every connector gets timeouts, bounded retries, pagination, raw preservation,
structured logs, a last-success record, offline tests, and a live canary that
is not required for normal unit tests.

## Implementation milestones

1. **Foundation + TED vertical slice (this PR):** models, schema, snapshots,
   TED client, normalization, conservative opportunity creation, exports,
   fixture tests.
2. **UK explicit demand:** Find a Tender releases, award/framework models,
   change detection, and recurrence evidence.
3. **Trade evidence:** Comext country/category facts and UK trader activity,
   after access and disclosure assumptions are verified.
4. **Resolution and scoring:** explainable legal-name normalization, candidate
   links, human-reviewed ambiguous merges, ten-component score and 7/12 gate.
5. **Operations:** daily GitHub Action, incremental state, material-change
   comparison, validation queue, contradiction reporting, live canaries.

## Risks and unknowns

- TED fields are flattened and can change shape or language cardinality. Raw
  snapshots and tolerant parsing limit data loss; validation warnings must be
  reviewed rather than suppressed.
- A CPV code describes procurement subject matter; an HS/CN code describes a
  traded good. Crosswalks are probabilistic and require evidence and review.
- Find a Tender release/version semantics differ from TED notice semantics.
- UK trader-level data availability, licensing, and importer/exporter role
  fields must be confirmed before automating named recurrent accounts.
- 2026 is incomplete. It must be labelled YTD with an explicit cutoff date and
  never compared to a full year without qualification.
- Procurement buyer names are not automatically commercial buyer contacts.
- Monetary values may be lot-level, multi-currency, missing, or ranges; v0 does
  not sum them unless units and scope are unambiguous.

## Minimal first PR scope

The first PR ends at a reproducible, offline-tested TED vertical slice. It does
not claim recurrent accounts, contactability, HS certainty, or recommended
opportunities. A TED notice becomes `Verified Explicit Demand` only when a
source notice ID, buyer identity, requirement/title, and source link are all
present; otherwise it is partial or unverified. Recommendation remains `Needs
Further Validation` until at least seven evidence-backed gate answers exist.

## Primary references

- [TED Search API v3](https://docs.ted.europa.eu/api/latest/search.html)
- [TED Search pagination and iteration](https://docs.ted.europa.eu/ODS/latest/reuse/search-api.html)
- [TED official search-field list](https://docs.ted.europa.eu/ODS/latest/reuse/field-list.html)
- [OP-TED Open Data examples](https://github.com/OP-TED/ted-open-data-examples)
- [TED Search API reference implementation](https://github.com/kasey6801/TED-Search-API)
