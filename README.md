# European B2B Demand Intelligence Harness

Private, provenance-first research tooling that turns public procurement and
trade observations into evidence-backed commercial validation queues. The
implemented source and operational paths are:

```text
TED Search API ----------> explicit demand --------------------------+
Find a Tender OCDS ------> releases, awards, parties ----------------+--> DuckDB
UK Trade Info OData -----> trader/product/month activity ------------+
                                   |                                      |
                                   +--> current-state reconciliation -----+
                                                                          |
                 material changes <------+----> entity-review queue       |
                 selected-category current view <-------------------------+
                                                                          |
                                               CSV + Parquet + daily report
```

The harness does **not** call a company a procurement buyer or supplier
without procurement evidence, or an importer/exporter without trade evidence.
Observed facts, entity-link decisions, taxonomy candidates, and commercial
next actions remain separate and auditable.

## Quick start

```bash
python -m venv .venv
.venv/bin/pip install '.[dev]'
.venv/bin/demand-harness init-db
.venv/bin/demand-harness fixture tests/fixtures/ted_search_page.json
```

The fixture command is offline. It snapshots and processes the checked-in TED
fixture, reconciles derived current state, evaluates links, records material
changes, refreshes the entity-review queue, and writes exports.

### Find a Tender

Fetch a bounded Find a Tender canary containing tender and award-stage
releases:

```bash
.venv/bin/demand-harness find-tender \
  --stage tender --stage award \
  --updated-from 2026-08-01T00:00:00 \
  --limit 100 --max-pages 1
```

Omitting `--stage` runs separate tender and award streams. `--updated-to` pins
the other end of an incremental window, and `--notice-id 076913-2026`
retrieves a specific notice. The command also accepts `--state-file` and the
common `--database`, `--raw-dir`, `--exports-dir`, and `--taxonomy` paths.
Limits and page counts must be positive integers.

### UK Trade Info

Fetch named trader/product/month observations for selected HS/CN prefixes:

```bash
.venv/bin/demand-harness uk-trade-info \
  --hs-prefix 8203 --hs-prefix 8302 \
  --year 2025 --trader-filter All \
  --minimum-months 2 --max-pages 1
```

`--trader-filter` accepts `Importers`, `Exporters`, or `All`. An HS/CN prefix
contains two to eight digits, multiple `--hs-prefix` options are processed as
separate source scopes, and `--minimum-months` must be at least two.

The first completed or partial UK Trade Info run fixes the recurrence threshold
for that database. Every later UK Trade Info run must use the same
`--minimum-months`. Establishing a different policy requires a new database;
an audited rebaseline workflow is not implemented.

The one-page command is a live canary, not an authoritative current-state run.
A bounded, interrupted, or failed run never treats unobserved rows as removed
and cannot authorize the selected-category commercial view. Omit
`--max-pages` to follow the source-owned pagination to natural exhaustion:

```bash
.venv/bin/demand-harness uk-trade-info \
  --hs-prefix 8203 \
  --year 2025 --trader-filter Importers \
  --minimum-months 2
```

Only a naturally exhausted run is marked completed and authoritative. It can
reconcile removed or suppressed rows within that exact year, prefix, and
trader-filter scope. If every requested prefix completes authoritatively, the
command also refreshes the matching commercial view.

### Selected-category commercial view

Define or replace the selected-category view with `commercial-export`:

```bash
.venv/bin/demand-harness commercial-export \
  --year 2025 --hs-prefix 8203 \
  --trader-filter Importers

.venv/bin/demand-harness find-tender \
  --notice-id 076913-2026
```

Select either repeatable `--hs-prefix` options or repeatable taxonomy
`--category` IDs, never both. For example:

```bash
.venv/bin/demand-harness commercial-export \
  --year 2025 --category non_powered_hand_tools \
  --trader-filter Importers
```

By default, `commercial-export` resolves the latest compatible completed,
authoritative UK Trade Info run for every selected prefix. It prefers the
narrowest compatible scope and then its latest completion. Repeatable
`--authoritative-run-id` options can pin exact runs for reproducibility. A
taxonomy category requires authoritative coverage for all of its configured
candidate HS prefixes; a narrow HS-prefix selection needs only compatible
coverage for that prefix. `All` coverage can support importer or exporter
views, but a specific-role scope cannot support `All` or the opposite role.
Explicit `--authoritative-run-id` values receive the same trader-filter
compatibility check before materialization. Coverage from an `All` run can
support a specific requested role, but the commercial rows are still filtered
to that requested role.

The recurrence threshold is read from the selected authoritative UK Trade Info
runs and must match the database-wide policy; mixed thresholds are rejected as
a defensive check. `commercial-export` cannot override it. Both the
current-view rows and persisted selection state record that threshold, and
scopes with fewer active months are excluded.

The explicit selection, trader filter, taxonomy path, authoritative coverage
run IDs, and recurrence threshold are persisted in
`selected_category_commercial_view_state`. Once that state exists,
`find-tender` and `link` automatically refresh the same commercial selection
after procurement facts or entity-link decisions change. If no selection has
yet been persisted, those commands leave the commercial view unmaterialized.

The resulting `selected_category_commercial_view` answers:

> Which companies in the selected product categories show repeated trade
> activity, and which of those companies also have public procurement
> evidence?

Rows remain separate by activity year, disclosed commodity code and level, and
trade role. The export includes company, product, active months, role,
procurement facts and roles, source count, confidence, validation status, next
action, accepted-link evidence, and the authoritative coverage runs. It ranks
procurement-linked rows first, followed by evidence-source count, recurrent
months, confidence, and deterministic tie-breakers. This is a concise ordering,
not an advanced commercial score.

### Offline relinking and export

To re-evaluate conservative links, reconcile opportunities, record material
changes, refresh the review queue, and regenerate exports without a network
request:

```bash
.venv/bin/demand-harness link \
  --database data/demand.duckdb \
  --exports-dir data/exports
```

`link` automatically refreshes the last persisted commercial selection. Run
`commercial-export` only to define the first selection, replace it, or pin a
different set of authoritative runs.

## Operational reliability

- UK Trade Info runs are scoped by year, HS/CN prefix, and trader filter. One
  active lease prevents concurrent runs from reconciling the same scope.
- Small UK Trade Info pages persist atomically. Large official pages use
  bounded, audited positive-observation transactions followed by bounded
  recurrence rebuilds. A page is counted only after every observation and
  recurrence batch succeeds. If a later batch fails, earlier positive batches
  remain auditable, but the run is non-authoritative and carries no negative
  evidence. Completed authoritative finalization applies removals and rebuilds
  affected recurrence in one transaction.
- Scope membership records `new`, `unchanged`, `corrected`, `removed`, and
  `restored` observations. A row remains current while any overlapping tracked
  scope still reports it.
- Find a Tender retains immutable release observations. The latest observed
  payload for a release ID is current; prior payloads are marked replaced, and
  correction or restoration transitions remain auditable.
- Opportunity current state distinguishes active, closed, and expired tenders.
  Explicit source closure, a later closing release for the same OCID, or an
  elapsed deadline can close an opportunity. Absence from an incremental Find
  a Tender response is never interpreted as closure.
- Every source command and `link` reconciles Find a Tender opportunities,
  evaluates entity links, detects material changes, refreshes the review queue,
  and then exports. The exporter itself only serializes existing database state.

The material-change vocabulary is:

- `new_tender`
- `tender_closed`
- `deadline_changed`
- `value_changed`
- `new_award`
- `new_supplier`
- `new_recurrent_trader`
- `recurrence_threshold_crossed`
- `procurement_evidence_newly_linked`
- `evidence_contradiction`

The baseline and emitted events commit together. The first detector run reports
current qualifying facts as new; later equivalent or unchanged facts are
silent. Source, link, and other unrelated CLI detector runs reuse the latest
persisted recurrence threshold instead of reverting to a default, preventing a
threshold change from creating false recurrence events. `daily_report.md`
contains events only from the latest detector run.

## Outputs

Outputs are written under `data/`, which is ignored except for `.gitkeep`
files. Every table in the export allowlist is emitted as CSV and Parquet. The
Phase 3 operational additions are:

- run and scope audit: `ingestion_run_scopes`, `ingestion_run_records`,
  `active_scope_leases`, and `source_scope_membership`;
- procurement versions and lifecycle: `procurement_release_observations`,
  `release_transitions`, `opportunity_current_state`, and
  `opportunity_state_history`;
- meaningful changes: `material_change_state`, `change_detection_runs`, and
  `change_events`;
- human and commercial views: `entity_review_queue`,
  `selected_category_commercial_view_state`, and
  `selected_category_commercial_view`.

They accompany the source, evidence, demand, procurement, trade, company,
linking, recurrence, aggregate, signal, and score tables. The review-queue
export contains current cases only; non-current queue history remains in
DuckDB. `change_events` retains event history, while the Markdown report shows
the latest run plus the current commercial top rows and review count. See
[docs/architecture.md](docs/architecture.md) for the complete data contracts.

## Quality checks

```bash
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/mypy src
```

## Audit contract

- Every normalized record has one or more evidence IDs.
- Every evidence row points at an immutable raw snapshot and source URL.
- `observed_fact` and `inference` are separate fields.
- Missing data stays `null`; it is never inferred merely to fill a column.
- Ambiguous entities are never silently merged.
- Human review metadata never changes an entity link or merges source
  identities by itself.

## Interpretation limits

- A UK Trade Info row proves that HMRC published a named trader, commodity,
  month, and importer/exporter role. The public named-trader feed does not
  disclose trader-level values, quantities, partner/origin country, customers,
  VAT number, or EORI number. Those columns remain null.
- UK Trade Info evidence does not prove UK incorporation, commercial scale, a
  supplier relationship, or China sourcing. The harness never infers China
  sourcing from importer status, product code, address, or repeated months.
- Recurrence is qualified within one disclosed commodity code and code level,
  trader role, and calendar year. `recurrent_account_scopes` preserves that
  exact basis; `recurrent_accounts` is the company-level rollup. Neither is a
  volume estimate. A historic activity year is not evidence that a company is
  trading today.
- Removal or suppression is established only after a completed authoritative
  UK Trade Info run for the exact tracked scope. Missing months still do not
  prove inactivity because named-trader publication is incomplete.
- A link is accepted only for a unique exact normalized legal-name candidate
  corroborated by a matching strong typed identifier, exact postcode, or exact
  normalized address. Ambiguous, conflicting, and insufficiently corroborated
  candidates remain explicit non-links; superseded decisions are non-current;
  source identities are never coalesced.
- The human queue captures candidate identities, support, conflicts,
  confidence, and the reason automatic linking was rejected. Updating its
  workflow status does not attach procurement evidence; an explicit accepted
  link decision is still required.
- Find a Tender awards remain award evidence. Only current, non-expired,
  taxonomy-matched active tenders enter explicit demand and opportunity
  scoring. Incremental absence is not a Find a Tender tombstone.
- Procurement evidence linked to a trader proves an entity relationship to the
  public record, not that the procurement subject matches the traded product.
  CPV-to-HS mappings remain candidate mappings requiring review.
- The selected-category table is one replaceable current view, not a history of
  every commercial selection. Its ranking is deliberately simple and does not
  prove purchase volume, switching intent, contactability, or market priority.
- A hard process termination can leave an active source-scope lease requiring
  audited manual recovery before the same scope can run again.

## Next milestone

Phase 3 operational reliability is implemented. Eurostat Comext is the next
major source milestone. TED SPARQL, a frontend, an MCP server, paid shipment
databases, contact enrichment, and large-scale country expansion remain
deferred.
