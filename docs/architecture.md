# Demand intelligence architecture and delivery plan

## Implemented architecture

The harness is a small, batch-oriented Python application. Connectors retrieve
and snapshot source payloads; normalizers convert source-specific payloads into
shared observations; pipelines attach evidence and persist deterministic
records. Reconciliation, change detection, entity linking, and commercial
materialization operate on those records rather than writing conclusions from
a connector response.

```text
TED ----------------------> notice ----------> explicit demand --------+
Find a Tender ------------> OCDS release ----> tender/award facts ------+--> DuckDB
UK Trade Info ------------> trader month ----> recurrent activity ------+
                                                                          |
source scope membership -------> present / corrected / removed ----------+
release observations ----------> current / replaced + lifecycle ---------+
                                                                          |
current facts --> material changes --> latest-run report                  |
current identities --> link decisions --> human review queue             |
current recurrence + accepted links --> selected-category commercial view+
                                                                          |
                                                     CSV + Parquet exports
```

Every source payload is retained in an immutable snapshot. Stable row-level
payload hashes distinguish repeated observations from changed records even
when they appear in different page snapshots. The product taxonomy remains
external JSON configuration. CPV-to-HS relationships are candidate mappings,
not an authoritative crosswalk.

## Database schema

### Procurement observations and opportunities

- `procurement_releases` stores every Find a Tender release payload version,
  its OCID, stages, buyer, CPV codes, framework facts, raw release, payload
  hashes, snapshot, taxonomy result, and version state.
- `procurement_release_observations` records each immutable observation of a
  release payload. The chronologically latest observation selects the current
  payload for that release ID.
- `release_transitions` preserves changes between current payload hashes as
  `corrected` or `restored` transitions. Replaced payload rows remain stored.
- `procurement_awards` stores every award independently, including exact
  status, suppliers, award value, and linked contract value. Values with
  different bases are never silently combined.
- `procurement_parties` retains source party roles, identifiers, locations,
  and raw party payloads. An identifier is not assumed global or strongly
  typed unless the source supplies its scheme.
- `opportunity_current_state` records the current active, closed, or expired
  state and reason for each materialized Find a Tender opportunity.
- `opportunity_state_history` appends each lifecycle transition with its source
  release, payload, observation time, and evidence.
- `explicit_demand` contains active opportunities derived from TED or Find a
  Tender. Award-only and inactive records do not enter this table.

### Trade, companies, and recurrence

- `trade_activity` stores one UK Trade Info trader, disclosed commodity code
  and level, month, and role tuple. It also records whether the observation is
  current, its first and last seen run, and any removal time. Undisclosed value,
  quantity, and partner-country fields remain null.
- `canonical_companies` stores source-scoped company identities and observed
  names, identifiers, addresses, and source location labels. A UK address or
  country label is not evidence of legal incorporation.
- `company_source_roles` records the source record and observed role behind
  each company.
- `entity_link_decisions` records candidate-link status, method, confidence,
  reasoning, evidence, and whether the decision is current. Superseded
  decisions remain auditable and cannot keep procurement enrichment active.
- `recurrent_account_scopes` records each qualifying calendar year,
  commodity code and disclosure level, trader role, exact months, and evidence.
- `recurrent_accounts` is the company-level rollup across qualifying scopes,
  with procurement evidence only when a supported current link exists.

### Run and current-state audit

- `ingestion_runs` retains the original connector-level run ledger.
- `ingestion_run_scopes` records the exact source scope, parameters, status,
  completion reason, page and record counts, and whether completion was
  authoritative.
- `ingestion_run_records` records the payload hash and observation state for
  each source row seen or removed in a scoped run.
- `active_scope_leases` permits only one active run for a connector and scope.
- `source_scope_membership` tracks present/absent membership, current and prior
  hashes, first/last seen runs, removal provenance, and the last state change.
- `source_records` preserves immutable source-record snapshots and hashes.

### Changes, review, and commercial current view

- `material_change_state` is the persistent canonical baseline used by the
  detector. It is not a notification feed.
- `change_detection_runs` records each detector invocation, scope, authority,
  recurrence threshold, timestamp, and event count.
- `change_events` records only supported material transitions, including old
  and new values, evidence, confidence, and details.
- `entity_review_queue` materializes current non-accepted company-link
  candidates with both identities, supporting and conflicting evidence,
  confidence, and rejection reason. Human workflow fields remain separate.
- `selected_category_commercial_view` is one replaceable, exact-scope current
  answer containing recurrence, current procurement evidence, accepted-link
  evidence, source count, confidence, validation status, next action, and the
  authoritative UK Trade Info runs and recurrence threshold used.
- `selected_category_commercial_view_state` persists the last explicit year,
  selector type and values, trader filter, taxonomy path, authoritative
  coverage runs, and recurrence threshold so procurement and link updates can
  refresh that same commercial view.

The existing `country_category`, `evidence`, `signals`, and
`opportunity_scores` tables remain. Arrays and structured objects are JSON
columns in DuckDB and are retained in CSV and Parquet exports.

## Connector contracts

### TED Search v3

The anonymous `POST /v3/notices/search` connector supports page-number and
iteration pagination, bounded retry for 429/5xx responses, raw JSON snapshots,
response validation, and last-success state. Drift-sensitive flattened field
names are isolated in one preset.

### Find a Tender OCDS

The connector uses the anonymous official
`GET /api/1.0/ocdsReleasePackages` endpoint. The service limit is 100 releases
per page. Supported filters are a single upstream stage (`planning`, `tender`,
or `award`) and `updatedFrom`/`updatedTo`; the CLI can run tender and award
streams in one invocation. It follows the service-owned `links.next` URL
verbatim, bounds retries, honors `Retry-After`, snapshots every package, and
preserves original releases rather than treating a compiled process as the
audit source. A specific notice can be fetched by notice ID.

Each observed release payload is stored independently. The latest observation
time for a release ID selects the current payload; prior versions are marked
`replaced`. If a previously seen payload becomes current again, the transition
is recorded as `restored`; otherwise a changed payload is `corrected`.

A release is eligible for derived explicit demand only when it is a current,
non-expired active tender and matches the configured product taxonomy. Awards
and contracts remain procurement evidence and are not scored as open
opportunities. CLI operational reconciliation expires elapsed deadlines and
retracts a prior tender when its current source status closes it, a later
release for the same OCID explicitly observes closure, or it leaves the
selected taxonomy. Incremental absence is never interpreted as closure or
suppression. Multiple awards, suppliers, lots, monetary bases, and periods
remain separate.

Framework establishment is observed from
`tender.techniques.hasFrameworkAgreement`; a call-off is observed from a
`relatedProcesses` relationship of `framework`. Tender, lot, award, and
contract periods retain their source context rather than being collapsed into
an inferred framework duration.

### UK Trade Info

The connector uses the anonymous HMRC OData `GET /Trade` endpoint. It filters
by year, CN8/HS prefix, and optional trader role, selects the public activity
keys, and expands published trader, commodity, and trade-type records. It
leaves page sizing to the service and follows `@odata.nextLink` verbatim. An
optional source `$top` is a total-result bound rather than a page-size setting.
`TradeTypeId` 1 is recorded as importer, 2 as exporter, and any other value
remains unresolved.

The stable activity key is trader, commodity, month, and trade type. Recurrence
requires the configured number of distinct months within the same calendar
year, disclosed commodity code and level, and trade role. Aggregate HS2, HS4,
or HS6 disclosures remain at their actual level rather than being padded into
an invented CN8 code.

Each CLI scope is identified by year, HS/CN prefix, and trader filter. Starting
a run acquires an atomic lease for that exact connector/scope pair. A small
source page persists its source rows, evidence, canonical identities, trade
activity, scope membership, and recurrence in one transaction.

Large official responses use bounded transactions to control memory. Each
committed observation batch contains only audited positive facts; affected
recurrence is then rebuilt in bounded company batches. The source page count is
incremented only after every observation and recurrence batch succeeds. If a
later batch fails, only that batch is rolled back: earlier audited positives
remain stored, the run fails as non-authoritative, and no unseen row can be
tombstoned. `records_seen` can therefore be positive while `pages_fetched`
remains zero for a failed large page.

A run is authoritative only after at least one page and natural pagination
exhaustion. A `--max-pages` bound, stopped pagination, or an exception creates
a partial or failed non-authoritative run. Such a run records positive
observations but never infers removal from absence. Authoritative completion
classifies scope members as new, unchanged, corrected, restored, or removed,
then updates current trade activity and rebuilds affected recurrence inside
one transaction. If overlapping tracked scopes contain the same source row,
the row remains current while any one of those scopes still reports it.

The first completed or partial UK Trade Info run establishes one fixed
recurrence threshold for the database. Subsequent UK Trade Info commands are
rejected before ingestion if `--minimum-months` differs. A different threshold
requires a new database; an audited in-place rebaseline is not implemented.
This policy prevents configuration-only recurrence and change events.

The public named-trader data does not disclose trader-level monetary value,
quantity, partner/origin country, customer, VAT number, or EORI number.
Publication is incomplete because disclosure controls and opt-outs apply.
These absences never become China-sourcing evidence.

Published named-trader coverage starts in 2016 for non-EU trade, 2021 for
GB-to-EU exports, and 2022 for EU-to-GB imports. Northern Ireland trade with the
EU is excluded. Year-over-year comparisons must respect these start dates and
incomplete current-year periods.

Every implemented connector has timeouts, bounded retries, service-owned
pagination, raw preservation, structured state, offline fixtures, and an
optional live canary that is not required by the unit suite.

## Entity linking and human review

Source identities remain distinct. Legal-name normalization performs limited,
explainable canonicalization, including punctuation/case normalization and
known legal-suffix expansion; it does not delete legal suffixes or invent a
registration number. A normalized-name match is a candidate, not an implicit
merge.

Each proposal is an `entity_link_decisions` row with status, match type,
confidence, reasoning, timestamp, and evidence IDs. Ambiguous names, local
Find a Tender identifiers, scheme-less numbers, and withheld suppliers remain
unresolved. Importer, exporter, unresolved trader, procurement buyer, and
awarded supplier roles remain independently observable.

Candidate generation requires an exact normalized legal name. Automatic
acceptance additionally requires one unique procurement candidate plus a
matching strong typed identifier (`GB-COH`, `GB-PPON`, `GB-CHC`, or `GB-NHS`),
exact postcode, or exact normalized address. Identifier or location conflicts
are rejected; multiple candidates are ambiguous; a unique name without
corroboration remains unresolved. Even an accepted decision connects two
source identities without coalescing them.

The current review queue contains non-accepted link candidates whose trade and
procurement identities are both still current. Each row includes the candidate
identities and roles, source evidence, shared strong identifiers or postcodes,
observable identifier/postcode/address conflicts, confidence, and the reason
automatic linking was rejected. Human-owned `queue_status`, reviewer, notes,
and review time survive refreshes. Accepted or superseded candidates become
non-current; the machine-readable queue export filters them out while the
DuckDB history remains.

A human queue disposition is workflow metadata only. It does not merge
companies, change the automatic decision, or attach procurement evidence. That
requires a separate explicit accepted link decision; no queue-approval CLI is
implemented in this milestone.

## Meaningful-change contract

After each source command and `link`, orchestration reconciles Find a Tender
opportunities, re-evaluates links, projects material current facts, compares
them with the persistent baseline, refreshes the review queue, and then writes
exports. The change event inserts and baseline advance form one transaction.

The supported events are:

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

Canonical JSON, normalized currency and numeric forms, normalized time zones,
and order-insensitive sets prevent equivalent values from producing noise.
First-observed already closed or expired tenders are not announced as new.
The first detector invocation reports existing current qualifying facts as
new; subsequent unchanged runs emit no events. A deadline expiry or explicit
closure emits once.

CLI runs unrelated to UK Trade Info recurrence reuse the recurrence threshold
from the latest persisted `change_detection_runs` row. They do not silently
fall back to two months after a three-month authoritative run, which prevents
false `new_recurrent_trader` or `recurrence_threshold_crossed` events caused
only by detector configuration drift.

Missing Find a Tender records never retire tender baselines because its CLI
streams are incremental. Missing recurrence scopes are retired from the
detector baseline only when the detector receives a completed authoritative
UK Trade Info scope, and only within that exact parsed year, prefix, and role
filter. A partial or failed fetch cannot create negative change evidence.

## Selected-category commercial current view

`commercial-export` requires an activity year and exactly one selection family:
repeatable `--category` IDs or repeatable `--hs-prefix` values. It can use
repeatable explicit `--authoritative-run-id` values. If none are supplied, it
selects the latest completed authoritative UK Trade Info coverage compatible
with the requested year, prefixes, and `--trader-filter`, preferring the
narrowest compatible scope.

Explicit run IDs are validated against the requested trader filter before
materialization. An `All` run can cover a requested importer or exporter view;
an importer-only or exporter-only run cannot cover `All` or the opposite role.
When `All` supplies coverage for a specific role, materialization still filters
rows to the requested role.

The selected authoritative runs also supply the recurrence threshold used for
change detection and the view. It must match the fixed database policy; mixed
thresholds are rejected defensively. `commercial-export` does not expose an
override. Materialization stores the threshold in both the view rows and
persisted selection state, and excludes any row whose current active-month
count is below it.

Category selection expands to every candidate HS prefix configured for that
category and therefore requires completed coverage for each prefix. An `All`
scope covers importer, exporter, and unresolved roles; importer-only and
exporter-only scopes cover only their named role. The selected run IDs and
per-row coverage are retained in the view.

Materialization reads only current trade rows and current accepted entity
links to current Find a Tender payloads. Rows remain separate by activity year,
commodity code and disclosed level, and trader role. Each row contains:

- company and product/category identity;
- exact active months and recurrent-month count;
- importer, exporter, or unresolved role;
- current procurement roles, release facts, award and contract facts, source
  URLs, and evidence IDs where linked;
- accepted-link decision evidence and authoritative coverage;
- evidence-source count, confidence, validation status, and next action.

Rows with current procurement evidence rank first, followed by evidence-source
count, recurrent months, confidence, and deterministic company/code/role
tie-breakers. This is deliberately not an advanced score. Every taxonomy match
remains labelled as a candidate requiring review.

The table is one replaceable current selection. Running `commercial-export`
deletes the prior selection and materializes the requested one; it does not
retain a history of commercial selections. In the same transaction it persists
the selection, trader filter, taxonomy path, and authoritative run IDs in
`selected_category_commercial_view_state`. A complete `uk-trade-info` command
also materializes and persists its requested-prefix view when every requested
scope is authoritative.

After that state exists, `find-tender` and `link` automatically rebuild the
same commercial selection after refreshing procurement facts, links, changes,
and review candidates. They do not choose a new category or coverage scope. If
no selection state exists, they report the commercial view as not yet selected.

The acceptance question is answered directly from
`selected_category_commercial_view`:

```text
current recurrent trade scopes
  -> source-scoped canonical company
  -> current accepted entity-link decision
  -> current procurement party / release / award
  -> evidence -> immutable raw snapshots
```

`entity_review_queue` provides the companion list of plausible but rejected or
ambiguous links. “Current” describes validity in the reconciled local evidence
store. A 2025 activity row does not assert that a company trades today, and a
procurement identity link does not assert that the procurement subject matches
the traded product.

## Machine-readable exports

The exporter serializes existing database state; reconciliation and derived
view refreshes are orchestration responsibilities. Every table in the allowlist
is written to both CSV and Parquet:

- provenance and runs: `ingestion_runs`, `ingestion_run_scopes`,
  `ingestion_run_records`, `active_scope_leases`, `source_scope_membership`,
  and `source_records`;
- demand and procurement: `explicit_demand`, `procurement_releases`,
  `procurement_release_observations`, `release_transitions`,
  `procurement_awards`, `procurement_parties`, `opportunity_current_state`, and
  `opportunity_state_history`;
- trade and entities: `trade_activity`, `canonical_companies`,
  `company_source_roles`, `entity_link_decisions`, `entity_review_queue`,
  `recurrent_accounts`, and `recurrent_account_scopes`;
- commercial and analytical views:
  `selected_category_commercial_view_state`,
  `selected_category_commercial_view`, `country_category`, `evidence`,
  `signals`, and `opportunity_scores`;
- change audit: `material_change_state`, `change_detection_runs`, and
  `change_events`.

The file count is `2 * exported tables + 1` for `daily_report.md`. The review
queue export contains only current candidates; `change_events` contains event
history. The report contains only the latest change-detection run, the top ten
current commercial rows, the current review count, and interpretation limits.

## CLI orchestration

- `fixture`, `ted`, `find-tender`, and `uk-trade-info` ingest source records,
  then reconcile opportunities, evaluate links, detect changes, refresh the
  review queue, and export.
- `find-tender` and `link` also refresh the last persisted commercial
  selection, if one exists.
- `link` performs those operational steps without a source fetch.
- `commercial-export` performs those steps, verifies authoritative UK Trade
  Info coverage, replaces the selected commercial view, and exports.
- `init-db` creates both the core and operational export schemas.

The source commands accept common `--database`, `--raw-dir`, `--exports-dir`,
and `--taxonomy` paths. `link` needs only the database and export directory.

## Implementation milestones

1. **Foundation + TED vertical slice (complete):** models, schema, snapshots,
   TED ingestion, conservative scoring, exports, and fixture tests.
2. **Phase 2A, UK procurement evidence (complete):** Find a Tender release
   pagination, current tender derivation, award/party preservation, framework
   facts, and source-scoped identifiers.
3. **Phase 2B, named trade activity (complete):** UK Trade Info
   trader/product/month observations, explicit roles, recurrence, and
   disclosed-field limitations.
4. **Phase 2C, conservative linking (complete):** source-scoped companies and
   auditable link decisions without silent merges.
5. **Phase 3, operational reliability (complete):** completed-run current-state
   reconciliation, version and lifecycle history, material-only changes,
   entity-review queue, and authoritative selected-category commercial export.
6. **Recommended next milestone:** Eurostat Comext as the next major source,
   preserving the same evidence, snapshot, current-state, and change contracts.

Deferred work remains explicit: TED SPARQL, a frontend, an MCP server, paid
shipment databases, contact enrichment, and large-scale country expansion.

## Risks and interpretation boundary

- TED fields are flattened and can change shape or language cardinality. Raw
  snapshots and tolerant parsing limit data loss; validation warnings require
  review.
- A CPV code describes procurement subject matter; an HS/CN code describes a
  traded good. Crosswalks are probabilistic candidates. A company-level link
  between trade and procurement sources does not prove product-level alignment.
- Find a Tender release/version semantics differ from TED notice semantics.
  Release ID, OCID, package snapshot, row payload hash, and observation time
  remain distinct. Incremental absence is not a tombstone.
- Find a Tender party identifiers can be local, missing a scheme, or withheld.
  They cannot all be treated as Companies House identifiers.
- The UK Trade Info feed is a presence/activity dataset, not a shipment
  dataset. It cannot support trader-level value, quantity, origin, destination,
  customer, or China-share claims.
- Named-trader coverage differs by flow and period and is incomplete due to
  disclosure controls. Missing months do not prove inactivity. A removal is
  established only within an exact completed authoritative scope, and
  overlapping scopes deliberately use “any present wins.”
- A hard process termination can leave an active scope lease. This milestone
  has no automatic stale-lease expiry; recovery must be audited manually.
- A historic activity year is not a claim of present-day trading. An incomplete
  current year must be labelled YTD with a cutoff and not compared directly to
  a full year.
- The first change-detection pass establishes the baseline and reports current
  qualifying facts as new. Consumers that need only post-baseline alerts should
  seed and acknowledge that first run.
- A human review status does not apply a link, and no review-approval CLI is
  included. Ambiguous identities remain unmerged.
- The commercial current view replaces the prior selection and uses a simple
  evidence ordering, not a predictive score or selection history.
- Procurement buyer names are not commercial contacts. Procurement values may
  be estimates, awards, framework maxima, gross amounts, or contract values;
  they remain source-labelled and are not summed without an unambiguous basis
  and currency.

The system can identify companies with repeated public trader/product/month
observations and show whether an auditable link connects them to current public
procurement buyer or supplier evidence. It does not claim purchase volume,
commercial scale, China sourcing, contactability, supplier switchability,
product fit, or HS certainty. Every recommendation remains a validation action,
not an asserted sales opportunity.

## Primary references

- [TED Search API v3](https://docs.ted.europa.eu/api/latest/search.html)
- [TED Search pagination and iteration](https://docs.ted.europa.eu/ODS/latest/reuse/search-api.html)
- [TED official search-field list](https://docs.ted.europa.eu/ODS/latest/reuse/field-list.html)
- [OP-TED Open Data examples](https://github.com/OP-TED/ted-open-data-examples)
- [Find a Tender data and API documentation](https://www.find-tender.service.gov.uk/Developer/Documentation)
- [Find a Tender release-package API](https://www.find-tender.service.gov.uk/apidocumentation/1.0/GET-ocdsReleasePackages)
- [OCDS 1.1.5 release reference](https://standard.open-contracting.org/latest/en/schema/reference/)
- [OCDS framework guidance](https://standard.open-contracting.org/latest/en/guidance/map/framework_agreements/)
- [UK Trade Info API documentation](https://www.uktradeinfo.com/api-documentation/)
- [UK Trade Info named-trader guidance](https://www.uktradeinfo.com/find-uk-traders/how-to-use-our-tool-to-find-uk-traders/)
