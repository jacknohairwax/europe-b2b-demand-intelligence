# Demand intelligence architecture and delivery plan

## Proposed architecture

The harness is a small, batch-oriented Python application. Connectors retrieve
and snapshot source payloads; normalizers convert source-specific payloads into
shared observations; pipelines attach evidence, persist deterministic records,
and derive conservative demand or recurrence views. No connector writes a
commercial conclusion directly.

```text
TED ---------------------> notice ---------> explicit demand ----+
Find a Tender -----------> release --------> tender/award facts --+-> DuckDB -> exports
UK Trade Info -----------> trader month ---> recurrent activity --+
                                                   |
source companies -> normalized-name candidates -> explicit link decisions
```

Every source payload is retained in an immutable snapshot. Stable row-level
payload hashes distinguish a repeated observation from a changed release even
when it appears in a different page snapshot. The product taxonomy remains
external JSON configuration. CPV-to-HS relationships are candidate mappings,
not an authoritative crosswalk.

## Database schema

Procurement observations are separated from derived opportunities:

- `procurement_releases` stores each Find a Tender release version, its OCID,
  stages, buyer, CPV codes, framework facts, raw release, payload hash, snapshot,
  and taxonomy result.
- `procurement_awards` stores every award independently, including its exact
  status, suppliers, award value, and linked contract value. Values with
  different bases are never silently combined.
- `procurement_parties` retains source party roles, identifiers, locations, and
  raw party payloads. A party identifier is not assumed to be global or typed
  unless the source supplies its scheme.
- `explicit_demand` contains active opportunities derived from TED or Find a
  Tender. Award-only records do not enter this table.

Trade and company observations are stored separately:

- `trade_activity` stores one UK Trade Info trader, disclosed commodity code
  and code level, month, and role tuple. Undisclosed value, quantity, and
  partner-country fields remain null.
- `canonical_companies` stores source-scoped company identities and observed
  names, identifiers, addresses, and source location labels. A stored country
  or UK address is not evidence of legal incorporation in that country.
- `company_source_roles` records the source record and observed role behind
  each company.
- `entity_link_decisions` records candidate-link status, method, confidence,
  reasoning, evidence, and whether the decision is current. Superseded
  decisions remain auditable but cannot keep procurement enrichment active.
  It is the audit boundary that prevents silent entity merges.
- `recurrent_account_scopes` records each qualifying calendar-year,
  commodity-code, disclosure-level, and trader-role group with its exact months
  and evidence.
- `recurrent_accounts` is the company-level rollup across qualifying scopes,
  with procurement evidence only when a supported link exists.

The existing `country_category`, `evidence`, `signals`, and
`opportunity_scores` tables remain. `source_records` and `ingestion_runs`
provide ingestion provenance and operational state. Arrays and structured
objects are JSON columns in DuckDB and are retained in CSV and Parquet exports.

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
streams in one invocation. The connector follows the service-owned
`links.next` URL verbatim, bounds retries, honors `Retry-After`, snapshots every
package, and preserves original releases rather than treating a compiled
process as the audit source. A specific notice can be fetched by notice ID.

A release is eligible for derived explicit demand only when it is a current,
non-expired active tender and matches the configured product taxonomy. Awards
and contracts remain procurement evidence and are not scored as open
opportunities. The current-view reconciliation run before export expires old
deadlines and retracts an earlier tender only when a later release for the same
OCID explicitly observes closure. Multiple awards, suppliers, lots, monetary
bases, and observed periods remain separate.

Framework establishment is observed from
`tender.techniques.hasFrameworkAgreement`; a call-off is observed from a
`relatedProcesses` relationship of `framework`. Tender, lot, award, and
contract periods retain their source context rather than being collapsed into
an inferred framework duration.

### UK Trade Info

The connector uses the anonymous HMRC OData `GET /Trade` endpoint. It filters
by year, CN8/HS prefix, and optional trader role, selects the public activity
keys, and expands the published trader, commodity, and trade-type records. It
leaves page sizing to the service and follows `@odata.nextLink` verbatim. An
optional `$top` is treated as a total-result bound rather than a page-size
setting. `TradeTypeId` 1 is recorded as importer, 2 as exporter, and any other
value remains unresolved.

The stable activity key is trader, commodity, month, and trade type. Recurrence
requires the configured number of distinct months within the same calendar
year, disclosed commodity code and code level, and trade role. Aggregate HS2,
HS4, or HS6 disclosures are retained at their actual level rather than padded
into an invented CN8 code. The public named-trader data does not disclose
trader-level monetary value, quantity, partner/origin country, customer, VAT
number, or EORI number. Publication is also incomplete because disclosure
controls and opt-outs apply. These absences remain explicit limitations and
never become China-sourcing evidence.

Published named-trader coverage starts in 2016 for non-EU trade, 2021 for
GB-to-EU exports, and 2022 for EU-to-GB imports. Northern Ireland trade with the
EU is excluded. Year-over-year comparisons must respect these different start
dates as well as incomplete current-year periods.

Every implemented connector has timeouts, bounded retries, service-owned
pagination, raw preservation, structured state, offline fixtures, and an
optional live canary that is not required for the unit suite.

## Entity and linking contract

Source identities remain distinct. Legal-name normalization performs limited,
explainable canonicalization, including punctuation/case normalization and
known legal-suffix expansion; it does not delete legal suffixes or invent a
registration number. A normalized-name match is a candidate, not an implicit
merge.

Each cross-source proposal is represented by an `entity_link_decisions` row
with an explicit status, match type, confidence, reasoning, timestamp, and
evidence IDs. Ambiguous names, local Find a Tender identifiers, scheme-less
numbers, and withheld suppliers must remain unresolved. Procurement evidence
can be attached to a recurrent account only through an accepted, auditable
link. Importer, exporter, unresolved trader, procurement buyer, and awarded
supplier roles remain independently observable.

Candidate generation requires an exact normalized legal name. Automatic
acceptance additionally requires a unique procurement candidate plus a matching
strong typed identifier (`GB-COH`, `GB-PPON`, `GB-CHC`, or `GB-NHS`), exact
normalized postcode, or exact normalized address. Identifier or location
conflicts are rejected; multiple candidates are ambiguous; a unique name
without corroboration remains unresolved. Even an accepted decision connects
two source identities without coalescing them.

Both the `find-tender` and `uk-trade-info` ingestion commands evaluate links
against all records currently in DuckDB immediately before export. The offline
command below re-evaluates the same decisions and refreshes exports without
fetching either source:

```bash
demand-harness link --database data/demand.duckdb --exports-dir data/exports
```

This contract supports the milestone question as a traceable join:

```text
recurrent_accounts
  -> canonical_companies / company_source_roles
  -> entity_link_decisions
  -> procurement_parties / procurement_releases / procurement_awards
  -> evidence -> raw snapshots
```

## Machine-readable exports

Every table in the export allowlist is written to both CSV and Parquet. The
allowlist includes procurement releases, awards, and parties; trade activity;
canonical companies and source roles; entity-link decisions; recurrent
accounts and their exact recurrence scopes; explicit demand; evidence; signals;
country/category aggregates; and opportunity scores. The file count is
therefore `2 * exported tables + 1` for the Markdown daily report.

## Implementation milestones

1. **Foundation + TED vertical slice (complete):** models, schema, snapshots,
   TED ingestion, conservative scoring, exports, and fixture tests.
2. **Phase 2A, UK procurement evidence:** Find a Tender release pagination,
   active tender derivation, award/party preservation, framework facts, and
   source-scoped identifiers.
3. **Phase 2B, named trade activity:** UK Trade Info trader/product/month
   observations, explicit importer/exporter roles, recurrence, and disclosed-
   field limitations.
4. **Phase 2C, conservative linking:** source-scoped companies and auditable
   link decisions between trade and procurement identities without silent
   merges.
5. **Next operations milestone:** incremental scheduling, material-change
   comparison, link-review workflow, contradiction reporting, and monitored
   live canaries.

## Risks and unknowns

- TED fields are flattened and can change shape or language cardinality. Raw
  snapshots and tolerant parsing limit data loss; validation warnings must be
  reviewed rather than suppressed.
- A CPV code describes procurement subject matter; an HS/CN code describes a
  traded good. Crosswalks are probabilistic and require evidence and review.
- Find a Tender release/version semantics differ from TED notice semantics.
  Release ID, OCID, package snapshot, and row payload hash must all be retained.
- Find a Tender party identifiers can be local, missing a scheme, or deliberately
  withheld. They cannot all be treated as Companies House identifiers.
- The named UK Trade Info feed is a presence/activity dataset, not a shipment
  dataset. It cannot support trader-level value, quantity, origin, destination,
  customer, or China-share claims.
- Named-trader coverage differs by flow and period and is incomplete due to
  disclosure controls. Missing months do not prove inactivity.
- UK Trade Info ingestion currently appends or upserts observations. Immutable
  source snapshots preserve history, but a row removed or suppressed in a
  later complete publication is not yet tombstoned and can remain in the
  current recurrence view. Safe reconciliation requires completed-run scope
  membership and must not run after a bounded or failed fetch.
- 2026 is incomplete. It must be labelled YTD with an explicit cutoff date and
  never compared to a full year without qualification.
- Procurement buyer names are not automatically commercial buyer contacts.
- Procurement monetary values may be tender estimates, award values, framework
  maxima, gross amounts, or linked contract values. They remain source-labelled
  and are not summed unless scope and currency are unambiguous.

## Phase 2 interpretation boundary

The system can identify companies with repeated public trader/product/month
observations and show whether an auditable link connects those companies to
public procurement buyer or supplier evidence. It does not claim purchase
volume, commercial scale, China sourcing, contactability, supplier
switchability, or HS certainty. Recommendations remain `Needs Further
Validation` until the existing evidence gate is satisfied.

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
