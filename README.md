# European B2B Demand Intelligence Harness

Private, provenance-first research tooling that turns public procurement and
trade observations into evidence-backed commercial validation queues. The
implemented source paths are:

```text
TED Search API ---------> explicit demand ----------------------+
Find a Tender OCDS -----> releases, awards and parties ---------+-> DuckDB -> CSV/Parquet/report
UK Trade Info OData ----> trader/product/month activity --------+
                                      |-> canonical companies -> explicit link decisions
```

It deliberately does **not** call a company a procurement buyer without
procurement evidence or an importer/exporter without trade evidence, and it
keeps observed facts separate from inferences.

## Quick start

```bash
python -m venv .venv
.venv/bin/pip install '.[dev]'
.venv/bin/demand-harness init-db
.venv/bin/demand-harness ted \
  --query 'publication-date >= 20240101 AND main-classification-proc = 44511000' \
  --scope ACTIVE --max-pages 1
```

Fetch a bounded Find a Tender canary containing both tender and award-stage
releases:

```bash
.venv/bin/demand-harness find-tender \
  --stage tender --stage award \
  --updated-from 2026-08-01T00:00:00 \
  --limit 100 --max-pages 1
```

Omitting `--stage` runs separate tender and award streams. `--updated-to` can
pin the other end of an incremental window, and `--notice-id 080005-2026`
retrieves a specific notice. The Find a Tender command also accepts
`--state-file` and the common `--database`, `--raw-dir`, `--exports-dir`, and
`--taxonomy` paths.

Fetch named trader/product/month observations for selected HS prefixes from UK
Trade Info. The one-page bound is useful for a live canary; remove it for a
complete run:

```bash
.venv/bin/demand-harness uk-trade-info \
  --hs-prefix 8203 --hs-prefix 8302 \
  --year 2025 --trader-filter All \
  --minimum-months 2 --max-pages 1
```

`--trader-filter` accepts `Importers`, `Exporters`, or `All`. Multiple
`--hs-prefix` options are processed independently.

Both `find-tender` and `uk-trade-info` evaluate conservative cross-source
links against the records already in the database before writing exports. To
re-evaluate links and regenerate exports without making a network request, run:

```bash
.venv/bin/demand-harness link \
  --database data/demand.duckdb \
  --exports-dir data/exports
```

Offline demonstration (uses the checked-in fixture and makes no network call):

```bash
.venv/bin/demand-harness fixture tests/fixtures/ted_search_page.json
```

Outputs are written under `data/`, which is ignored except for `.gitkeep`
files. Every exported table is emitted as CSV and Parquet. Phase 2 exports
include `procurement_releases`, `procurement_awards`, `procurement_parties`,
`trade_activity`, `canonical_companies`, `company_source_roles`,
`entity_link_decisions`, `recurrent_accounts`, and
`recurrent_account_scopes`, alongside the existing demand, evidence, signal,
aggregate, and score tables. A concise
`daily_report.md` is also generated. See
[docs/architecture.md](docs/architecture.md) for the detailed data contracts.

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
  volume estimate.
- Source snapshots are immutable, but UK Trade Info rows are currently
  append/upsert-only. A later complete publication that removes or suppresses
  an earlier row is not yet reconciled automatically.
- A link is accepted only for a unique exact normalized legal-name candidate
  corroborated by a matching strong typed identifier, exact postcode, or exact
  normalized address. Ambiguous, conflicting, and insufficiently corroborated
  candidates remain explicit non-links, superseded decisions are marked
  non-current, and source identities are not merged.
- Find a Tender awards remain award evidence. Only current, non-expired,
  taxonomy-matched active tenders enter explicit demand and opportunity
  scoring.
- CPV-to-HS mappings are candidate product-taxonomy mappings that require
  review. Exact source identifiers and explicit link decisions remain separate
  from normalized-name candidates.
