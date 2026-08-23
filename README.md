# European B2B Demand Intelligence Harness

Private, provenance-first research tooling that turns public procurement and
trade observations into evidence-backed commercial validation queues.

The first vertical slice is implemented:

`TED Search API -> raw JSON snapshot -> normalized notice -> evidence -> opportunity -> CSV/Parquet/Markdown`

It deliberately does **not** call a company a buyer without procurement or
trade evidence, and it keeps observed facts separate from inferences.

## Quick start

```bash
python -m venv .venv
.venv/bin/pip install '.[dev]'
.venv/bin/demand-harness init-db
.venv/bin/demand-harness ted \
  --query 'publication-date >= 20240101 AND main-classification-proc = 44511000' \
  --scope ACTIVE --max-pages 1
```

Offline demonstration (uses the checked-in fixture and makes no network call):

```bash
.venv/bin/demand-harness fixture tests/fixtures/ted_search_page.json
```

Outputs are written under `data/`, which is ignored except for `.gitkeep`
files. See [docs/architecture.md](docs/architecture.md) for the schema,
connector plan, milestones, risks, and minimal first-PR scope.

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
