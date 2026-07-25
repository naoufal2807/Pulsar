<div align="center">
  <img src="logo.svg" alt="Pulsar Logo" width="120" height="120">
</div>

# Pulsar ⚡

**Dataset onboarding intelligence. One command to a quality verdict.**

---

## What It Does

Drop any CSV or Parquet file on Pulsar and get a schema audit, null/quality verdict, and a plain-English narrative — all in under 60 seconds.

```bash
pulsar scout data.csv
```

Four specialized AI agents run in parallel:

- **SchemaAgent** — column types, cardinality, nullability
- **QualityAgent** — null rates, issues, CLEAN / WARN / BLOCK verdict
- **StatsAgent** — distributions, outliers, skewness
- **NarratorAgent** — synthesizes everything into an actionable report

---

## Install

```bash
# From PyPI (once published)
pip install pulsar-dq

# Or from source
git clone https://github.com/naoufal2807/Pulsar
cd Pulsar
pip install -e .
```

Ollama is required for local inference. Install from [ollama.com](https://ollama.com), then pull a model:

```bash
ollama pull llama3
```

Optional cloud LLM providers:

```bash
pip install pulsar-dq[openai]      # OpenAI
pip install pulsar-dq[anthropic]   # Anthropic Claude
pip install pulsar-dq[google]      # Google Gemini
pip install pulsar-dq[all]         # All providers
```

---

## Usage

```bash
# Quick scout: schema + quality + narrative (<60s)
pulsar scout data.csv

# Full analysis: scout + statistics
pulsar scout data.csv --mode full

# Use a specific model
pulsar scout data.csv --model llama3

# Analyze a Parquet file
pulsar scout warehouse.parquet
```

---

## Example Output

```
┌─────────────────────────────────────────────────────┐
│  Pulsar Scout — netflix_titles.csv                  │
│  8,807 rows · 12 columns                            │
└─────────────────────────────────────────────────────┘

Quality Verdict: WARN
  director   → 29.9% null
  cast       → 9.8% null
  country    → 6.9% null

Schema
  show_id    String   8,807 unique
  type       String   2 categories (Movie / TV Show)
  title      String   8,803 unique
  release_year Int64  1925–2021
  rating     String   14 categories

Narrative
  This is a media content catalog for a streaming platform covering
  8,807 titles across Movies and TV Shows. The dataset is usable but
  has director and cast gaps — likely due to missing metadata at
  ingest time, not data corruption. Safe to proceed with content
  analysis; enrich director/cast from a secondary source if needed.
```

---

## Architecture

Pulsar uses **Approach B**: four domain-specialized agents with exclusive tool sets and a typed shared state store.

```
pulsar scout data.csv
        │
        ├─── SchemaAgent ────┐
        │    (types, cardin.)│
        │                    ▼
        ├─── QualityAgent ──► SharedStateStore ──► NarratorAgent
        │    (nulls, verdict)│                     (synthesis)
        │                    │
        └─── StatsAgent ─────┘ (full mode only)
             (distributions)
```

SharedStateStore typed keys written by agents:
- `schema.columns`, `schema.types`, `schema.cardinality`
- `quality.verdict` (CLEAN / WARN / BLOCK), `quality.null_report`, `quality.issues`

---

## Requirements

- Python 3.10+
- Ollama (for local models) or an API key for OpenAI / Anthropic / Google
- Polars, Typer, Rich (installed automatically)

---

## License

MIT. Use it, fork it, modify it.

---

**Make data understand itself. Pulsar ⚡**
