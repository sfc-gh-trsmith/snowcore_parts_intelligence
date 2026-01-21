# Unified Parts Intelligence Platform (UPIP)

GenAI-powered supply chain and engineering demo built on Snowflake. The platform demonstrates part deduplication, supplier consolidation, and compliance acceleration for a post-acquisition scenario where **Snowcore Industries** has acquired **BioFlux Automation**.

## Business Goals

| Goal | Target |
|------|--------|
| SKU Rationalization | 30% reduction via functional equivalence clustering |
| Procurement Efficiency | 15% cost savings via supplier consolidation |
| Compliance Velocity | 50% reduction in time-to-audit for FDA/ISO requirements |

## Features

- **Part Similarity Matching**: Vector embeddings with `EMBED_TEXT_768` and cosine similarity to identify duplicate/equivalent parts across legacy systems
- **Cortex Search (RAG)**: Search engineering docs, compliance certifications, and SOPs for regulatory requirements
- **Cortex Analyst**: Natural language queries against structured part, supplier, and procurement data
- **Cortex Agent**: "Sourcing Assistant" that orchestrates Analyst + Search + custom Python tools
- **Multi-Persona Streamlit App**: Role-based workflows for R&D Engineer, VP Supply Chain, and Procurement Manager

## Project Layout

```
├── sql/                        # Snowflake DDL and Cortex services
│   ├── 01_setup.sql            # Roles, warehouse, database, schemas
│   ├── 02_tables.sql           # Table definitions
│   ├── 03_cortex_search.sql    # ENGINEERING_DOCS_SEARCH service
│   ├── 04_semantic_view.sql    # Semantic view helpers
│   ├── 05_cortex_agent.sql     # Agent tool functions (UDFs)
│   ├── 06_synthetic_procedures.sql  # Data generation procedures
│   └── 07_cortex_agent.sql     # SOURCING_ASSISTANT agent
├── data/
│   ├── synthetic/              # CSV datasets (<10K rows)
│   └── documents/              # Compliance docs for Cortex Search
├── notebooks/                  # Snowpark ML notebook
│   └── part_similarity.ipynb   # Embeddings and similarity scoring
├── streamlit/                  # Streamlit in Snowflake app
│   ├── streamlit_app.py        # Landing page with KPIs
│   └── pages/
│       ├── 1_Part_Matcher.py       # R&D Engineer persona
│       ├── 2_Supply_Chain_Tower.py # VP Supply Chain persona
│       ├── 3_Procurement_Ops.py    # Procurement Manager persona
│       └── 4_About.py              # Architecture docs
├── semantic_views/             # Cortex Analyst semantic model
│   └── upip_semantic_model.yaml
├── agents/                     # Agent configuration reference
├── solution_presentation/      # Slides, diagrams, video script
├── deploy.sh                   # Full deployment script
├── run.sh                      # Runtime operations
└── clean.sh                    # Teardown script
```

## Prerequisites

- **Snowflake CLI** (`snow` command) - [Install Guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)
- A Snowflake connection profile (default: `demo`)

## Quick Start

### 1. Deploy Everything

```bash
./deploy.sh
```

This will:
- Create role, warehouse, database, and schemas
- Deploy all tables and Cortex services
- Generate synthetic data (20K parts)
- Load CSV datasets
- Create semantic view and Cortex agent
- Deploy Streamlit app and notebook

### 2. Verify Deployment

```bash
./run.sh test
```

### 3. Get Streamlit App URL

```bash
./run.sh streamlit
```

## Deployment Options

| Command | Description |
|---------|-------------|
| `./deploy.sh` | Full deployment |
| `./deploy.sh --only-sql` | Deploy SQL objects only |
| `./deploy.sh --only-data` | Generate and load data only |
| `./deploy.sh --only-streamlit` | Deploy Streamlit app only |
| `./deploy.sh --only-notebook` | Deploy notebook only |
| `./deploy.sh -c <name>` | Use a different connection profile |
| `./deploy.sh -p <prefix>` | Add environment prefix (e.g., `DEV`) |

## Runtime Commands

| Command | Description |
|---------|-------------|
| `./run.sh main` | Execute similarity notebook |
| `./run.sh test` | Run validation queries |
| `./run.sh status` | Show deployment status |
| `./run.sh streamlit` | Get Streamlit app URL |

## Cleanup

```bash
./clean.sh
```

Drops the warehouse, database, and role. Use `--force` to skip confirmation.

## Architecture

### Data Flow

```
PLM Exports (Windchill, SolidWorks PDM)
    ↓
RAW Schema (staging)
    ↓
ATOMIC Schema (normalized part/supplier master)
    ↓
DATA_SCIENCE Schema
    ├── PART_SIMILARITY_SCORES (ML output)
    ├── PARTS_ANALYTICS (unified view)
    ├── ENGINEERING_DOCS_SEARCH (Cortex Search)
    ├── UPIP_SEMANTIC_MODEL (Cortex Analyst)
    └── SOURCING_ASSISTANT (Cortex Agent)
```

### Cortex Agent Tools

The **SOURCING_ASSISTANT** agent provides:

1. **UPIP_ANALYTICS** - Text-to-SQL via Cortex Analyst semantic view
2. **ENGINEERING_DOCS** - RAG search over compliance documents
3. **CALCULATE_RETOOLING_COST** - Estimate supplier switching costs
4. **ASSESS_SUPPLIER_RISK** - Get composite risk scores for suppliers
5. **GET_CONSOLIDATION_SCENARIO** - Retrieve consolidation scenario details

## Sample Queries

**Cortex Analyst (via Agent)**:
> "Show me the total inventory value of stainless steel parts that are duplicates, grouped by business unit"

**Cortex Search (via Agent)**:
> "What are the biocompatibility testing requirements for actuators per FDA 21 CFR Part 11?"

**Direct SQL**:
```sql
SELECT BUSINESS_UNIT, SUM(INVENTORY_VALUE) AS TOTAL_INV_VALUE 
FROM DATA_SCIENCE.PARTS_ANALYTICS 
WHERE IS_DUPLICATE = TRUE AND MATERIAL = 'Stainless Steel' 
GROUP BY BUSINESS_UNIT;
```

## Notes

- Synthetic datasets >10K rows are generated via stored procedures
- Smaller datasets are stored as CSVs under `data/synthetic/`
- The semantic model supports all three personas with role-specific measures and dimensions
- See `DRD.md` for full requirements and `solution_presentation/` for architecture diagrams
