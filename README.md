# Unified Parts Intelligence Platform (UPIP)

GenAI-powered supply chain and engineering demo built on Snowflake. The demo focuses on part deduplication, supplier consolidation, and compliance acceleration across mixed-acquisition portfolios.

## Requirements Checklist (from `DRD.md`)
- Identify duplicate parts across legacy Windchill and BioFlux PDM sources with >90% relevance in <2s.
- Reduce active part count by ~30% through functional equivalence clustering.
- Show 15% cost savings potential from supplier consolidation.
- Provide FDA/ISO compliance traceability and reduce audit time by 50%.
- Deliver two persona flows:
  - Engineer: part matcher and similarity results with reuse actions.
  - Supply chain leader: high-level KPIs and savings opportunities.
- Support Cortex Analyst (structured KPIs) and Cortex Search (RAG over compliance docs).
- Provide a Cortex Agent (“Sourcing Assistant”) that orchestrates Analyst + Search + cost tool.

## Project Layout
- `sql/` Snowflake DDL and Cortex service definitions.
- `data/synthetic/` Small CSV datasets (<=10K rows).
- `data/documents/` Unstructured documents for Cortex Search.
- `notebooks/` Snowpark ML notebooks for embeddings and similarity scoring.
- `streamlit/` Streamlit in Snowflake application.
- `semantic_views/` Semantic model YAML for Cortex Analyst.
- `agents/` Cortex Agent configuration assets.
- `solution_presentation/` Executive summary and diagram assets.

## Quick Start
1. Deploy core objects and load data:
   - `./deploy.sh`
2. Deploy Streamlit:
   - `./deploy.sh --only-streamlit`
3. Run post-setup steps (tests, status, notebook triggers):
   - `./run.sh test`

## Notes
- Synthetic data sets >10K rows are generated via Snowflake stored procedures.
- Smaller datasets are stored as local CSVs under `data/synthetic/`.
# snowcore_parts_intelligence
