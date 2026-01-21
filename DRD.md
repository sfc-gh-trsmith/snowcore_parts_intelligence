# Demo Requirements Document (DRD): Unified Parts Intelligence Platform (UPIP)

GITHUB REPO NAME: `snowcore_parts_intelligence`
GITHUB REPO DESCRIPTION: A GenAI-powered supply chain and engineering platform utilizing Snowpark ML for part rationalization and Cortex for regulatory compliance across mixed-acquisition portfolios.

## 1. Strategic Overview

* **Problem Statement:** Following the recent acquisition of **BioFlux Automation**, **Snowcore Industries** faces fragmented product data across disconnected PLM systems (Legacy Windchill vs. BioFlux's SolidWorks PDM). This has resulted in severe "part proliferation" (duplicate inventory), inefficient procurement due to split volume, and heightened regulatory risks as the company integrates high-precision life sciences assets into its industrial portfolio.
* **Target Business Goals (KPIs):**
* **SKU Rationalization:** Reduce total active part count by 30% through identifying functional equivalents.
* **Procurement Efficiency:** Achieve 15% cost savings by consolidating suppliers for common components across the Industrial and Bio-Tech divisions.
* **Compliance Velocity:** Reduce time-to-audit for FDA/ISO requirements by 50% via automated traceability.


* **The "Wow" Moment:** An engineer uploads a spec sheet for a "new" high-precision valve. The system analyzes the geometry and specs using Vector Search, instantly retrieving three identical valves already available in the legacy Snowcore inventory, flagging one that is already FDA-compliant, preventing the creation of a duplicate SKU.

## 2. User Personas & Stories

| Persona Level | Role Title | Key User Story (Demo Flow) |
| --- | --- | --- |
| **Strategic** | **VP of Global Supply Chain** | "As a VP, I want to see the total potential savings from supplier consolidation across all business units (Heavy Industry & Bio-Automation)." |
| **Operational** | **Procurement Manager** | "As a Manager, I want to identify which 'BioFlux' parts are actually standard industrial parts marked up by niche vendors." |
| **Technical** | **R&D Engineer** | "As an Engineer, I want to find an existing motor assembly that fits my new design constraints without searching through five different PLM systems." |

## 3. Data Architecture & Snowpark ML (Backend)

* **Structured Data (Inferred Schema):**
* `RAW.PLM_EXPORTS`: Raw dumps from Windchill and SolidWorks PDM.
* `ATOMIC.PART_MASTER`: Normalized table uniting disparate part numbers. Columns: `GLOBAL_ID`, `LOCAL_ID`, `SOURCE_SYSTEM`, `MATERIAL`, `WEIGHT`, `DIMENSIONS_JSON`, `COST`, `SUPPLIER_ID`.
* `ATOMIC.SUPPLIER_MASTER`: Vendor performance and aggregate spend data.


* **Unstructured Data (Tribal Knowledge):**
* **Source Material:** PDF Engineering Drawings, CAD Metadata files, BioFlux FDA Compliance Certifications (21 CFR Part 11 logs), Standard Operating Procedures (SOPs).
* **Purpose:** Used to index technical specifications and regulatory requirements for Cortex Search.


* **ML Notebook Specification (Snowpark ML):**
* **Objective:** Intelligent Parts Clustering (Deduplication).
* **Technique:** Generate Vector Embeddings on concatenated text descriptions and numeric specs using `snowflake.cortex.EMBED_TEXT_768`.
* **Algorithm:** Approximate Nearest Neighbor (ANN) / Cosine Similarity to group parts.
* **Inference Output:** `DATA_SCIENCE.PART_SIMILARITY_SCORES` (Source Part A <-> Target Part B <-> Similarity Score %).



## 4. Cortex Intelligence Specifications

### Cortex Analyst (Structured Data / SQL)

* **Semantic Model Scope:**
* **Measures:** `Total_Spend`, `Inventory_Quantity`, `Unit_Cost`, `Supplier_Lead_Time`.
* **Dimensions:** `Business_Unit` (Industrial vs. Bio-Tech), `Material_Type`, `Supplier_Region`, `Compliance_Status` (FDA Approved).


* **Golden Query (Verification):**
* *User Prompt:* "Show me the total inventory value of stainless steel fasteners across BioFlux and Industrial units that are duplicates."
* *Expected SQL Operation:* `SELECT SUM(Inventory_Value) FROM Part_Master WHERE Is_Duplicate = TRUE AND Material = 'Stainless Steel' GROUP BY Business_Unit`



### Cortex Search (Unstructured Data / RAG)

* **Service Name:** `ENGINEERING_DOCS_SEARCH`
* **Indexing Strategy:**
* **Document Attribute:** Indexing by `Part_Family` and `Regulatory_Standard` (e.g., ISO 13485).


* **Sample RAG Prompt:** "What represent the biocompatibility testing requirements for this specific actuator based on the BioFlux compliance documents?"

### Cortex Agents (Orchestration)

* **Role:** The "Sourcing Assistant" Agent.
* **Tools:**
1. **Analyst Tool:** To check current stock levels and price.
2. **Search Tool:** To check technical feasibility and compliance.
3. **Custom Tool (Python):** `calculate_retooling_cost()` to estimate the cost of switching suppliers.



## 5. Streamlit Application UX/UI

* **Layout Strategy:**
* **Page 1 (The "Part Matcher"):** Search interface for Engineers. Input text or upload a file; output is a grid of "Similar Existing Parts" with confidence scores and "Select for Reuse" buttons.
* **Page 2 (Supply Chain Tower):** High-level dashboards showing consolidation progress, duplicate reduction metrics, and "High Spend / Low Variety" opportunities.


* **Component Logic:**
* **Visualizations:** Plotly Scatterplot (t-SNE visualization) showing clusters of similar parts.
* **Chat Integration:** A sidebar assistant where users can ask, "Why was this part flagged as non-compliant?" (Calls Cortex Search) or "How many do we have in stock?" (Calls Cortex Analyst).



## 6. Success Criteria

* **Technical Validator:** The vector search returns functionally similar parts (e.g., matching a "Hex Screw" to a "Screw, Hexagonal") with >90% relevance in under 2 seconds.
* **Business Validator:** The solution identifies at least 15% of the "BioFlux" inventory as duplicative of existing "Industrial" inventory, demonstrating immediate ROI.
