"""
UPIP About Page

Comprehensive documentation for both business and technical audiences
following the Snowflake Streamlit About Section Guide.
"""

import streamlit as st

st.set_page_config(
    page_title="About UPIP",
    page_icon=":material/info:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def render_data_card(name: str, badge: str, description: str, badge_color: str = "blue"):
    """Render a data source card with badge."""
    color_map = {
        "blue": "#3b82f6",
        "orange": "#f59e0b",
        "green": "#22c55e",
        "purple": "#8b5cf6",
    }
    bg_color = color_map.get(badge_color, "#3b82f6")
    st.markdown(
        f"""
        <div style="padding: 12px; border: 1px solid #334155; border-radius: 8px; margin-bottom: 8px;">
            <span style="background-color: {bg_color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px;">{badge}</span>
            <br><strong>{name}</strong>
            <p style="color: #94a3b8; font-size: 14px; margin: 4px 0 0 0;">{description}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main():
    # ============================================
    # 1. HEADER
    # ============================================
    st.title("About UPIP")
    st.markdown(
        "*AI-powered part rationalization and regulatory compliance for post-acquisition integration*"
    )
    st.caption("Snowcore Industries | BioFlux Automation Integration")

    st.divider()

    # ============================================
    # 2. OVERVIEW (Problem + Solution)
    # ============================================
    st.header("Overview")

    col_problem, col_solution = st.columns([2, 1])

    with col_problem:
        st.subheader("The Problem")
        st.markdown(
            """
            Following the acquisition of **BioFlux Automation**, Snowcore Industries faces 
            fragmented product data across disconnected PLM systems:
            
            - **Legacy Windchill** (Industrial division) vs. **SolidWorks PDM** (BioFlux)
            - No common part identifiers across systems
            - Duplicate parts ordered under different SKUs
            - Split procurement volume preventing supplier leverage
            - Regulatory risk integrating FDA-regulated life sciences assets
            
            **The cost of inaction:** Engineers unknowingly create duplicate SKUs, procurement 
            pays premium prices to niche vendors for standard parts, and compliance audits 
            take weeks instead of hours.
            """
        )

    with col_solution:
        st.subheader("The Solution")
        st.markdown(
            """
            UPIP uses **AI-powered similarity matching** to:
            
            - Find existing parts that match new requirements
            - Flag FDA-compliant options automatically
            - Identify vendor markup opportunities
            - Quantify consolidation savings
            
            **Result:** Prevent duplicate SKUs before they're created.
            """
        )

    st.divider()

    # ============================================
    # 3. BUSINESS IMPACT
    # ============================================
    st.header("Business Impact")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "SKU Rationalization",
        "30%",
        "Target reduction via functional equivalents",
        delta_color="normal",
    )
    col2.metric(
        "Procurement Savings",
        "15%",
        "Supplier consolidation across divisions",
        delta_color="normal",
    )
    col3.metric(
        "Compliance Velocity",
        "50%",
        "Faster FDA/ISO audit turnaround",
        delta_color="normal",
    )

    st.divider()

    # ============================================
    # 4. DATA ARCHITECTURE
    # ============================================
    st.header("Data Architecture")

    col_internal, col_external, col_outputs = st.columns(3)

    with col_internal:
        st.markdown("#### Internal Data")
        render_data_card(
            "RAW.PLM_EXPORTS",
            "ERP",
            "Raw dumps from Windchill and SolidWorks PDM systems",
            "blue",
        )
        render_data_card(
            "ATOMIC.PART_MASTER",
            "ERP",
            "Unified part catalog with GLOBAL_ID, material, dimensions, cost",
            "blue",
        )
        render_data_card(
            "ATOMIC.SUPPLIER_MASTER",
            "ERP",
            "Vendor performance, lead times, and aggregate spend data",
            "blue",
        )

    with col_external:
        st.markdown("#### Unstructured / External")
        render_data_card(
            "RAW.ENGINEERING_DOCS",
            "DOCS",
            "PDF engineering drawings, CAD metadata files",
            "orange",
        )
        render_data_card(
            "FDA Certifications",
            "COMPLIANCE",
            "21 CFR Part 11 logs, ISO 13485 documentation",
            "orange",
        )
        render_data_card(
            "SOPs",
            "DOCS",
            "Standard Operating Procedures for regulated processes",
            "orange",
        )

    with col_outputs:
        st.markdown("#### Model Outputs")
        render_data_card(
            "DATA_SCIENCE.PART_SIMILARITY_SCORES",
            "ML",
            "Source-to-target similarity scores from vector embeddings",
            "green",
        )
        render_data_card(
            "DATA_SCIENCE.PARTS_ANALYTICS",
            "ANALYTICS",
            "Enriched view with duplicate flags, compliance status, spend",
            "green",
        )
        render_data_card(
            "ENGINEERING_DOCS_SEARCH",
            "CORTEX",
            "Cortex Search service for document RAG queries",
            "purple",
        )

    st.divider()

    # ============================================
    # 5. HOW IT WORKS (Tabbed)
    # ============================================
    st.header("How It Works")

    exec_tab, tech_tab = st.tabs(["Executive Overview", "Technical Deep-Dive"])

    with exec_tab:
        st.markdown(
            """
            ### Why Traditional Approaches Fall Short
            """
        )

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**Manual Spreadsheet Matching**")
            st.markdown(
                """
                - Requires exact text matches
                - Misses "Hex Screw" = "Screw, Hexagonal"
                - Labor-intensive, error-prone
                - Outdated by completion time
                """
            )

        with col2:
            st.markdown("**Basic Database Joins**")
            st.markdown(
                """
                - Requires common identifiers
                - Windchill and PDM have none
                - Can't handle description variations
                - No fuzzy matching capability
                """
            )

        with col3:
            st.markdown("**UPIP AI Approach**")
            st.markdown(
                """
                - Understands meaning, not just text
                - Matches "M6x20 bolt" to "6mm hex fastener"
                - Learns from engineering context
                - Continuously improves with data
                """
            )

        st.markdown("---")

        st.markdown(
            """
            ### The "Wow" Moment
            
            > An engineer uploads a spec sheet for a "new" high-precision valve. The system 
            > analyzes the geometry and specs using AI, instantly retrieving three identical 
            > valves already available in the legacy Snowcore inventory—**flagging one that is 
            > already FDA-compliant**—preventing the creation of a duplicate SKU.
            
            ### Business Value
            
            | Capability | Benefit |
            |------------|---------|
            | **Instant Part Matching** | Engineers find existing parts in seconds, not days |
            | **Compliance Flagging** | FDA-approved parts highlighted automatically |
            | **Vendor Markup Detection** | Identify BioFlux parts sold at premium vs. Industrial equivalents |
            | **Savings Quantification** | Real-time ROI tracking for consolidation efforts |
            """
        )

    with tech_tab:
        st.markdown("### ML Pipeline Architecture")

        # Embedded SVG with Snowflake dark theme
        st.markdown(
            """
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 920 180" width="100%" height="180">
              <defs>
                <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
                  <polygon points="0 0, 10 3.5, 0 7" fill="#FF9F36"/>
                </marker>
                <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
                  <feDropShadow dx="2" dy="2" stdDeviation="3" flood-color="#000000" flood-opacity="0.5"/>
                </filter>
              </defs>
              <g filter="url(#shadow)">
                <rect x="10" y="20" width="190" height="140" rx="8" ry="8" fill="#24323D" stroke="#29B5E8" stroke-width="2"/>
                <text x="105" y="52" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="16" font-weight="bold" fill="#29B5E8">1. Ingest</text>
                <line x1="30" y1="62" x2="180" y2="62" stroke="#29B5E8" stroke-width="1" opacity="0.3"/>
                <text x="105" y="88" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">Load PLM data</text>
                <text x="105" y="108" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">from Windchill</text>
                <text x="105" y="128" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">and PDM</text>
              </g>
              <line x1="210" y1="90" x2="238" y2="90" stroke="#FF9F36" stroke-width="3" marker-end="url(#arrowhead)"/>
              <g filter="url(#shadow)">
                <rect x="250" y="20" width="190" height="140" rx="8" ry="8" fill="#24323D" stroke="#29B5E8" stroke-width="2"/>
                <text x="345" y="52" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="16" font-weight="bold" fill="#29B5E8">2. Embed</text>
                <line x1="270" y1="62" x2="420" y2="62" stroke="#29B5E8" stroke-width="1" opacity="0.3"/>
                <text x="345" y="88" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">Generate 768-d</text>
                <text x="345" y="108" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">vectors via</text>
                <text x="345" y="128" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">Cortex EMBED</text>
              </g>
              <line x1="450" y1="90" x2="478" y2="90" stroke="#FF9F36" stroke-width="3" marker-end="url(#arrowhead)"/>
              <g filter="url(#shadow)">
                <rect x="490" y="20" width="190" height="140" rx="8" ry="8" fill="#24323D" stroke="#29B5E8" stroke-width="2"/>
                <text x="585" y="52" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="16" font-weight="bold" fill="#29B5E8">3. Match</text>
                <line x1="510" y1="62" x2="660" y2="62" stroke="#29B5E8" stroke-width="1" opacity="0.3"/>
                <text x="585" y="88" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">ANN similarity</text>
                <text x="585" y="108" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">search across</text>
                <text x="585" y="128" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">all parts</text>
              </g>
              <line x1="690" y1="90" x2="718" y2="90" stroke="#FF9F36" stroke-width="3" marker-end="url(#arrowhead)"/>
              <g filter="url(#shadow)">
                <rect x="730" y="20" width="180" height="140" rx="8" ry="8" fill="#24323D" stroke="#29B5E8" stroke-width="2"/>
                <text x="820" y="52" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="16" font-weight="bold" fill="#29B5E8">4. Serve</text>
                <line x1="750" y1="62" x2="890" y2="62" stroke="#29B5E8" stroke-width="1" opacity="0.3"/>
                <text x="820" y="88" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">Write scores</text>
                <text x="820" y="108" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">to analytics</text>
                <text x="820" y="128" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="12" fill="#FFFFFF">tables</text>
              </g>
            </svg>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            """
            ### Vector Embedding Approach
            
            **Model:** `snowflake.cortex.EMBED_TEXT_768`
            
            **Input:** Concatenated part attributes:
            ```sql
            CONCAT(PART_NAME, ' | ', MATERIAL, ' | ', COALESCE(PART_DESCRIPTION, ''))
            ```
            
            **Algorithm:** Approximate Nearest Neighbor (ANN) with cosine similarity
            
            **Output:** `DATA_SCIENCE.PART_SIMILARITY_SCORES`
            - `SOURCE_GLOBAL_ID` → `TARGET_GLOBAL_ID` → `SIMILARITY_SCORE`
            - Threshold: 0.85+ for high-confidence matches
            
            ### Cortex Integration
            
            | Service | Purpose | Configuration |
            |---------|---------|---------------|
            | **Cortex Analyst** | Natural language → SQL for structured queries | Semantic model: Total_Spend, Inventory_Quantity, Unit_Cost, Supplier_Lead_Time |
            | **Cortex Search** | RAG over engineering documents | Service: `ENGINEERING_DOCS_SEARCH`, indexed by Part_Family and Regulatory_Standard |
            
            ### Key Tables Schema
            
            **ATOMIC.PART_MASTER:**
            ```
            GLOBAL_ID | LOCAL_ID | SOURCE_SYSTEM | MATERIAL | WEIGHT | DIMENSIONS_JSON | COST | SUPPLIER_ID
            ```
            
            **DATA_SCIENCE.PART_SIMILARITY_SCORES:**
            ```
            SOURCE_GLOBAL_ID | TARGET_GLOBAL_ID | SIMILARITY_SCORE | CREATED_AT
            ```
            """
        )

    st.divider()

    # ============================================
    # 6. APPLICATION PAGES (Persona-Based)
    # ============================================
    st.header("Application Pages")
    st.markdown("Each page is optimized for a specific persona with role-appropriate KPIs and workflows.")

    col_page1, col_page2, col_page3 = st.columns(3)

    with col_page1:
        st.markdown(
            """
            ### Part Matcher
            
            **Persona:** R&D Engineer (Technical)
            
            **Purpose:** Find existing validated components for new designs. 
            Prevent duplicate SKUs and reduce time-to-market.
            
            **Key Features:**
            - Spec sheet upload or text search
            - Similarity matching with confidence scores
            - FDA compliance badges
            - Design reuse tracking
            - Cost impact analysis
            
            **Primary KPIs:**
            - Parts reused
            - Engineering hours saved
            - Costs avoided
            """
        )

    with col_page2:
        st.markdown(
            """
            ### Supply Chain Tower
            
            **Persona:** VP of Global Supply Chain (Strategic)
            
            **Purpose:** Strategic view of supplier consolidation, risk 
            mitigation, and cross-BU synergies.
            
            **Key Features:**
            - Consolidation scenario ROI
            - Supplier network visualization
            - Risk heatmap by supplier
            - Cross-BU synergy analysis
            - Part similarity clusters
            
            **Primary KPIs:**
            - Projected savings (15%)
            - Supply base rationalization
            - Risk exposure
            """
        )

    with col_page3:
        st.markdown(
            """
            ### Procurement Ops
            
            **Persona:** Procurement Manager (Operational)
            
            **Purpose:** Identify maverick spend, price anomalies, 
            and supplier optimization opportunities.
            
            **Key Features:**
            - Maverick spend dashboard
            - Price anomaly detection
            - Supplier scorecard with risk
            - Contract compliance tracking
            - Cost avoidance opportunities
            
            **Primary KPIs:**
            - Maverick spend %
            - Contract compliance
            - Avg cycle time
            """
        )

    st.divider()

    # ============================================
    # 7. TECHNOLOGY STACK
    # ============================================
    st.header("Technology Stack")

    st.markdown(
        """
        <div style="display: flex; flex-wrap: wrap; gap: 8px;">
            <span style="background-color: #0ea5e9; color: white; padding: 4px 12px; border-radius: 16px;">Snowflake</span>
            <span style="background-color: #8b5cf6; color: white; padding: 4px 12px; border-radius: 16px;">Cortex AI</span>
            <span style="background-color: #22c55e; color: white; padding: 4px 12px; border-radius: 16px;">Snowpark</span>
            <span style="background-color: #ef4444; color: white; padding: 4px 12px; border-radius: 16px;">Streamlit</span>
            <span style="background-color: #f59e0b; color: white; padding: 4px 12px; border-radius: 16px;">Plotly</span>
            <span style="background-color: #3b82f6; color: white; padding: 4px 12px; border-radius: 16px;">Vector Embeddings</span>
            <span style="background-color: #ec4899; color: white; padding: 4px 12px; border-radius: 16px;">Cortex Agent</span>
            <span style="background-color: #14b8a6; color: white; padding: 4px 12px; border-radius: 16px;">Cortex Search</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("")  # Spacer

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
            **Data Platform:**
            - Snowflake (data warehouse, compute, governance)
            - Snowpark Python (data processing)
            - Analytics views with risk scoring
            """
        )

    with col2:
        st.markdown(
            """
            **AI/ML:**
            - Cortex EMBED_TEXT_768 (vector embeddings)
            - Cortex Agent (unified assistant with multi-tool orchestration)
            - Cortex Search (document RAG)
            """
        )

    st.divider()

    # ============================================
    # 8. GETTING STARTED
    # ============================================
    st.header("Getting Started")

    st.markdown(
        """
        **Choose Your Path:**
        
        | If You Are... | Start With... | Key Actions |
        |---------------|---------------|-------------|
        | **R&D Engineer** | Part Matcher | Upload spec, find alternatives, track reuse |
        | **VP Supply Chain** | Supply Chain Tower | Review consolidation scenarios, assess risk |
        | **Procurement Manager** | Procurement Ops | Identify maverick spend, compare suppliers |
        
        **Sample Questions for the Assistant:**
        - *Engineer:* "What are the FDA requirements for actuators?"
        - *VP:* "What is the total projected savings from consolidation?"
        - *Procurement:* "Which suppliers have the highest maverick spend?"
        """
    )

    st.info(
        "This demo uses synthetic data representing a post-acquisition scenario. "
        "All company names, part numbers, and financial figures are fictional."
    )


if __name__ == "__main__":
    main()
