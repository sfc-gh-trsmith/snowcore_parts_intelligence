"""
Part Matcher - Engineering Component Discovery
Target Persona: R&D Engineer (Technical Level)

STAR Flow:
- Situation: Need component for new design, multiple PLM systems to search
- Task: Find existing validated/compliant part that meets requirements
- Action: Upload spec or search, filter by compliance, select for reuse
- Result: Similar parts with FDA status, time/cost savings from reuse
"""

import uuid

import streamlit as st

from utils.agent import render_agent_panel
from utils.cortex import run_analyst_query, run_cortex_search
from utils.data_loader import run_queries_parallel
from utils.query_registry import register_query
from utils.snowflake import get_session

st.set_page_config(page_title="Part Matcher", page_icon=":material/search:", layout="wide")

st.title("Part Matcher")
st.caption("Find existing validated components for your design. Prevent duplicate SKUs and reduce time-to-market.")

session = get_session()

# Initialize session state for selected parts and reuse events
if "selected_parts" not in st.session_state:
    st.session_state.selected_parts = []
if "reuse_project" not in st.session_state:
    st.session_state.reuse_project = ""

# ============================================================
# SEARCH AND FILTERS (Main Content)
# ============================================================
st.subheader("Search Criteria")

# File uploader and text input in columns
col_upload, col_text = st.columns(2)

with col_upload:
    uploaded_file = st.file_uploader(
        "Upload spec sheet",
        type=["pdf", "txt", "csv"],
        help="Upload engineering drawings, CAD metadata, or spec sheets"
    )

with col_text:
    input_text = st.text_area(
        "Part requirements",
        placeholder="e.g., high-precision valve, stainless steel, FDA compliant",
        height=100
    )

# Filters row
col_cat, col_comp, col_max, col_project = st.columns(4)

with col_cat:
    category_filter = st.selectbox(
        "Part Category",
        options=["All", "Valve", "Motor", "Fastener", "Actuator", "Sensor", "Pump"],
        index=0,
    )

with col_comp:
    compliance_filter = st.selectbox(
        "Compliance Requirement",
        options=["Any", "FDA Approved", "Pending"],
        index=0,
    )

with col_max:
    max_results = st.slider("Max results", 5, 30, 15, 5)

with col_project:
    st.session_state.reuse_project = st.text_input(
        "Project Name (for reuse tracking)",
        value=st.session_state.reuse_project,
        placeholder="e.g., BioReactor Redesign"
    )

st.divider()


# ============================================================
# HELPER FUNCTIONS
# ============================================================
def extract_text_from_upload(file) -> str:
    """Extract searchable text from uploaded file."""
    if file is None:
        return ""
    
    file_type = file.name.split(".")[-1].lower()
    
    if file_type == "txt":
        return file.read().decode("utf-8")
    elif file_type == "csv":
        import pandas as pd
        df = pd.read_csv(file)
        return " ".join(df.astype(str).values.flatten())
    elif file_type == "pdf":
        # For demo purposes, return placeholder
        return file.name.replace(".pdf", "").replace("_", " ")
    
    return ""


def render_compliance_badge(status: str) -> str:
    """Render visual compliance badge."""
    if status and "FDA" in str(status).upper():
        return "[FDA Approved]"
    elif status and "ISO" in str(status).upper():
        return "[ISO Certified]"
    elif status and "PENDING" in str(status).upper():
        return "[Pending]"
    elif status:
        return f"[{status}]"
    return "[Not Certified]"


def format_confidence(score: float) -> str:
    """Format similarity score as confidence percentage."""
    pct = score * 100 if score <= 1 else score
    if pct >= 95:
        return f"High: {pct:.0f}%"
    elif pct >= 80:
        return f"Medium: {pct:.0f}%"
    else:
        return f"Low: {pct:.0f}%"


def record_reuse_event(part_id: str, part_name: str, cost: float, project: str):
    """Record a part reuse event to the database."""
    if not project:
        project = "Unspecified Project"
    
    event_id = f"RE{uuid.uuid4().hex[:10].upper()}"
    # Estimate time saved: 16-40 hours per reused part
    time_saved = 16 + (hash(part_id) % 24)
    
    try:
        insert_sql = f"""
        INSERT INTO DATA_SCIENCE.PART_REUSE_EVENTS 
            (EVENT_ID, PART_GLOBAL_ID, PROJECT_NAME, DESIGN_TIME_SAVED_HOURS, COST_AVOIDED)
        VALUES 
            ('{event_id}', '{part_id}', '{project.replace("'", "''")}', {time_saved}, {cost})
        """
        session.sql(insert_sql).collect()
        return True, time_saved
    except Exception as e:
        # Table might not exist yet - silently fail for demo
        return False, time_saved


# ============================================================
# QUERIES
# ============================================================
# Build WHERE clause for category/compliance filters
filter_clauses = []
if category_filter != "All":
    filter_clauses.append(f"p.PART_CATEGORY = '{category_filter}'")
if compliance_filter != "Any":
    if compliance_filter == "FDA Approved":
        filter_clauses.append("p.COMPLIANCE_STATUS LIKE '%FDA%'")
    else:
        filter_clauses.append("p.COMPLIANCE_STATUS NOT LIKE '%FDA%'")

filter_where = f"WHERE {' AND '.join(filter_clauses)}" if filter_clauses else ""
filter_and = f"AND {' AND '.join(filter_clauses)}" if filter_clauses else ""

TOP_DUPLICATES_SQL = register_query(
    f"top_duplicates_{category_filter}_{compliance_filter}",
    f"""
    SELECT
        p.GLOBAL_ID,
        p.PART_NAME,
        p.PART_CATEGORY,
        p.MATERIAL,
        p.BUSINESS_UNIT,
        p.INVENTORY_VALUE,
        p.COMPLIANCE_STATUS,
        p.UNIT_COST,
        p.BENCHMARK_COST,
        CASE 
            WHEN p.BENCHMARK_COST > 0 
            THEN ROUND((p.UNIT_COST - p.BENCHMARK_COST) / p.BENCHMARK_COST * 100, 1)
            ELSE 0 
        END AS COST_VARIANCE_PCT
    FROM DATA_SCIENCE.PARTS_ANALYTICS p
    WHERE p.IS_DUPLICATE = TRUE
    {filter_and}
    ORDER BY p.INVENTORY_VALUE DESC
    LIMIT 15
    """,
    f"High-value duplicate parts with filters",
)

REUSE_METRICS_SQL = register_query(
    "reuse_metrics",
    """
    SELECT
        COUNT(*) AS TOTAL_REUSE_EVENTS,
        SUM(DESIGN_TIME_SAVED_HOURS) AS TOTAL_HOURS_SAVED,
        SUM(COST_AVOIDED) AS TOTAL_COST_AVOIDED,
        COUNT(DISTINCT PROJECT_NAME) AS PROJECTS_BENEFITED,
        COUNT(DISTINCT PART_GLOBAL_ID) AS UNIQUE_PARTS_REUSED
    FROM DATA_SCIENCE.PART_REUSE_EVENTS
    """,
    "Overall reuse metrics",
)


def build_match_query(user_text: str, limit: int) -> str:
    """Build SQL query for part matching based on user input."""
    safe_text = user_text.replace("'", "''").strip()
    key = f"part_match_{abs(hash((safe_text, limit, category_filter, compliance_filter)))}"
    
    return register_query(
        key,
        f"""
        WITH candidates AS (
            SELECT GLOBAL_ID, PART_NAME, PART_DESCRIPTION, COMPLIANCE_STATUS, PART_CATEGORY
            FROM ATOMIC.PART_MASTER
            WHERE (
                PART_DESCRIPTION ILIKE '%{safe_text}%'
                OR PART_NAME ILIKE '%{safe_text}%'
                OR MATERIAL ILIKE '%{safe_text}%'
            )
            {filter_and}
            ORDER BY INVENTORY_VALUE DESC
            LIMIT 10
        )
        SELECT
            c.GLOBAL_ID AS SOURCE_ID,
            c.PART_NAME AS SOURCE_PART,
            c.PART_CATEGORY AS SOURCE_CATEGORY,
            s.TARGET_GLOBAL_ID AS MATCH_ID,
            pm.PART_NAME AS MATCH_PART,
            pm.PART_CATEGORY AS MATCH_CATEGORY,
            pm.MATERIAL AS MATCH_MATERIAL,
            pm.COMPLIANCE_STATUS AS MATCH_COMPLIANCE,
            pm.UNIT_COST AS MATCH_COST,
            pm.BENCHMARK_COST AS MATCH_BENCHMARK,
            pm.BUSINESS_UNIT AS MATCH_BU,
            s.SIMILARITY_SCORE,
            -- Cost impact: savings vs designing new (avg new part design = $5000)
            5000 - COALESCE(pm.UNIT_COST, 0) AS DESIGN_SAVINGS
        FROM candidates c
        JOIN DATA_SCIENCE.PART_SIMILARITY_SCORES s
            ON c.GLOBAL_ID = s.SOURCE_GLOBAL_ID
        JOIN ATOMIC.PART_MASTER pm
            ON s.TARGET_GLOBAL_ID = pm.GLOBAL_ID
        WHERE s.SIMILARITY_SCORE > 75
        ORDER BY s.SIMILARITY_SCORE DESC
        LIMIT {limit}
        """,
        "Part similarity matches for user input",
    )


def render_match_results(df, show_reuse_buttons: bool = True):
    """Render match results with compliance badges, cost impact, and reuse buttons."""
    if df.empty:
        st.warning("No matching parts found. Try broadening your search criteria or removing filters.")
        return
    
    for idx, row in df.iterrows():
        with st.container():
            cols = st.columns([3, 2, 1, 2, 2])
            
            with cols[0]:
                st.markdown(f"**{row.get('MATCH_PART', row.get('PART_NAME', 'Unknown'))}**")
                part_id = row.get('MATCH_ID', row.get('GLOBAL_ID', 'N/A'))
                category = row.get('MATCH_CATEGORY', row.get('PART_CATEGORY', ''))
                bu = row.get('MATCH_BU', row.get('BUSINESS_UNIT', ''))
                st.caption(f"ID: {part_id} | {category} | {bu}")
            
            with cols[1]:
                # Compliance badge
                compliance = row.get("MATCH_COMPLIANCE", row.get("COMPLIANCE_STATUS", ""))
                st.markdown(render_compliance_badge(compliance))
            
            with cols[2]:
                # Confidence score
                if "SIMILARITY_SCORE" in row:
                    st.markdown(format_confidence(row["SIMILARITY_SCORE"]))
                elif "INVENTORY_VALUE" in row:
                    st.markdown(f"${row['INVENTORY_VALUE']:,.0f}")
            
            with cols[3]:
                # Cost impact
                cost = row.get("MATCH_COST", row.get("UNIT_COST", 0)) or 0
                benchmark = row.get("MATCH_BENCHMARK", row.get("BENCHMARK_COST", 0)) or cost * 0.9
                design_savings = row.get("DESIGN_SAVINGS", 5000 - cost)
                
                st.markdown(f"**${cost:,.0f}** unit cost")
                if design_savings > 0:
                    st.caption(f"~${design_savings:,.0f} vs. new design")
            
            with cols[4]:
                if show_reuse_buttons:
                    part_id = row.get("MATCH_ID", row.get("GLOBAL_ID", f"part_{idx}"))
                    is_fda = "FDA" in str(compliance).upper()
                    
                    if st.button(
                        "✓ Select for Reuse" if is_fda else "Select for Reuse",
                        key=f"reuse_{part_id}_{idx}",
                        type="primary" if is_fda else "secondary",
                        use_container_width=True
                    ):
                        # Record the reuse event
                        part_name = row.get("MATCH_PART", row.get("PART_NAME", "Unknown"))
                        recorded, time_saved = record_reuse_event(
                            part_id, part_name, cost, st.session_state.reuse_project
                        )
                        
                        st.session_state.selected_parts.append({
                            "id": part_id,
                            "name": part_name,
                            "compliance": compliance,
                            "cost": cost,
                            "time_saved": time_saved,
                            "recorded": recorded
                        })
                        st.success(f"Added {part_name} — Est. {time_saved}h saved")
            
            st.divider()


# ============================================================
# MAIN CONTENT
# ============================================================

# Determine search text from file upload or text input
search_text = ""
if uploaded_file:
    search_text = extract_text_from_upload(uploaded_file)
    st.info(f"Analyzing uploaded file: **{uploaded_file.name}**")
elif input_text.strip():
    search_text = input_text.strip()

# ============================================================
# SITUATION: Design Reuse Metrics (show value of reuse program)
# ============================================================
try:
    reuse_results = run_queries_parallel(session, {"metrics": REUSE_METRICS_SQL})
    reuse_metrics = reuse_results["metrics"].iloc[0] if not reuse_results["metrics"].empty else {}
    
    total_events = reuse_metrics.get("TOTAL_REUSE_EVENTS", 0) or 0
    total_hours = reuse_metrics.get("TOTAL_HOURS_SAVED", 0) or 0
    total_cost_avoided = reuse_metrics.get("TOTAL_COST_AVOIDED", 0) or 0
    unique_parts = reuse_metrics.get("UNIQUE_PARTS_REUSED", 0) or 0
    
    if total_events > 0:
        st.subheader("Design Reuse Impact")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Parts Reused", f"{total_events}")
        col2.metric("Engineering Hours Saved", f"{total_hours:,.0f}h")
        col3.metric("Cost Avoided", f"${total_cost_avoided:,.0f}")
        col4.metric("Unique Components", f"{unique_parts}")
        st.divider()
except Exception:
    # Table might not exist yet
    pass

# ============================================================
# TASK: Selected parts for current session
# ============================================================
if st.session_state.selected_parts:
    st.subheader("Selected for Reuse")
    
    total_cost = sum(p.get("cost", 0) for p in st.session_state.selected_parts)
    total_time = sum(p.get("time_saved", 20) for p in st.session_state.selected_parts)
    fda_count = sum(1 for p in st.session_state.selected_parts if "FDA" in str(p.get("compliance", "")).upper())
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Parts Selected", len(st.session_state.selected_parts))
    col2.metric("Est. Time Saved", f"{total_time}h")
    col3.metric("Est. Cost Avoided", f"${5000 * len(st.session_state.selected_parts) - total_cost:,.0f}")
    col4.metric("FDA Compliant", f"{fda_count}/{len(st.session_state.selected_parts)}")
    
    # List selected parts
    with st.expander("View Selection", expanded=False):
        for part in st.session_state.selected_parts:
            cols = st.columns([4, 2, 2])
            cols[0].markdown(f"**{part['name']}** ({part['id']})")
            cols[1].markdown(render_compliance_badge(part.get("compliance", "")))
            cols[2].markdown(f"${part.get('cost', 0):,.0f}")
        
        if st.button("Clear All Selections", type="secondary"):
            st.session_state.selected_parts = []
            st.rerun()
    
    st.divider()

# ============================================================
# ACTION: Search and Match
# ============================================================
if search_text:
    match_sql = build_match_query(search_text, max_results)
    results = run_queries_parallel(session, {"matches": match_sql})
    
    st.subheader("Similar Existing Parts")
    st.markdown("Parts marked [FDA Approved] are **FDA compliant** — prioritize these to avoid compliance delays.")
    
    render_match_results(results["matches"], show_reuse_buttons=True)
    
    # ============================================================
    # RESULT: Summary metrics
    # ============================================================
    matches_df = results["matches"]
    if not matches_df.empty:
        st.divider()
        st.subheader("Search Results Summary")
        
        fda_count = matches_df["MATCH_COMPLIANCE"].str.upper().str.contains("FDA", na=False).sum()
        avg_score = matches_df["SIMILARITY_SCORE"].mean()
        avg_score_pct = avg_score * 100 if avg_score <= 1 else avg_score
        avg_cost = matches_df["MATCH_COST"].mean()
        total_design_savings = matches_df["DESIGN_SAVINGS"].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Matches Found", len(matches_df))
        col2.metric("FDA Approved", fda_count)
        col3.metric("Avg Confidence", f"{avg_score_pct:.0f}%")
        col4.metric("Potential Savings", f"${total_design_savings:,.0f}")
        
        # Cost impact breakdown
        st.markdown("**Cost Impact Analysis:**")
        st.markdown(f"- Average unit cost: **${avg_cost:,.0f}**")
        st.markdown(f"- Estimated new part design cost: **$5,000**")
        st.markdown(f"- Savings per reused part: **${5000 - avg_cost:,.0f}**")
else:
    # ============================================================
    # Default view: High-value duplicates
    # ============================================================
    results = run_queries_parallel(session, {"top_duplicates": TOP_DUPLICATES_SQL})
    
    st.subheader("High-Value Duplicate Parts")
    st.markdown("These parts have existing equivalents in the catalog. Selecting one **prevents creating a duplicate SKU**.")
    
    render_match_results(results["top_duplicates"], show_reuse_buttons=True)
    
    # Duplicate summary
    dup_df = results["top_duplicates"]
    if not dup_df.empty:
        total_dup_value = dup_df["INVENTORY_VALUE"].sum()
        fda_dups = dup_df["COMPLIANCE_STATUS"].str.upper().str.contains("FDA", na=False).sum()
        
        st.divider()
        col1, col2, col3 = st.columns(3)
        col1.metric("Duplicates Shown", len(dup_df))
        col2.metric("Combined Inventory Value", f"${total_dup_value:,.0f}")
        col3.metric("FDA Approved Alternatives", fda_dups)

# ============================================================
# ASK THE ASSISTANT
# ============================================================
render_agent_panel(session, persona_context="engineer")
