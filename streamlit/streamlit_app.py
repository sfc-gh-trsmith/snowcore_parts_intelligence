"""
UPIP Data Explorer - Landing Page

Provides exploratory visualizations and KPIs to set context for the 
post-acquisition parts intelligence challenge.
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.data_loader import run_queries_parallel
from utils.query_registry import register_query
from utils.snowflake import get_session

st.set_page_config(
    page_title="UPIP – Unified Parts Intelligence Platform",
    page_icon=":material/settings:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def format_currency(value: float) -> str:
    """Format large numbers as currency with K/M suffixes."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}"


def format_number(value: float) -> str:
    """Format large numbers with K/M suffixes."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.0f}"


def create_sankey_diagram(sankey_df):
    """Create a Sankey diagram showing data flow from source systems through processing."""
    # Build nodes and links for the Sankey
    # Flow: Source System -> Business Unit -> Part Category -> Output Type
    
    # Aggregate data for each flow step
    source_to_bu = sankey_df.groupby(["SOURCE_SYSTEM", "BUSINESS_UNIT"])["PART_COUNT"].sum().reset_index()
    bu_to_category = sankey_df.groupby(["BUSINESS_UNIT", "PART_CATEGORY"])["PART_COUNT"].sum().reset_index()
    
    # Define all unique nodes
    sources = sankey_df["SOURCE_SYSTEM"].unique().tolist()
    bus = sankey_df["BUSINESS_UNIT"].unique().tolist()
    categories = sankey_df["PART_CATEGORY"].unique().tolist()
    outputs = ["Similarity Scores", "Parts Analytics", "Cortex Search"]
    
    all_nodes = sources + bus + categories + outputs
    node_indices = {node: i for i, node in enumerate(all_nodes)}
    
    # Build links
    link_sources = []
    link_targets = []
    link_values = []
    link_colors = []
    
    # Snowflake color palette
    colors = {
        "source": "rgba(41, 181, 232, 0.6)",  # Cyan
        "bu": "rgba(255, 159, 54, 0.6)",       # Orange
        "category": "rgba(34, 197, 94, 0.6)",  # Green
        "output": "rgba(139, 92, 246, 0.6)",   # Purple
    }
    
    # Source System -> Business Unit
    for _, row in source_to_bu.iterrows():
        link_sources.append(node_indices[row["SOURCE_SYSTEM"]])
        link_targets.append(node_indices[row["BUSINESS_UNIT"]])
        link_values.append(int(row["PART_COUNT"]))
        link_colors.append(colors["source"])
    
    # Business Unit -> Category
    for _, row in bu_to_category.iterrows():
        link_sources.append(node_indices[row["BUSINESS_UNIT"]])
        link_targets.append(node_indices[row["PART_CATEGORY"]])
        link_values.append(int(row["PART_COUNT"]))
        link_colors.append(colors["bu"])
    
    # Category -> Outputs (distribute evenly)
    total_parts = sankey_df["PART_COUNT"].sum()
    for cat in categories:
        cat_total = sankey_df[sankey_df["PART_CATEGORY"] == cat]["PART_COUNT"].sum()
        for output in outputs:
            link_sources.append(node_indices[cat])
            link_targets.append(node_indices[output])
            link_values.append(int(cat_total / len(outputs)))
            link_colors.append(colors["category"])
    
    # Node colors
    node_colors = (
        ["#29B5E8"] * len(sources) +      # Cyan for sources
        ["#FF9F36"] * len(bus) +           # Orange for BU
        ["#22c55e"] * len(categories) +    # Green for categories
        ["#8b5cf6"] * len(outputs)         # Purple for outputs
    )
    
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="#1e293b", width=0.5),
            label=all_nodes,
            color=node_colors,
        ),
        link=dict(
            source=link_sources,
            target=link_targets,
            value=link_values,
            color=link_colors,
        )
    )])
    
    fig.update_layout(
        font=dict(size=12, color="white"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=350,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    
    return fig


def main():
    session = get_session()
    
    # ============================================
    # 1. HEADER
    # ============================================
    st.title("Unified Parts Intelligence Platform")
    st.markdown(
        "Exploring the post-acquisition data landscape: **Snowcore Industries + BioFlux Automation**"
    )
    
    st.divider()
    
    # ============================================
    # 2. LOAD DATA
    # ============================================
    
    # Define queries for KPIs and visualizations
    KPI_SQL = register_query(
        "landing_kpis",
        """
        SELECT
            COUNT(DISTINCT GLOBAL_ID) AS TOTAL_SKUS,
            SUM(CASE WHEN IS_DUPLICATE THEN 1 ELSE 0 END) AS DUPLICATE_COUNT,
            COUNT(DISTINCT SUPPLIER_ID) AS SUPPLIER_COUNT,
            SUM(CASE WHEN COMPLIANCE_STATUS LIKE '%FDA%' THEN 1 ELSE 0 END) AS FDA_COUNT,
            SUM(TOTAL_SPEND) AS TOTAL_SPEND,
            SUM(INVENTORY_VALUE) AS INVENTORY_VALUE
        FROM DATA_SCIENCE.PARTS_ANALYTICS
        """,
        "Overview KPIs for landing page",
    )
    
    SANKEY_SQL = register_query(
        "landing_sankey",
        """
        SELECT 
            SOURCE_SYSTEM, 
            BUSINESS_UNIT, 
            PART_CATEGORY, 
            COUNT(*) AS PART_COUNT
        FROM ATOMIC.PART_MASTER
        GROUP BY SOURCE_SYSTEM, BUSINESS_UNIT, PART_CATEGORY
        """,
        "Data for Sankey diagram",
    )
    
    SAVINGS_SQL = register_query(
        "landing_savings",
        """
        SELECT 
            SUM(PROJECTED_SAVINGS) AS TOTAL_POTENTIAL_SAVINGS,
            COUNT(*) AS SCENARIO_COUNT
        FROM DATA_SCIENCE.CONSOLIDATION_SCENARIOS
        """,
        "Consolidation savings potential",
    )
    
    BU_BREAKDOWN_SQL = register_query(
        "landing_bu_breakdown",
        """
        SELECT
            BUSINESS_UNIT,
            COUNT(DISTINCT GLOBAL_ID) AS SKU_COUNT,
            SUM(TOTAL_SPEND) AS TOTAL_SPEND,
            SUM(INVENTORY_VALUE) AS INVENTORY_VALUE,
            SUM(CASE WHEN IS_DUPLICATE THEN 1 ELSE 0 END) AS DUPLICATE_COUNT
        FROM DATA_SCIENCE.PARTS_ANALYTICS
        GROUP BY BUSINESS_UNIT
        """,
        "Breakdown by business unit",
    )
    
    CATEGORY_BREAKDOWN_SQL = register_query(
        "landing_category_breakdown",
        """
        SELECT
            PART_CATEGORY,
            COUNT(DISTINCT GLOBAL_ID) AS SKU_COUNT,
            SUM(TOTAL_SPEND) AS TOTAL_SPEND,
            AVG(UNIT_COST) AS AVG_UNIT_COST
        FROM DATA_SCIENCE.PARTS_ANALYTICS
        GROUP BY PART_CATEGORY
        ORDER BY TOTAL_SPEND DESC
        """,
        "Breakdown by part category",
    )
    
    COMPLIANCE_BREAKDOWN_SQL = register_query(
        "landing_compliance_breakdown",
        """
        SELECT
            COMPLIANCE_STATUS,
            COUNT(DISTINCT GLOBAL_ID) AS SKU_COUNT
        FROM DATA_SCIENCE.PARTS_ANALYTICS
        GROUP BY COMPLIANCE_STATUS
        """,
        "Breakdown by compliance status",
    )
    
    REGION_BREAKDOWN_SQL = register_query(
        "landing_region_breakdown",
        """
        SELECT
            SUPPLIER_REGION,
            COUNT(DISTINCT SUPPLIER_ID) AS SUPPLIER_COUNT,
            SUM(TOTAL_SPEND) AS TOTAL_SPEND
        FROM DATA_SCIENCE.PARTS_ANALYTICS
        GROUP BY SUPPLIER_REGION
        """,
        "Breakdown by supplier region",
    )
    
    # Run all queries in parallel
    with st.spinner("Loading data..."):
        results = run_queries_parallel(session, {
            "kpis": KPI_SQL,
            "sankey": SANKEY_SQL,
            "savings": SAVINGS_SQL,
            "bu_breakdown": BU_BREAKDOWN_SQL,
            "category_breakdown": CATEGORY_BREAKDOWN_SQL,
            "compliance_breakdown": COMPLIANCE_BREAKDOWN_SQL,
            "region_breakdown": REGION_BREAKDOWN_SQL,
        })
    
    kpi_df = results["kpis"]
    sankey_df = results["sankey"]
    savings_df = results["savings"]
    bu_df = results["bu_breakdown"]
    category_df = results["category_breakdown"]
    compliance_df = results["compliance_breakdown"]
    region_df = results["region_breakdown"]
    
    # ============================================
    # 3. HIGH-LEVEL KPIs
    # ============================================
    st.subheader("Integration Challenge at a Glance")
    
    kpi_row = kpi_df.iloc[0]
    savings_row = savings_df.iloc[0]
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    col1.metric(
        "Total SKUs",
        format_number(kpi_row["TOTAL_SKUS"]),
        "Across both systems",
    )
    col2.metric(
        "Duplicates Found",
        format_number(kpi_row["DUPLICATE_COUNT"]),
        f"{kpi_row['DUPLICATE_COUNT'] / kpi_row['TOTAL_SKUS'] * 100:.0f}% of catalog",
    )
    col3.metric(
        "Suppliers",
        format_number(kpi_row["SUPPLIER_COUNT"]),
        "Active vendors",
    )
    col4.metric(
        "FDA Compliant",
        format_number(kpi_row["FDA_COUNT"]),
        "Regulated parts",
    )
    col5.metric(
        "Total Spend",
        format_currency(kpi_row["TOTAL_SPEND"]),
        "Annual procurement",
    )
    col6.metric(
        "Savings Potential",
        format_currency(savings_row["TOTAL_POTENTIAL_SAVINGS"] or 0),
        f"{int(savings_row['SCENARIO_COUNT'] or 0)} scenarios",
    )
    
    st.divider()
    
    # ============================================
    # 4. SANKEY DIAGRAM - DATA FLOW
    # ============================================
    st.subheader("Data Flow: From Source Systems to Intelligence")
    st.caption(
        "Parts flow from legacy PLM systems through our unified data model to analytical outputs"
    )
    
    sankey_fig = create_sankey_diagram(sankey_df)
    st.plotly_chart(sankey_fig, use_container_width=True)
    
    st.divider()
    
    # ============================================
    # 5. DISTRIBUTION VISUALIZATIONS
    # ============================================
    st.subheader("Data Distribution")
    
    col_bu, col_category = st.columns(2)
    
    with col_bu:
        st.markdown("**Parts by Business Unit**")
        bu_fig = px.pie(
            bu_df,
            values="SKU_COUNT",
            names="BUSINESS_UNIT",
            color="BUSINESS_UNIT",
            color_discrete_map={
                "Industrial": "#29B5E8",
                "Bio-Tech": "#FF9F36",
            },
            hole=0.4,
        )
        bu_fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            height=280,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        )
        st.plotly_chart(bu_fig, use_container_width=True)
    
    with col_category:
        st.markdown("**Spend by Part Category**")
        cat_fig = px.bar(
            category_df,
            x="PART_CATEGORY",
            y="TOTAL_SPEND",
            color="TOTAL_SPEND",
            color_continuous_scale=["#1e3a5f", "#29B5E8"],
        )
        cat_fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            height=280,
            margin=dict(l=20, r=20, t=20, b=40),
            showlegend=False,
            xaxis_title="",
            yaxis_title="Total Spend ($)",
            coloraxis_showscale=False,
        )
        cat_fig.update_xaxes(tickangle=45)
        st.plotly_chart(cat_fig, use_container_width=True)
    
    col_compliance, col_region = st.columns(2)
    
    with col_compliance:
        st.markdown("**Compliance Status Distribution**")
        comp_fig = px.pie(
            compliance_df,
            values="SKU_COUNT",
            names="COMPLIANCE_STATUS",
            color="COMPLIANCE_STATUS",
            color_discrete_map={
                "FDA Approved": "#22c55e",
                "Pending": "#f59e0b",
            },
            hole=0.4,
        )
        comp_fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            height=280,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        )
        st.plotly_chart(comp_fig, use_container_width=True)
    
    with col_region:
        st.markdown("**Supplier Concentration by Region**")
        region_fig = px.bar(
            region_df,
            x="SUPPLIER_REGION",
            y="SUPPLIER_COUNT",
            color="TOTAL_SPEND",
            color_continuous_scale=["#3b1e5f", "#8b5cf6"],
            text="SUPPLIER_COUNT",
        )
        region_fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            height=280,
            margin=dict(l=20, r=20, t=20, b=40),
            showlegend=False,
            xaxis_title="",
            yaxis_title="Supplier Count",
            coloraxis_showscale=False,
        )
        region_fig.update_traces(textposition="outside")
        st.plotly_chart(region_fig, use_container_width=True)
    
    st.divider()
    
    # ============================================
    # 6. BUSINESS UNIT COMPARISON
    # ============================================
    st.subheader("Business Unit Comparison")
    
    col1, col2 = st.columns(2)
    
    for idx, (_, row) in enumerate(bu_df.iterrows()):
        col = col1 if idx == 0 else col2
        with col:
            bu_name = row["BUSINESS_UNIT"]
            color = "#29B5E8" if bu_name == "Industrial" else "#FF9F36"
            
            st.markdown(
                f"""
                <div style="padding: 16px; border: 2px solid {color}; border-radius: 8px; margin-bottom: 8px;">
                    <h4 style="color: {color}; margin: 0 0 12px 0;">{bu_name}</h4>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
                        <div>
                            <span style="color: #94a3b8; font-size: 12px;">SKUs</span><br>
                            <strong style="font-size: 18px;">{format_number(row['SKU_COUNT'])}</strong>
                        </div>
                        <div>
                            <span style="color: #94a3b8; font-size: 12px;">Duplicates</span><br>
                            <strong style="font-size: 18px;">{format_number(row['DUPLICATE_COUNT'])}</strong>
                        </div>
                        <div>
                            <span style="color: #94a3b8; font-size: 12px;">Total Spend</span><br>
                            <strong style="font-size: 18px;">{format_currency(row['TOTAL_SPEND'])}</strong>
                        </div>
                        <div>
                            <span style="color: #94a3b8; font-size: 12px;">Inventory Value</span><br>
                            <strong style="font-size: 18px;">{format_currency(row['INVENTORY_VALUE'])}</strong>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    
    st.divider()
    
    # ============================================
    # 7. CALL-TO-ACTION NAVIGATION
    # ============================================
    st.subheader("Explore by Role")
    st.markdown("Select your persona to dive into role-specific analytics and workflows.")
    
    col_eng, col_vp, col_proc = st.columns(3)
    
    with col_eng:
        st.markdown(
            """
            <div style="padding: 20px; border: 1px solid #334155; border-radius: 8px; text-align: center;">
                <div style="font-size: 32px; margin-bottom: 8px;">:material/engineering:</div>
                <h4 style="margin: 0 0 8px 0;">R&D Engineer</h4>
                <p style="color: #94a3b8; font-size: 14px; margin-bottom: 12px;">
                    Find existing parts for new designs.<br>
                    Prevent duplicate SKUs.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("Open Part Matcher", key="btn_eng", use_container_width=True):
            st.switch_page("pages/1_Part_Matcher.py")
    
    with col_vp:
        st.markdown(
            """
            <div style="padding: 20px; border: 1px solid #334155; border-radius: 8px; text-align: center;">
                <div style="font-size: 32px; margin-bottom: 8px;">:material/domain:</div>
                <h4 style="margin: 0 0 8px 0;">VP Supply Chain</h4>
                <p style="color: #94a3b8; font-size: 14px; margin-bottom: 12px;">
                    Strategic consolidation scenarios.<br>
                    Supplier risk analysis.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("Open Supply Chain Tower", key="btn_vp", use_container_width=True):
            st.switch_page("pages/2_Supply_Chain_Tower.py")
    
    with col_proc:
        st.markdown(
            """
            <div style="padding: 20px; border: 1px solid #334155; border-radius: 8px; text-align: center;">
                <div style="font-size: 32px; margin-bottom: 8px;">:material/shopping_cart:</div>
                <h4 style="margin: 0 0 8px 0;">Procurement Manager</h4>
                <p style="color: #94a3b8; font-size: 14px; margin-bottom: 12px;">
                    Maverick spend detection.<br>
                    Price anomaly analysis.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("Open Procurement Ops", key="btn_proc", use_container_width=True):
            st.switch_page("pages/3_Procurement_Ops.py")
    
    st.markdown("")  # Spacer
    
    st.info(
        "**New to UPIP?** Visit the [About page](About) to learn about the platform architecture, "
        "data sources, and how AI-powered similarity matching works."
    )


if __name__ == "__main__":
    main()
