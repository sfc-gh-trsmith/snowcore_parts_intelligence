"""
Supply Chain Tower Dashboard
Target Persona: VP of Global Supply Chain (Strategic Level)

STAR Flow:
- Situation: See duplicate spend and risk exposure across business units
- Task: Identify consolidation ROI and supplier rationalization opportunities
- Action: Filter by BU, review scenarios, assess supplier risk
- Result: Projected savings, risk scores, consolidation roadmap
"""

import networkx as nx
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.agent import render_agent_panel
from utils.data_loader import run_queries_parallel
from utils.query_registry import register_query
from utils.snowflake import get_session

st.set_page_config(page_title="Supply Chain Tower", page_icon=":material/domain:", layout="wide")

st.title("Supply Chain Tower")
st.caption("Strategic view of supplier consolidation, risk mitigation, and cross-BU synergies.")

session = get_session()

# ============================================================
# FILTERS AND TARGETS (Main Content)
# ============================================================
col_filter, col_targets = st.columns([1, 3])

with col_filter:
    business_unit = st.selectbox(
        "Business Unit",
        options=["All", "Industrial", "Bio-Tech"],
        index=0,
        help="Filter metrics by business unit (post-acquisition)"
    )

with col_targets:
    st.info("**Strategic Targets:** SKU Reduction: 30% | Procurement Savings: 15% | Supplier Count: -20% | Risk Score: <0.4")

st.divider()

# Build WHERE clause based on filter
bu_filter = ""
if business_unit != "All":
    bu_filter = f"WHERE BUSINESS_UNIT = '{business_unit}'"

# Define queries
KPI_SQL = register_query(
    f"kpi_metrics_{business_unit}",
    f"""
    SELECT
        SUM(TOTAL_SPEND) AS TOTAL_SPEND,
        SUM(INVENTORY_VALUE) AS TOTAL_INVENTORY_VALUE,
        SUM(CASE WHEN IS_DUPLICATE THEN INVENTORY_VALUE ELSE 0 END) AS DUPLICATE_INVENTORY_VALUE,
        AVG(SUPPLIER_LEAD_TIME) AS AVG_LEAD_TIME,
        COUNT(DISTINCT GLOBAL_ID) AS TOTAL_SKUS,
        SUM(CASE WHEN IS_DUPLICATE THEN 1 ELSE 0 END) AS DUPLICATE_SKUS,
        SUM(CASE WHEN COMPLIANCE_STATUS LIKE '%FDA%' THEN 1 ELSE 0 END) AS FDA_COMPLIANT_COUNT,
        COUNT(DISTINCT SUPPLIER_ID) AS SUPPLIER_COUNT,
        AVG(COMPOSITE_RISK) AS AVG_RISK
    FROM DATA_SCIENCE.PARTS_ANALYTICS
    {bu_filter}
    """,
    f"Primary KPI metrics for {business_unit}",
)

BU_BREAKDOWN_SQL = register_query(
    "bu_breakdown",
    """
    SELECT
        BUSINESS_UNIT,
        SUM(TOTAL_SPEND) AS TOTAL_SPEND,
        SUM(CASE WHEN IS_DUPLICATE THEN INVENTORY_VALUE ELSE 0 END) AS DUPLICATE_VALUE,
        COUNT(DISTINCT GLOBAL_ID) AS SKU_COUNT,
        SUM(CASE WHEN IS_DUPLICATE THEN 1 ELSE 0 END) AS DUPLICATE_COUNT,
        COUNT(DISTINCT SUPPLIER_ID) AS SUPPLIER_COUNT,
        AVG(COMPOSITE_RISK) AS AVG_RISK
    FROM DATA_SCIENCE.PARTS_ANALYTICS
    GROUP BY BUSINESS_UNIT
    """,
    "Spend and duplicates by business unit",
)

CONSOLIDATION_SCENARIOS_SQL = register_query(
    "consolidation_scenarios",
    """
    SELECT
        c.SCENARIO_ID,
        c.SCENARIO_NAME,
        c.SOURCE_SUPPLIERS,
        c.TARGET_SUPPLIER_ID,
        s.SUPPLIER_NAME AS TARGET_SUPPLIER_NAME,
        c.PARTS_AFFECTED,
        c.PROJECTED_SAVINGS,
        c.IMPLEMENTATION_COST,
        c.ROI_PCT,
        c.PROJECTED_SAVINGS - c.IMPLEMENTATION_COST AS NET_BENEFIT,
        c.STATUS,
        COALESCE(r.COMPOSITE_RISK, 0.5) AS TARGET_RISK
    FROM DATA_SCIENCE.CONSOLIDATION_SCENARIOS c
    LEFT JOIN ATOMIC.SUPPLIER_MASTER s ON c.TARGET_SUPPLIER_ID = s.SUPPLIER_ID
    LEFT JOIN DATA_SCIENCE.SUPPLIER_RISK_SCORES r ON c.TARGET_SUPPLIER_ID = r.SUPPLIER_ID
    ORDER BY c.PROJECTED_SAVINGS DESC
    """,
    "Consolidation scenarios with supplier details",
)

SUPPLIER_RISK_SQL = register_query(
    "supplier_risk",
    """
    SELECT
        s.SUPPLIER_ID,
        s.SUPPLIER_NAME,
        s.SUPPLIER_REGION,
        s.SUPPLIER_TIER,
        s.TOTAL_SPEND,
        COALESCE(r.FINANCIAL_RISK, 0.5) AS FINANCIAL_RISK,
        COALESCE(r.DELIVERY_RISK, 0.5) AS DELIVERY_RISK,
        COALESCE(r.QUALITY_RISK, 0.5) AS QUALITY_RISK,
        COALESCE(r.COMPOSITE_RISK, 0.5) AS COMPOSITE_RISK,
        COALESCE(r.SUPPLY_CONTINUITY, 0.5) AS SUPPLY_CONTINUITY,
        COUNT(DISTINCT p.GLOBAL_ID) AS PART_COUNT
    FROM ATOMIC.SUPPLIER_MASTER s
    LEFT JOIN DATA_SCIENCE.SUPPLIER_RISK_SCORES r ON s.SUPPLIER_ID = r.SUPPLIER_ID
    LEFT JOIN ATOMIC.PART_MASTER p ON s.SUPPLIER_ID = p.SUPPLIER_ID
    GROUP BY s.SUPPLIER_ID, s.SUPPLIER_NAME, s.SUPPLIER_REGION, s.SUPPLIER_TIER,
             s.TOTAL_SPEND, r.FINANCIAL_RISK, r.DELIVERY_RISK, r.QUALITY_RISK,
             r.COMPOSITE_RISK, r.SUPPLY_CONTINUITY
    ORDER BY COMPOSITE_RISK DESC
    """,
    "Supplier risk scores for heatmap",
)

SUPPLIER_NETWORK_SQL = register_query(
    f"supplier_network_{business_unit}",
    f"""
    SELECT
        SUPPLIER_NAME,
        SUPPLIER_REGION,
        BUSINESS_UNIT,
        SUPPLIER_TIER,
        COUNT(DISTINCT GLOBAL_ID) AS PART_COUNT,
        SUM(INVENTORY_VALUE) AS INVENTORY_VALUE,
        AVG(COMPOSITE_RISK) AS AVG_RISK
    FROM DATA_SCIENCE.PARTS_ANALYTICS
    {bu_filter}
    GROUP BY SUPPLIER_NAME, SUPPLIER_REGION, BUSINESS_UNIT, SUPPLIER_TIER
    """,
    "Supplier network data for visualization",
)

CLUSTER_SQL = register_query(
    f"cluster_points_{business_unit}",
    f"""
    SELECT
        MOD(ABS(HASH(p.GLOBAL_ID)), 100) AS TSNE_X,
        MOD(ABS(HASH(p.GLOBAL_ID, 99)), 100) AS TSNE_Y,
        p.MATERIAL,
        p.IS_DUPLICATE,
        p.BUSINESS_UNIT,
        p.PART_CATEGORY
    FROM ATOMIC.PART_MASTER p
    {bu_filter.replace('BUSINESS_UNIT', 'p.BUSINESS_UNIT') if bu_filter else ''}
    LIMIT 1000
    """,
    f"Pseudo t-SNE cluster coordinates for {business_unit}",
)

# Run queries in parallel
results = run_queries_parallel(
    session,
    {
        "kpi": KPI_SQL,
        "bu_breakdown": BU_BREAKDOWN_SQL,
        "consolidation": CONSOLIDATION_SCENARIOS_SQL,
        "supplier_risk": SUPPLIER_RISK_SQL,
        "supplier_network": SUPPLIER_NETWORK_SQL,
        "clusters": CLUSTER_SQL,
    },
)

kpi = results["kpi"].iloc[0] if not results["kpi"].empty else {}

# Calculate projected savings
total_spend = kpi.get("TOTAL_SPEND", 0) or 0
duplicate_value = kpi.get("DUPLICATE_INVENTORY_VALUE", 0) or 0
total_skus = kpi.get("TOTAL_SKUS", 1) or 1
duplicate_skus = kpi.get("DUPLICATE_SKUS", 0) or 0
supplier_count = kpi.get("SUPPLIER_COUNT", 0) or 0
avg_risk = kpi.get("AVG_RISK", 0.5) or 0.5

# DRD targets
projected_savings = total_spend * 0.15
sku_reduction_pct = (duplicate_skus / total_skus * 100) if total_skus > 0 else 0

# ============================================================
# SITUATION: Strategic KPIs Overview
# ============================================================
st.subheader("Consolidation Impact")
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Projected Savings (15%)",
    f"${projected_savings:,.0f}",
    delta="Target: 15% of spend",
    delta_color="normal"
)
col2.metric(
    "Duplicate Inventory at Risk",
    f"${duplicate_value:,.0f}",
    delta=f"{duplicate_skus} duplicate SKUs"
)
col3.metric(
    "Active Suppliers",
    f"{supplier_count}",
    delta="Target: -20% reduction"
)
col4.metric(
    "Avg Supply Risk",
    f"{avg_risk:.2f}",
    delta="Target: <0.40",
    delta_color="normal" if avg_risk < 0.4 else "inverse"
)

st.divider()

# ============================================================
# TASK: Consolidation Scenarios (VP Strategic Planning)
# ============================================================
st.subheader("Consolidation Scenarios")
st.markdown("Active supplier consolidation initiatives with projected ROI and implementation status.")

consolidation_df = results["consolidation"]
if not consolidation_df.empty:
    # Summary metrics
    total_scenario_savings = consolidation_df["PROJECTED_SAVINGS"].sum()
    total_impl_cost = consolidation_df["IMPLEMENTATION_COST"].sum()
    avg_roi = consolidation_df["ROI_PCT"].mean()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Projected Savings", f"${total_scenario_savings:,.0f}")
    col2.metric("Implementation Cost", f"${total_impl_cost:,.0f}")
    col3.metric("Net Benefit", f"${total_scenario_savings - total_impl_cost:,.0f}")
    col4.metric("Avg ROI", f"{avg_roi:.0f}%")
    
    # Scenario visualization
    col_chart, col_table = st.columns([1, 1])
    
    with col_chart:
        # Waterfall-style bar chart
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name="Projected Savings",
            x=consolidation_df["SCENARIO_NAME"],
            y=consolidation_df["PROJECTED_SAVINGS"],
            marker_color="#22c55e"
        ))
        
        fig.add_trace(go.Bar(
            name="Implementation Cost",
            x=consolidation_df["SCENARIO_NAME"],
            y=-consolidation_df["IMPLEMENTATION_COST"],
            marker_color="#ef4444"
        ))
        
        fig.update_layout(
            barmode="relative",
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            font=dict(color="#e2e8f0"),
            height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_tickangle=-45
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col_table:
        # Status-colored scenario table
        def status_color(status):
            colors = {
                "proposed": "#3b82f6",
                "approved": "#f59e0b",
                "in_progress": "#8b5cf6",
                "completed": "#22c55e"
            }
            return f"background-color: {colors.get(status, '#64748b')}"
        
        display_df = consolidation_df[[
            "SCENARIO_NAME", "TARGET_SUPPLIER_NAME", "PARTS_AFFECTED",
            "PROJECTED_SAVINGS", "ROI_PCT", "STATUS"
        ]].copy()
        display_df.columns = ["Scenario", "Target Supplier", "Parts", "Savings", "ROI %", "Status"]
        
        st.dataframe(
            display_df.style.applymap(status_color, subset=["Status"]),
            use_container_width=True,
            height=350
        )

st.divider()

# ============================================================
# ACTION: Supplier Risk Heatmap
# ============================================================
st.subheader("Supplier Risk Assessment")
st.markdown("Identify high-risk suppliers for prioritized mitigation or replacement.")

risk_df = results["supplier_risk"]
if not risk_df.empty:
    # Multi-dimensional risk heatmap
    risk_categories = ["FINANCIAL_RISK", "DELIVERY_RISK", "QUALITY_RISK"]
    risk_data = risk_df[risk_categories].values.T
    
    fig = go.Figure(data=go.Heatmap(
        z=risk_data,
        x=risk_df["SUPPLIER_NAME"].tolist(),
        y=["Financial", "Delivery", "Quality"],
        colorscale=[
            [0, "#22c55e"],
            [0.3, "#22c55e"],
            [0.3, "#f59e0b"],
            [0.6, "#f59e0b"],
            [0.6, "#ef4444"],
            [1.0, "#ef4444"]
        ],
        showscale=True,
        colorbar=dict(title="Risk"),
        hovertemplate="Supplier: %{x}<br>Category: %{y}<br>Risk: %{z:.2f}<extra></extra>"
    ))
    
    fig.update_layout(
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        font=dict(color="#e2e8f0"),
        height=250,
        xaxis_tickangle=-45
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # High-risk supplier callout
    high_risk = risk_df[risk_df["COMPOSITE_RISK"] >= 0.5]
    if not high_risk.empty:
        st.warning(f"**{len(high_risk)} suppliers** have elevated risk (>0.5). Consider diversification or mitigation strategies.")

st.divider()

# ============================================================
# Business Unit Comparison
# ============================================================
st.subheader("Cross-BU Synergy Analysis")
bu_df = results["bu_breakdown"]
if not bu_df.empty:
    col_chart, col_metrics = st.columns([2, 1])
    
    with col_chart:
        fig_bu = go.Figure()
        
        fig_bu.add_trace(go.Bar(
            name="Total Spend",
            x=bu_df["BUSINESS_UNIT"],
            y=bu_df["TOTAL_SPEND"],
            marker_color="#3b82f6"
        ))
        
        fig_bu.add_trace(go.Bar(
            name="Duplicate Value",
            x=bu_df["BUSINESS_UNIT"],
            y=bu_df["DUPLICATE_VALUE"],
            marker_color="#ef4444"
        ))
        
        fig_bu.update_layout(
            barmode="group",
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            font=dict(color="#e2e8f0"),
            height=300,
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        
        st.plotly_chart(fig_bu, use_container_width=True)
    
    with col_metrics:
        for _, row in bu_df.iterrows():
            bu_name = row["BUSINESS_UNIT"]
            savings = row["TOTAL_SPEND"] * 0.15
            st.metric(
                f"{bu_name} Synergy Potential",
                f"${savings:,.0f}",
                delta=f"{row['SUPPLIER_COUNT']} suppliers, {row['DUPLICATE_COUNT']} duplicates"
            )
        
        # Cross-BU synergy indicator
        if len(bu_df) > 1:
            shared_spend = bu_df["DUPLICATE_VALUE"].sum()
            st.metric(
                "Cross-BU Consolidation Pool",
                f"${shared_spend:,.0f}",
                delta="Addressable via harmonization"
            )

st.divider()

# ============================================================
# RESULT: Supplier Network Visualization
# ============================================================
st.subheader("Supplier Network")
st.markdown("Supplier relationships across business units. Node size = inventory value, color = risk level.")

network_df = results["supplier_network"]
if not network_df.empty:
    # Build network graph
    G = nx.Graph()
    
    # Add region nodes
    regions = network_df["SUPPLIER_REGION"].unique()
    for region in regions:
        G.add_node(f"region_{region}", node_type="region", label=region)
    
    # Add supplier nodes with attributes
    for _, row in network_df.iterrows():
        supplier = row["SUPPLIER_NAME"]
        G.add_node(
            supplier,
            node_type="supplier",
            region=row["SUPPLIER_REGION"],
            bu=row["BUSINESS_UNIT"],
            tier=row["SUPPLIER_TIER"],
            value=row["INVENTORY_VALUE"],
            risk=row["AVG_RISK"],
            parts=row["PART_COUNT"]
        )
        # Connect supplier to region
        G.add_edge(f"region_{row['SUPPLIER_REGION']}", supplier)
    
    # Generate layout
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    
    # Create edge traces
    edge_x, edge_y = [], []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color="#64748b"),
        hoverinfo="none",
        mode="lines"
    )
    
    # Create node traces (suppliers only)
    supplier_nodes = [n for n in G.nodes() if G.nodes[n].get("node_type") == "supplier"]
    node_x = [pos[n][0] for n in supplier_nodes]
    node_y = [pos[n][1] for n in supplier_nodes]
    node_text = [n for n in supplier_nodes]
    node_sizes = [max(10, min(50, G.nodes[n].get("value", 10000) / 50000)) for n in supplier_nodes]
    node_colors = [G.nodes[n].get("risk", 0.5) for n in supplier_nodes]
    node_hover = [
        f"{n}<br>Tier: {G.nodes[n].get('tier', 'N/A')}<br>"
        f"Parts: {G.nodes[n].get('parts', 0)}<br>"
        f"Risk: {G.nodes[n].get('risk', 0.5):.2f}"
        for n in supplier_nodes
    ]
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode="markers+text",
        hoverinfo="text",
        hovertext=node_hover,
        text=node_text,
        textposition="top center",
        textfont=dict(size=8, color="#e2e8f0"),
        marker=dict(
            size=node_sizes,
            color=node_colors,
            colorscale=[
                [0, "#22c55e"],
                [0.5, "#f59e0b"],
                [1, "#ef4444"]
            ],
            colorbar=dict(title="Risk"),
            line=dict(width=1, color="#0f172a")
        )
    )
    
    # Region labels
    region_nodes = [n for n in G.nodes() if G.nodes[n].get("node_type") == "region"]
    region_x = [pos[n][0] for n in region_nodes]
    region_y = [pos[n][1] for n in region_nodes]
    region_text = [G.nodes[n].get("label", n) for n in region_nodes]
    
    region_trace = go.Scatter(
        x=region_x, y=region_y,
        mode="markers+text",
        text=region_text,
        textposition="bottom center",
        textfont=dict(size=12, color="#3b82f6", weight="bold"),
        marker=dict(size=20, color="#3b82f6", symbol="diamond"),
        hoverinfo="text",
        hovertext=[f"Region: {t}" for t in region_text]
    )
    
    fig = go.Figure(
        data=[edge_trace, node_trace, region_trace],
        layout=go.Layout(
            showlegend=False,
            hovermode="closest",
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            font=dict(color="#e2e8f0"),
            height=500,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            margin=dict(l=20, r=20, t=20, b=20)
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================
# Part Similarity Clusters
# ============================================================
st.subheader("Part Similarity Clusters")
st.markdown("t-SNE visualization of part embeddings. Clusters indicate consolidation candidates.")

cluster_df = results["clusters"]
if not cluster_df.empty:
    cluster_df["IS_DUPLICATE"] = cluster_df["IS_DUPLICATE"].astype(bool)
    
    fig = px.scatter(
        cluster_df,
        x="TSNE_X",
        y="TSNE_Y",
        color="PART_CATEGORY",
        symbol="IS_DUPLICATE",
        opacity=0.7,
        hover_data=["MATERIAL", "BUSINESS_UNIT"]
    )
    
    fig.update_layout(
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        font=dict(color="#e2e8f0"),
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02)
    )
    
    st.plotly_chart(fig, use_container_width=True)

# ============================================================
# Current State Metrics
# ============================================================
st.subheader("Current State Summary")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Spend", f"${total_spend:,.0f}")
col2.metric("Inventory Value", f"${kpi.get('TOTAL_INVENTORY_VALUE', 0):,.0f}")
col3.metric("Total SKUs", f"{total_skus:,}")
col4.metric("FDA Compliant Parts", f"{kpi.get('FDA_COMPLIANT_COUNT', 0):,}")

# ============================================================
# ASK THE ASSISTANT
# ============================================================
render_agent_panel(session, persona_context="vp")
