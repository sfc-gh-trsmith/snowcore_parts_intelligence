"""
Procurement Operations Dashboard
Target Persona: Procurement Manager (Operational Level)

STAR Flow:
- Situation: See maverick spend and price markups across suppliers
- Task: Identify savings opportunities and optimize procurement
- Action: Filter by category, compare suppliers, select alternatives
- Result: Quantified cost avoidance and savings estimates
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.agent import render_agent_panel
from utils.data_loader import run_queries_parallel
from utils.query_registry import register_query
from utils.snowflake import get_session

st.set_page_config(page_title="Procurement Ops", page_icon=":material/analytics:", layout="wide")

st.title("Procurement Operations")
st.caption("Identify maverick spend, price anomalies, and supplier optimization opportunities.")

session = get_session()

# ============================================================
# FILTERS AND TARGETS (Main Content)
# ============================================================
col_cat, col_tier, col_targets = st.columns([1, 1, 2])

with col_cat:
    category_filter = st.selectbox(
        "Part Category",
        options=["All", "Valve", "Motor", "Fastener", "Actuator", "Sensor", "Pump"],
        index=0,
    )

with col_tier:
    supplier_tier_filter = st.selectbox(
        "Supplier Tier",
        options=["All", "Preferred", "Approved", "Conditional"],
        index=0,
    )

with col_targets:
    st.info("**Procurement Targets:** Cost Avoidance: 15% | Maverick Spend: <10% | Contract Compliance: >90%")

st.divider()

# Build WHERE clauses based on filters
po_where_clauses = []
if category_filter != "All":
    po_where_clauses.append(f"PART_CATEGORY = '{category_filter}'")
if supplier_tier_filter != "All":
    po_where_clauses.append(f"SUPPLIER_TIER = '{supplier_tier_filter}'")

po_where = f"WHERE {' AND '.join(po_where_clauses)}" if po_where_clauses else ""

# Define queries
MAVERICK_KPI_SQL = register_query(
    f"maverick_kpi_{category_filter}_{supplier_tier_filter}",
    f"""
    SELECT
        SUM(TOTAL_AMOUNT) AS TOTAL_SPEND,
        SUM(CASE WHEN IS_MAVERICK THEN TOTAL_AMOUNT ELSE 0 END) AS MAVERICK_SPEND,
        COUNT(*) AS TOTAL_ORDERS,
        SUM(CASE WHEN IS_MAVERICK THEN 1 ELSE 0 END) AS MAVERICK_ORDERS,
        AVG(TOTAL_CYCLE_DAYS) AS AVG_CYCLE_DAYS
    FROM DATA_SCIENCE.PURCHASE_ORDERS_ANALYTICS
    {po_where}
    """,
    "Maverick spend KPIs",
)

MAVERICK_BY_SUPPLIER_SQL = register_query(
    f"maverick_by_supplier_{category_filter}_{supplier_tier_filter}",
    f"""
    SELECT
        SUPPLIER_NAME,
        SUPPLIER_TIER,
        SUM(TOTAL_AMOUNT) AS TOTAL_SPEND,
        SUM(CASE WHEN IS_MAVERICK THEN TOTAL_AMOUNT ELSE 0 END) AS MAVERICK_SPEND,
        ROUND(SUM(CASE WHEN IS_MAVERICK THEN TOTAL_AMOUNT ELSE 0 END) / 
              NULLIF(SUM(TOTAL_AMOUNT), 0) * 100, 1) AS MAVERICK_PCT,
        COUNT(*) AS ORDER_COUNT
    FROM DATA_SCIENCE.PURCHASE_ORDERS_ANALYTICS
    {po_where}
    GROUP BY SUPPLIER_NAME, SUPPLIER_TIER
    HAVING SUM(TOTAL_AMOUNT) > 0
    ORDER BY MAVERICK_SPEND DESC
    LIMIT 15
    """,
    "Maverick spend by supplier",
)

PRICE_ANOMALIES_SQL = register_query(
    f"price_anomalies_{category_filter}_{supplier_tier_filter}",
    f"""
    SELECT
        PART_NAME,
        PART_CATEGORY,
        SUPPLIER_NAME,
        UNIT_PRICE,
        BENCHMARK_COST,
        PRICE_VARIANCE_PCT,
        TOTAL_AMOUNT
    FROM DATA_SCIENCE.PURCHASE_ORDERS_ANALYTICS
    {po_where + ' AND' if po_where else 'WHERE'} PRICE_VARIANCE_PCT > 15
    ORDER BY PRICE_VARIANCE_PCT DESC
    LIMIT 20
    """,
    "Price anomalies above benchmark",
)

SUPPLIER_SCORECARD_SQL = register_query(
    f"supplier_scorecard_{supplier_tier_filter}",
    f"""
    SELECT
        SUPPLIER_NAME,
        SUPPLIER_TIER,
        RATING,
        AVG_LEAD_TIME_DAYS,
        COMPOSITE_RISK,
        SUPPLY_CONTINUITY,
        PART_COUNT,
        TOTAL_INVENTORY_VALUE,
        FDA_COMPLIANT_PARTS,
        QUALITY_CERTIFICATION
    FROM DATA_SCIENCE.SUPPLIER_SCORECARD
    {f"WHERE SUPPLIER_TIER = '{supplier_tier_filter}'" if supplier_tier_filter != "All" else ""}
    ORDER BY COMPOSITE_RISK ASC
    """,
    "Supplier scorecard with risk metrics",
)

SAVINGS_OPPORTUNITIES_SQL = register_query(
    f"savings_opportunities_{category_filter}",
    f"""
    WITH bio_parts AS (
        SELECT 
            p.PART_NAME,
            p.PART_CATEGORY,
            p.SUPPLIER_ID AS BIOFLUX_SUPPLIER_ID,
            s1.SUPPLIER_NAME AS BIOFLUX_SUPPLIER,
            p.COST AS BIOFLUX_COST,
            p.BENCHMARK_COST
        FROM ATOMIC.PART_MASTER p
        JOIN ATOMIC.SUPPLIER_MASTER s1 ON p.SUPPLIER_ID = s1.SUPPLIER_ID
        WHERE p.BUSINESS_UNIT = 'Bio-Tech'
        {f"AND p.PART_CATEGORY = '{category_filter}'" if category_filter != "All" else ""}
    ),
    industrial_matches AS (
        SELECT 
            bp.PART_NAME,
            bp.PART_CATEGORY,
            bp.BIOFLUX_SUPPLIER,
            bp.BIOFLUX_COST,
            pss.SIMILARITY_SCORE,
            p2.COST AS INDUSTRIAL_COST,
            s2.SUPPLIER_NAME AS INDUSTRIAL_SUPPLIER,
            ROUND(bp.BIOFLUX_COST - p2.COST, 2) AS SAVINGS_PER_UNIT,
            ROUND((bp.BIOFLUX_COST - p2.COST) / NULLIF(p2.COST, 0) * 100, 1) AS MARKUP_PCT
        FROM bio_parts bp
        JOIN DATA_SCIENCE.PART_SIMILARITY_SCORES pss
            ON bp.PART_NAME LIKE '%' || SUBSTRING(pss.SOURCE_GLOBAL_ID, 2, 5) || '%'
        JOIN ATOMIC.PART_MASTER p2
            ON pss.TARGET_GLOBAL_ID = p2.GLOBAL_ID
            AND p2.BUSINESS_UNIT = 'Industrial'
        JOIN ATOMIC.SUPPLIER_MASTER s2 ON p2.SUPPLIER_ID = s2.SUPPLIER_ID
        WHERE pss.SIMILARITY_SCORE > 85
          AND bp.BIOFLUX_COST > p2.COST
    )
    SELECT *
    FROM industrial_matches
    ORDER BY SAVINGS_PER_UNIT DESC
    LIMIT 15
    """,
    "Cross-BU savings opportunities",
)

# Run queries in parallel
results = run_queries_parallel(
    session,
    {
        "maverick_kpi": MAVERICK_KPI_SQL,
        "maverick_by_supplier": MAVERICK_BY_SUPPLIER_SQL,
        "price_anomalies": PRICE_ANOMALIES_SQL,
        "supplier_scorecard": SUPPLIER_SCORECARD_SQL,
    },
)

# ============================================================
# SITUATION: Current maverick spend and procurement metrics
# ============================================================
st.subheader("Procurement Health Metrics")

kpi = results["maverick_kpi"].iloc[0] if not results["maverick_kpi"].empty else {}
total_spend = kpi.get("TOTAL_SPEND", 0) or 0
maverick_spend = kpi.get("MAVERICK_SPEND", 0) or 0
maverick_pct = (maverick_spend / total_spend * 100) if total_spend > 0 else 0
total_orders = kpi.get("TOTAL_ORDERS", 0) or 0
maverick_orders = kpi.get("MAVERICK_ORDERS", 0) or 0
avg_cycle = kpi.get("AVG_CYCLE_DAYS", 0) or 0

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Procurement Spend",
    f"${total_spend:,.0f}",
    delta=f"{total_orders:,} orders"
)
col2.metric(
    "Maverick Spend",
    f"${maverick_spend:,.0f}",
    delta=f"{maverick_pct:.1f}% of total",
    delta_color="inverse"  # Red if high
)
col3.metric(
    "Contract Compliance",
    f"{100 - maverick_pct:.1f}%",
    delta="Target: >90%",
    delta_color="normal" if (100 - maverick_pct) >= 90 else "inverse"
)
col4.metric(
    "Avg Cycle Time",
    f"{avg_cycle:.1f} days",
    delta="Order to receipt"
)

st.divider()

# ============================================================
# TASK: Identify maverick spend by supplier
# ============================================================
st.subheader("Maverick Spend by Supplier")
st.markdown("Off-contract purchases by supplier. High maverick % indicates contract coverage gaps.")

maverick_df = results["maverick_by_supplier"]
if not maverick_df.empty:
    col_chart, col_table = st.columns([2, 1])
    
    with col_chart:
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name="Compliant Spend",
            x=maverick_df["SUPPLIER_NAME"],
            y=maverick_df["TOTAL_SPEND"] - maverick_df["MAVERICK_SPEND"],
            marker_color="#22c55e"
        ))
        
        fig.add_trace(go.Bar(
            name="Maverick Spend",
            x=maverick_df["SUPPLIER_NAME"],
            y=maverick_df["MAVERICK_SPEND"],
            marker_color="#ef4444"
        ))
        
        fig.update_layout(
            barmode="stack",
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            font=dict(color="#e2e8f0"),
            height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_tickangle=-45
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col_table:
        # Highlight high maverick suppliers
        def highlight_maverick(row):
            if row["Maverick %"] > 30:
                return ["background-color: #dc2626"] * len(row)
            elif row["Maverick %"] > 15:
                return ["background-color: #f59e0b"] * len(row)
            return [""] * len(row)
        
        display_df = maverick_df[["SUPPLIER_NAME", "SUPPLIER_TIER", "MAVERICK_PCT", "ORDER_COUNT"]].copy()
        display_df.columns = ["Supplier", "Tier", "Maverick %", "Orders"]
        
        st.dataframe(
            display_df.style.apply(highlight_maverick, axis=1),
            use_container_width=True,
            height=350
        )

st.divider()

# ============================================================
# ACTION: Price anomaly detection
# ============================================================
st.subheader("Price Anomalies")
st.markdown("Parts purchased above benchmark cost. Consider switching to preferred suppliers or renegotiating.")

anomalies_df = results["price_anomalies"]
if not anomalies_df.empty:
    # Summary metrics
    total_anomaly_spend = anomalies_df["TOTAL_AMOUNT"].sum()
    avg_variance = anomalies_df["PRICE_VARIANCE_PCT"].mean()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Anomalous Spend", f"${total_anomaly_spend:,.0f}")
    col2.metric("Avg Price Variance", f"+{avg_variance:.1f}%")
    col3.metric("Parts Affected", len(anomalies_df))
    
    # Scatter plot: variance vs spend
    fig = px.scatter(
        anomalies_df,
        x="PRICE_VARIANCE_PCT",
        y="TOTAL_AMOUNT",
        color="SUPPLIER_NAME",
        size="TOTAL_AMOUNT",
        hover_data=["PART_NAME", "UNIT_PRICE", "BENCHMARK_COST"],
        labels={
            "PRICE_VARIANCE_PCT": "Price Variance (%)",
            "TOTAL_AMOUNT": "Total Spend ($)"
        }
    )
    
    fig.update_layout(
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        font=dict(color="#e2e8f0"),
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    with st.expander("View Details", expanded=False):
        st.dataframe(anomalies_df, use_container_width=True)
else:
    st.info("No significant price anomalies detected with current filters.")

st.divider()

# ============================================================
# RESULT: Supplier Scorecard
# ============================================================
st.subheader("Supplier Scorecard")
st.markdown("Comprehensive supplier performance and risk metrics for sourcing decisions.")

scorecard_df = results["supplier_scorecard"]
if not scorecard_df.empty:
    # Risk heatmap
    fig = go.Figure(data=go.Heatmap(
        z=[scorecard_df["COMPOSITE_RISK"].tolist()],
        x=scorecard_df["SUPPLIER_NAME"].tolist(),
        y=["Risk Score"],
        colorscale=[
            [0, "#22c55e"],      # Low risk - green
            [0.3, "#22c55e"],
            [0.3, "#f59e0b"],    # Medium risk - yellow
            [0.6, "#f59e0b"],
            [0.6, "#ef4444"],    # High risk - red
            [1.0, "#ef4444"]
        ],
        showscale=True,
        colorbar=dict(title="Risk"),
        hovertemplate="Supplier: %{x}<br>Risk: %{z:.2f}<extra></extra>"
    ))
    
    fig.update_layout(
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        font=dict(color="#e2e8f0"),
        height=120,
        margin=dict(t=20, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Scorecard table
    display_cols = [
        "SUPPLIER_NAME", "SUPPLIER_TIER", "RATING", "AVG_LEAD_TIME_DAYS",
        "COMPOSITE_RISK", "SUPPLY_CONTINUITY", "PART_COUNT", "FDA_COMPLIANT_PARTS"
    ]
    display_df = scorecard_df[display_cols].copy()
    display_df.columns = [
        "Supplier", "Tier", "Rating", "Lead Time (days)",
        "Risk", "Continuity", "Parts", "FDA Parts"
    ]
    
    # Custom styling function (avoids matplotlib dependency)
    def style_risk_continuity(row):
        styles = [""] * len(row)
        risk_idx = display_df.columns.get_loc("Risk")
        cont_idx = display_df.columns.get_loc("Continuity")
        
        # Risk: higher is worse (red), lower is better (green)
        risk_val = row["Risk"]
        if risk_val >= 0.6:
            styles[risk_idx] = "background-color: #ef4444"
        elif risk_val >= 0.3:
            styles[risk_idx] = "background-color: #f59e0b"
        else:
            styles[risk_idx] = "background-color: #22c55e"
        
        # Continuity: higher is better (green), lower is worse (red)
        cont_val = row["Continuity"]
        if cont_val >= 0.7:
            styles[cont_idx] = "background-color: #22c55e"
        elif cont_val >= 0.4:
            styles[cont_idx] = "background-color: #f59e0b"
        else:
            styles[cont_idx] = "background-color: #ef4444"
        
        return styles
    
    st.dataframe(
        display_df.style.apply(style_risk_continuity, axis=1)
                       .format({"Risk": "{:.2f}", "Continuity": "{:.2f}", "Rating": "{:.1f}"}),
        use_container_width=True
    )

st.divider()

# ============================================================
# Savings Tracker
# ============================================================
st.subheader("Cost Avoidance Opportunities")
st.markdown("Cross-BU sourcing opportunities where Industrial alternatives cost less than Bio-Tech parts.")

try:
    savings_results = run_queries_parallel(session, {"savings": SAVINGS_OPPORTUNITIES_SQL})
    savings_df = savings_results["savings"]
    
    if not savings_df.empty:
        total_savings = savings_df["SAVINGS_PER_UNIT"].sum() * 100  # Assume avg 100 units
        avg_markup = savings_df["MARKUP_PCT"].mean()
        
        col1, col2 = st.columns(2)
        col1.metric("Potential Annual Savings", f"${total_savings:,.0f}")
        col2.metric("Avg Markup Eliminated", f"{avg_markup:.1f}%")
        
        st.dataframe(
            savings_df[["PART_NAME", "BIOFLUX_SUPPLIER", "BIOFLUX_COST", 
                       "INDUSTRIAL_SUPPLIER", "INDUSTRIAL_COST", "SAVINGS_PER_UNIT", "MARKUP_PCT"]],
            use_container_width=True
        )
    else:
        st.info("Run the similarity analysis to identify cross-BU savings opportunities.")
except Exception:
    st.info("Savings analysis requires similarity scores. Ensure ML pipeline has been executed.")

# ============================================================
# ASK THE ASSISTANT
# ============================================================
render_agent_panel(session, persona_context="procurement")
