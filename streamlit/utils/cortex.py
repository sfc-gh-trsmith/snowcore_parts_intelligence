from __future__ import annotations

from utils.query_registry import register_query


def run_analyst_query(session, prompt: str):
    prompt_lower = prompt.lower()
    if "inventory value" in prompt_lower and "stainless" in prompt_lower:
        sql = register_query(
            "analyst_inventory_value",
            """
            SELECT BUSINESS_UNIT, SUM(INVENTORY_VALUE) AS TOTAL_INV_VALUE
            FROM DATA_SCIENCE.PARTS_ANALYTICS
            WHERE IS_DUPLICATE = TRUE AND MATERIAL = 'Stainless Steel'
            GROUP BY BUSINESS_UNIT
            """,
            "Golden query for inventory value of duplicate stainless parts",
        )
    else:
        sql = register_query(
            "analyst_default",
            """
            SELECT BUSINESS_UNIT, SUM(INVENTORY_VALUE) AS INVENTORY_VALUE
            FROM DATA_SCIENCE.PARTS_ANALYTICS
            GROUP BY BUSINESS_UNIT
            """,
            "Fallback analyst query for inventory value by business unit",
        )

    return session.sql(sql).to_pandas()


def run_cortex_search(session, query: str, top_k: int = 3):
    safe_query = query.replace("'", "''").strip()
    sql = register_query(
        f"cortex_search_{abs(hash((safe_query, top_k)))}",
        f"""
        SELECT *
        FROM TABLE(DATA_SCIENCE.ENGINEERING_DOCS_SEARCH(
            QUERY => '{safe_query}',
            TOP_K => {top_k}
        ))
        """,
        "Cortex Search query over engineering docs",
    )
    return session.sql(sql).to_pandas()
