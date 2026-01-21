-- Cortex Agent supporting objects (tool registry + cost function)

USE DATABASE IDENTIFIER($DATABASE_NAME);

CREATE TABLE IF NOT EXISTS DATA_SCIENCE.AGENT_TOOL_REGISTRY (
    TOOL_NAME STRING,
    TOOL_TYPE STRING,
    CONFIG VARIANT,
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE FUNCTION DATA_SCIENCE.CALCULATE_RETOOLING_COST(
    CURRENT_REGION STRING,
    TARGET_REGION STRING,
    PART_FAMILY STRING
)
RETURNS NUMBER(12, 2)
LANGUAGE SQL
AS
$$
    CASE
        WHEN CURRENT_REGION = TARGET_REGION THEN 2500.00
        WHEN PART_FAMILY ILIKE '%valve%' THEN 15000.00
        WHEN PART_FAMILY ILIKE '%motor%' THEN 22000.00
        ELSE 8000.00
    END
$$;

-- Assess supplier risk for agent tool calls
CREATE OR REPLACE FUNCTION DATA_SCIENCE.ASSESS_SUPPLIER_RISK(SUPPLIER_ID_PARAM STRING)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'supplier_id', s.SUPPLIER_ID,
        'supplier_name', s.SUPPLIER_NAME,
        'supplier_tier', s.SUPPLIER_TIER,
        'composite_risk', COALESCE(r.COMPOSITE_RISK, 0.5),
        'financial_risk', COALESCE(r.FINANCIAL_RISK, 0.5),
        'delivery_risk', COALESCE(r.DELIVERY_RISK, 0.5),
        'quality_risk', COALESCE(r.QUALITY_RISK, 0.5),
        'supply_continuity', COALESCE(r.SUPPLY_CONTINUITY, 0.5),
        'recommendation', CASE 
            WHEN COALESCE(r.COMPOSITE_RISK, 0.5) < 0.3 THEN 'Low risk - Recommended for strategic partnership'
            WHEN COALESCE(r.COMPOSITE_RISK, 0.5) < 0.5 THEN 'Medium risk - Acceptable with monitoring'
            WHEN COALESCE(r.COMPOSITE_RISK, 0.5) < 0.7 THEN 'Elevated risk - Consider alternatives or mitigation'
            ELSE 'High risk - Recommend supplier diversification'
        END
    )
    FROM ATOMIC.SUPPLIER_MASTER s
    LEFT JOIN DATA_SCIENCE.SUPPLIER_RISK_SCORES r
        ON s.SUPPLIER_ID = r.SUPPLIER_ID
    WHERE s.SUPPLIER_ID = SUPPLIER_ID_PARAM
$$;

-- Recommend suppliers based on category and requirements
CREATE OR REPLACE FUNCTION DATA_SCIENCE.RECOMMEND_SUPPLIER(
    PART_CATEGORY_PARAM STRING,
    MIN_RATING NUMBER,
    MAX_LEAD_TIME NUMBER
)
RETURNS TABLE (
    SUPPLIER_ID STRING,
    SUPPLIER_NAME STRING,
    SUPPLIER_TIER STRING,
    RATING NUMBER,
    LEAD_TIME NUMBER,
    COMPOSITE_RISK NUMBER,
    RECOMMENDATION_SCORE NUMBER,
    RATIONALE STRING
)
LANGUAGE SQL
AS
$$
    SELECT
        s.SUPPLIER_ID,
        s.SUPPLIER_NAME,
        s.SUPPLIER_TIER,
        s.RATING,
        s.AVG_LEAD_TIME_DAYS AS LEAD_TIME,
        COALESCE(r.COMPOSITE_RISK, 0.5) AS COMPOSITE_RISK,
        -- Recommendation score: weighted combination of rating, lead time, and risk
        ROUND(
            (s.RATING / 5.0) * 40 +
            (1 - LEAST(s.AVG_LEAD_TIME_DAYS, 30) / 30.0) * 30 +
            (1 - COALESCE(r.COMPOSITE_RISK, 0.5)) * 30,
            1
        ) AS RECOMMENDATION_SCORE,
        CASE
            WHEN s.SUPPLIER_TIER = 'Preferred' THEN 'Preferred supplier with proven track record'
            WHEN COALESCE(r.COMPOSITE_RISK, 0.5) < 0.3 THEN 'Low risk supplier meeting all criteria'
            WHEN s.RATING >= 4.5 THEN 'High-rated supplier with excellent quality'
            ELSE 'Meets minimum requirements'
        END AS RATIONALE
    FROM ATOMIC.SUPPLIER_MASTER s
    LEFT JOIN DATA_SCIENCE.SUPPLIER_RISK_SCORES r
        ON s.SUPPLIER_ID = r.SUPPLIER_ID
    WHERE s.RATING >= MIN_RATING
      AND s.AVG_LEAD_TIME_DAYS <= MAX_LEAD_TIME
    ORDER BY RECOMMENDATION_SCORE DESC
    LIMIT 5
$$;

-- Get consolidation scenario details
CREATE OR REPLACE FUNCTION DATA_SCIENCE.GET_CONSOLIDATION_SCENARIO(SCENARIO_ID_PARAM STRING)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'scenario_id', c.SCENARIO_ID,
        'scenario_name', c.SCENARIO_NAME,
        'source_suppliers', c.SOURCE_SUPPLIERS,
        'target_supplier', OBJECT_CONSTRUCT(
            'id', s.SUPPLIER_ID,
            'name', s.SUPPLIER_NAME,
            'tier', s.SUPPLIER_TIER,
            'risk', COALESCE(r.COMPOSITE_RISK, 0.5)
        ),
        'parts_affected', c.PARTS_AFFECTED,
        'projected_savings', c.PROJECTED_SAVINGS,
        'implementation_cost', c.IMPLEMENTATION_COST,
        'roi_pct', c.ROI_PCT,
        'net_benefit', c.PROJECTED_SAVINGS - c.IMPLEMENTATION_COST,
        'status', c.STATUS
    )
    FROM DATA_SCIENCE.CONSOLIDATION_SCENARIOS c
    LEFT JOIN ATOMIC.SUPPLIER_MASTER s
        ON c.TARGET_SUPPLIER_ID = s.SUPPLIER_ID
    LEFT JOIN DATA_SCIENCE.SUPPLIER_RISK_SCORES r
        ON c.TARGET_SUPPLIER_ID = r.SUPPLIER_ID
    WHERE c.SCENARIO_ID = SCENARIO_ID_PARAM
$$;

MERGE INTO DATA_SCIENCE.AGENT_TOOL_REGISTRY t
USING (
    SELECT
        'cortex_analyst' AS TOOL_NAME,
        'analyst' AS TOOL_TYPE,
        OBJECT_CONSTRUCT('semantic_model', 'semantic_views/upip_semantic_model.yaml') AS CONFIG
    UNION ALL
    SELECT
        'cortex_search' AS TOOL_NAME,
        'search' AS TOOL_TYPE,
        OBJECT_CONSTRUCT('service', 'ENGINEERING_DOCS_SEARCH') AS CONFIG
    UNION ALL
    SELECT
        'calculate_retooling_cost' AS TOOL_NAME,
        'function' AS TOOL_TYPE,
        OBJECT_CONSTRUCT('handler', 'CALCULATE_RETOOLING_COST') AS CONFIG
    UNION ALL
    SELECT
        'assess_supplier_risk' AS TOOL_NAME,
        'function' AS TOOL_TYPE,
        OBJECT_CONSTRUCT('handler', 'ASSESS_SUPPLIER_RISK') AS CONFIG
    UNION ALL
    SELECT
        'recommend_supplier' AS TOOL_NAME,
        'function' AS TOOL_TYPE,
        OBJECT_CONSTRUCT('handler', 'RECOMMEND_SUPPLIER') AS CONFIG
    UNION ALL
    SELECT
        'get_consolidation_scenario' AS TOOL_NAME,
        'function' AS TOOL_TYPE,
        OBJECT_CONSTRUCT('handler', 'GET_CONSOLIDATION_SCENARIO') AS CONFIG
) s
ON t.TOOL_NAME = s.TOOL_NAME
WHEN MATCHED THEN UPDATE SET
    t.TOOL_TYPE = s.TOOL_TYPE,
    t.CONFIG = s.CONFIG,
    t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (TOOL_NAME, TOOL_TYPE, CONFIG) VALUES (s.TOOL_NAME, s.TOOL_TYPE, s.CONFIG);
