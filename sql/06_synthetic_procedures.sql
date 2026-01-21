-- Stored procedures for deterministic synthetic data (>10K rows)

USE DATABASE IDENTIFIER($DATABASE_NAME);

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_PLM_EXPORTS(ROW_COUNT NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE RAW.PLM_EXPORTS;

    LET insert_sql := '
    INSERT INTO RAW.PLM_EXPORTS (LOCAL_ID, SOURCE_SYSTEM, PART_DESCRIPTION, MATERIAL, WEIGHT, DIMENSIONS_JSON, COST, SUPPLIER_ID, BUSINESS_UNIT, COMPLIANCE_STATUS)
    WITH base AS (
        SELECT SEQ4() AS SEQ
        FROM TABLE(GENERATOR(ROWCOUNT => ' || ROW_COUNT || '))
    )
    SELECT
        ''L'' || LPAD(SEQ::STRING, 8, ''0'') AS LOCAL_ID,
        CASE WHEN MOD(SEQ, 2) = 0 THEN ''WINDCHILL'' ELSE ''SOLIDWORKS_PDM'' END AS SOURCE_SYSTEM,
        ''Precision valve assembly '' || MOD(ABS(HASH(42, SEQ)), 500)::STRING AS PART_DESCRIPTION,
        CASE MOD(ABS(HASH(42, SEQ)), 4)
            WHEN 0 THEN ''Stainless Steel''
            WHEN 1 THEN ''Aluminum''
            WHEN 2 THEN ''Titanium''
            ELSE ''Polymer''
        END AS MATERIAL,
        (10 + MOD(ABS(HASH(42, SEQ)), 900)) / 100 AS WEIGHT,
        OBJECT_CONSTRUCT(
            ''length_mm'', 20 + MOD(ABS(HASH(42, SEQ)), 180),
            ''diameter_mm'', 4 + MOD(ABS(HASH(42, SEQ)), 30),
            ''tolerance_mm'', 0.01 + (MOD(ABS(HASH(42, SEQ)), 10) / 1000)
        ) AS DIMENSIONS_JSON,
        (25 + MOD(ABS(HASH(42, SEQ)), 5000)) / 10 AS COST,
        ''SUP'' || LPAD((1 + MOD(ABS(HASH(42, SEQ)), 50))::STRING, 3, ''0'') AS SUPPLIER_ID,
        CASE WHEN MOD(ABS(HASH(42, SEQ)), 2) = 0 THEN ''Industrial'' ELSE ''Bio-Tech'' END AS BUSINESS_UNIT,
        CASE WHEN MOD(ABS(HASH(42, SEQ)), 5) = 0 THEN ''FDA Approved'' ELSE ''Pending'' END AS COMPLIANCE_STATUS
    FROM base';

    EXECUTE IMMEDIATE :insert_sql;

    RETURN 'PLM_EXPORTS generated';
END;
$$;

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_PART_MASTER(ROW_COUNT NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE ATOMIC.PART_MASTER;

    LET insert_sql := '
    INSERT INTO ATOMIC.PART_MASTER (GLOBAL_ID, LOCAL_ID, SOURCE_SYSTEM, PART_NAME, PART_DESCRIPTION, MATERIAL, WEIGHT, DIMENSIONS_JSON, COST, SUPPLIER_ID, BUSINESS_UNIT, INVENTORY_QUANTITY, INVENTORY_VALUE, COMPLIANCE_STATUS, IS_DUPLICATE, PART_CATEGORY, BENCHMARK_COST)
    WITH base AS (
        SELECT SEQ4() AS SEQ
        FROM TABLE(GENERATOR(ROWCOUNT => ' || ROW_COUNT || '))
    )
    SELECT
        ''G'' || LPAD(SEQ::STRING, 9, ''0'') AS GLOBAL_ID,
        ''L'' || LPAD(SEQ::STRING, 8, ''0'') AS LOCAL_ID,
        CASE WHEN MOD(SEQ, 2) = 0 THEN ''WINDCHILL'' ELSE ''SOLIDWORKS_PDM'' END AS SOURCE_SYSTEM,
        CASE MOD(ABS(HASH(42, SEQ)), 6)
            WHEN 0 THEN ''Valve Assembly ''
            WHEN 1 THEN ''Motor Unit ''
            WHEN 2 THEN ''Hex Fastener ''
            WHEN 3 THEN ''Linear Actuator ''
            WHEN 4 THEN ''Pressure Sensor ''
            ELSE ''Fluid Pump ''
        END || MOD(ABS(HASH(42, SEQ)), 500)::STRING AS PART_NAME,
        ''High-precision component for bioprocessing and industrial applications'' AS PART_DESCRIPTION,
        CASE MOD(ABS(HASH(42, SEQ)), 4)
            WHEN 0 THEN ''Stainless Steel''
            WHEN 1 THEN ''Aluminum''
            WHEN 2 THEN ''Titanium''
            ELSE ''Polymer''
        END AS MATERIAL,
        (10 + MOD(ABS(HASH(42, SEQ)), 900)) / 100 AS WEIGHT,
        OBJECT_CONSTRUCT(
            ''length_mm'', 20 + MOD(ABS(HASH(42, SEQ)), 180),
            ''diameter_mm'', 4 + MOD(ABS(HASH(42, SEQ)), 30),
            ''tolerance_mm'', 0.01 + (MOD(ABS(HASH(42, SEQ)), 10) / 1000)
        ) AS DIMENSIONS_JSON,
        (25 + MOD(ABS(HASH(42, SEQ)), 5000)) / 10 AS COST,
        ''SUP'' || LPAD((1 + MOD(ABS(HASH(42, SEQ)), 12))::STRING, 3, ''0'') AS SUPPLIER_ID,
        CASE WHEN MOD(ABS(HASH(42, SEQ)), 2) = 0 THEN ''Industrial'' ELSE ''Bio-Tech'' END AS BUSINESS_UNIT,
        (100 + MOD(ABS(HASH(42, SEQ)), 5000))::NUMBER AS INVENTORY_QUANTITY,
        ((100 + MOD(ABS(HASH(42, SEQ)), 5000)) * (25 + MOD(ABS(HASH(42, SEQ)), 5000)) / 10) AS INVENTORY_VALUE,
        CASE WHEN MOD(ABS(HASH(42, SEQ)), 5) = 0 THEN ''FDA Approved'' ELSE ''Pending'' END AS COMPLIANCE_STATUS,
        CASE WHEN MOD(ABS(HASH(42, SEQ)), 6) = 0 THEN TRUE ELSE FALSE END AS IS_DUPLICATE,
        -- PART_CATEGORY matches the part name pattern
        CASE MOD(ABS(HASH(42, SEQ)), 6)
            WHEN 0 THEN ''Valve''
            WHEN 1 THEN ''Motor''
            WHEN 2 THEN ''Fastener''
            WHEN 3 THEN ''Actuator''
            WHEN 4 THEN ''Sensor''
            ELSE ''Pump''
        END AS PART_CATEGORY,
        -- BENCHMARK_COST is 80-95% of actual cost (should-cost reference)
        ROUND((25 + MOD(ABS(HASH(42, SEQ)), 5000)) / 10 * (0.80 + MOD(ABS(HASH(42, SEQ)), 15) / 100), 2) AS BENCHMARK_COST
    FROM base';

    EXECUTE IMMEDIATE :insert_sql;

    RETURN 'PART_MASTER generated';
END;
$$;

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_SIMILARITY_SCORES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE DATA_SCIENCE.PART_SIMILARITY_SCORES;

    INSERT INTO DATA_SCIENCE.PART_SIMILARITY_SCORES (SOURCE_GLOBAL_ID, TARGET_GLOBAL_ID, SIMILARITY_SCORE, MATCH_REASON)
    WITH clustered AS (
        SELECT
            GLOBAL_ID,
            MOD(ABS(HASH(42, GLOBAL_ID)), 200) AS CLUSTER_ID
        FROM ATOMIC.PART_MASTER
    ),
    pairs AS (
        SELECT
            c1.GLOBAL_ID AS SOURCE_GLOBAL_ID,
            c2.GLOBAL_ID AS TARGET_GLOBAL_ID,
            90 + MOD(ABS(HASH(42, c1.GLOBAL_ID, c2.GLOBAL_ID)), 10) AS SIMILARITY_SCORE,
            'Clustered by synthetic hash group' AS MATCH_REASON
        FROM clustered c1
        JOIN clustered c2
            ON c1.CLUSTER_ID = c2.CLUSTER_ID
           AND c1.GLOBAL_ID <> c2.GLOBAL_ID
    )
    SELECT *
    FROM pairs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_GLOBAL_ID ORDER BY SIMILARITY_SCORE DESC) <= 3;

    RETURN 'SIMILARITY_SCORES generated';
END;
$$;

-- ============================================================
-- PROCUREMENT & ANALYTICS PROCEDURES (Multi-Persona Support)
-- ============================================================

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_PURCHASE_ORDERS(ROW_COUNT NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE ATOMIC.PURCHASE_ORDERS;

    LET insert_sql := '
    INSERT INTO ATOMIC.PURCHASE_ORDERS (PO_ID, PART_GLOBAL_ID, SUPPLIER_ID, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, PO_STATUS, CREATED_AT, APPROVED_AT, RECEIVED_AT, IS_MAVERICK)
    WITH base AS (
        SELECT SEQ4() AS SEQ
        FROM TABLE(GENERATOR(ROWCOUNT => ' || ROW_COUNT || '))
    ),
    raw_orders AS (
        SELECT
            ''PO'' || LPAD(SEQ::STRING, 6, ''0'') AS PO_ID,
            ''G'' || LPAD(MOD(SEQ, 1000)::STRING, 9, ''0'') AS PART_GLOBAL_ID,
            ''SUP'' || LPAD((1 + MOD(ABS(HASH(42, SEQ)), 12))::STRING, 3, ''0'') AS SUPPLIER_ID,
            (10 + MOD(ABS(HASH(42, SEQ)), 490))::NUMBER AS QUANTITY,
            ROUND((15 + MOD(ABS(HASH(42, SEQ)), 435)) + (MOD(ABS(HASH(42, SEQ)), 100) / 100), 2) AS UNIT_PRICE,
            -- Status: 60% received, 25% approved, 15% draft
            CASE
                WHEN MOD(ABS(HASH(42, SEQ)), 100) < 60 THEN ''received''
                WHEN MOD(ABS(HASH(42, SEQ)), 100) < 85 THEN ''approved''
                ELSE ''draft''
            END AS PO_STATUS,
            -- Created date: spread over 350 days from 2025-01-01
            DATEADD(day, MOD(ABS(HASH(42, SEQ)), 350), ''2025-01-01''::DATE) AS CREATED_AT,
            SEQ
        FROM base
    )
    SELECT
        PO_ID,
        PART_GLOBAL_ID,
        SUPPLIER_ID,
        QUANTITY,
        UNIT_PRICE,
        ROUND(QUANTITY * UNIT_PRICE, 2) AS TOTAL_AMOUNT,
        PO_STATUS,
        CREATED_AT,
        -- Approval: 1-5 days after creation (null if draft)
        CASE WHEN PO_STATUS IN (''approved'', ''received'')
             THEN DATEADD(day, 1 + MOD(ABS(HASH(42, SEQ, 1)), 5), CREATED_AT)
             ELSE NULL END AS APPROVED_AT,
        -- Received: 7-45 days after approval (null if not received)
        CASE WHEN PO_STATUS = ''received''
             THEN DATEADD(day, 7 + MOD(ABS(HASH(42, SEQ, 2)), 38),
                         DATEADD(day, 1 + MOD(ABS(HASH(42, SEQ, 1)), 5), CREATED_AT))
             ELSE NULL END AS RECEIVED_AT,
        -- Maverick: 15% overall, higher for non-preferred suppliers (SUP003,05,07,10,12)
        CASE
            WHEN SUPPLIER_ID IN (''SUP003'', ''SUP005'', ''SUP007'', ''SUP010'', ''SUP012'')
                 AND MOD(ABS(HASH(42, SEQ, 3)), 100) < 35 THEN TRUE
            WHEN MOD(ABS(HASH(42, SEQ, 3)), 100) < 5 THEN TRUE
            ELSE FALSE
        END AS IS_MAVERICK
    FROM raw_orders';

    EXECUTE IMMEDIATE :insert_sql;

    RETURN 'PURCHASE_ORDERS generated';
END;
$$;

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_SUPPLIER_RISK_SCORES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE DATA_SCIENCE.SUPPLIER_RISK_SCORES;

    -- Risk scores based on supplier characteristics
    -- Preferred suppliers get lower risk, higher continuity
    INSERT INTO DATA_SCIENCE.SUPPLIER_RISK_SCORES
        (SUPPLIER_ID, FINANCIAL_RISK, DELIVERY_RISK, QUALITY_RISK, COMPOSITE_RISK, SUPPLY_CONTINUITY)
    SELECT
        s.SUPPLIER_ID,
        -- Financial risk: inverse of rating + spend stability
        ROUND(GREATEST(0.05, 1.0 - (s.RATING / 5.0) - (CASE WHEN s.PREFERRED_FLAG THEN 0.15 ELSE 0 END) + (RANDOM() * 0.1)), 2) AS FINANCIAL_RISK,
        -- Delivery risk: based on lead time
        ROUND(GREATEST(0.05, (s.AVG_LEAD_TIME_DAYS / 50.0) - (CASE WHEN s.PREFERRED_FLAG THEN 0.10 ELSE 0 END) + (RANDOM() * 0.1)), 2) AS DELIVERY_RISK,
        -- Quality risk: inverse of rating
        ROUND(GREATEST(0.05, 1.0 - (s.RATING / 5.0) + (RANDOM() * 0.08)), 2) AS QUALITY_RISK,
        -- Composite: weighted average
        ROUND(GREATEST(0.05, (
            (1.0 - (s.RATING / 5.0) - (CASE WHEN s.PREFERRED_FLAG THEN 0.15 ELSE 0 END)) * 0.3 +
            (s.AVG_LEAD_TIME_DAYS / 50.0 - (CASE WHEN s.PREFERRED_FLAG THEN 0.10 ELSE 0 END)) * 0.3 +
            (1.0 - (s.RATING / 5.0)) * 0.4
        ) + (RANDOM() * 0.05)), 2) AS COMPOSITE_RISK,
        -- Supply continuity: higher for preferred, scales with rating
        ROUND(LEAST(0.99, (s.RATING / 5.0) + (CASE WHEN s.PREFERRED_FLAG THEN 0.15 ELSE 0 END) - (RANDOM() * 0.05)), 2) AS SUPPLY_CONTINUITY
    FROM ATOMIC.SUPPLIER_MASTER s;

    RETURN 'SUPPLIER_RISK_SCORES generated';
END;
$$;

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_CONSOLIDATION_SCENARIOS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE DATA_SCIENCE.CONSOLIDATION_SCENARIOS;

    -- Pre-defined consolidation scenarios for VP dashboard
    INSERT INTO DATA_SCIENCE.CONSOLIDATION_SCENARIOS
        (SCENARIO_ID, SCENARIO_NAME, SOURCE_SUPPLIERS, TARGET_SUPPLIER_ID, PARTS_AFFECTED, PROJECTED_SAVINGS, IMPLEMENTATION_COST, ROI_PCT, STATUS)
    VALUES
        ('CONS001', 'NA Fastener Consolidation', ARRAY_CONSTRUCT('SUP003', 'SUP010'), 'SUP001', 145, 285000.00, 45000.00, 533.33, 'proposed'),
        ('CONS002', 'EU BioTech Supplier Merge', ARRAY_CONSTRUCT('SUP005', 'SUP007'), 'SUP002', 89, 178000.00, 32000.00, 456.25, 'approved'),
        ('CONS003', 'APAC Motor Standardization', ARRAY_CONSTRUCT('SUP003', 'SUP012'), 'SUP008', 112, 156000.00, 28000.00, 457.14, 'in_progress'),
        ('CONS004', 'Premium Valve Consolidation', ARRAY_CONSTRUCT('SUP005', 'SUP010'), 'SUP011', 67, 198000.00, 55000.00, 260.00, 'proposed'),
        ('CONS005', 'Industrial Metals Optimization', ARRAY_CONSTRUCT('SUP003'), 'SUP004', 234, 312000.00, 62000.00, 403.23, 'completed'),
        ('CONS006', 'Cross-BU Actuator Alliance', ARRAY_CONSTRUCT('SUP005', 'SUP007', 'SUP010'), 'SUP006', 156, 425000.00, 85000.00, 400.00, 'proposed');

    RETURN 'CONSOLIDATION_SCENARIOS generated';
END;
$$;

CREATE OR REPLACE PROCEDURE DATA_SCIENCE.GENERATE_PART_REUSE_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE DATA_SCIENCE.PART_REUSE_EVENTS;

    -- Generate sample reuse events
    INSERT INTO DATA_SCIENCE.PART_REUSE_EVENTS
        (EVENT_ID, PART_GLOBAL_ID, PROJECT_NAME, DESIGN_TIME_SAVED_HOURS, COST_AVOIDED)
    SELECT
        'RE' || LPAD(SEQ4()::STRING, 6, '0') AS EVENT_ID,
        'G' || LPAD(MOD(ABS(HASH(42, SEQ4())), 500)::STRING, 9, '0') AS PART_GLOBAL_ID,
        CASE MOD(ABS(HASH(42, SEQ4())), 5)
            WHEN 0 THEN 'BioReactor Redesign'
            WHEN 1 THEN 'Industrial Pump System'
            WHEN 2 THEN 'Automated Cell Culture'
            WHEN 3 THEN 'Precision Valve Station'
            ELSE 'Motor Control Module'
        END AS PROJECT_NAME,
        ROUND(8 + (MOD(ABS(HASH(42, SEQ4())), 72)), 1) AS DESIGN_TIME_SAVED_HOURS,
        ROUND(500 + (MOD(ABS(HASH(42, SEQ4())), 4500)), 2) AS COST_AVOIDED
    FROM TABLE(GENERATOR(ROWCOUNT => 50));

    RETURN 'PART_REUSE_EVENTS generated';
END;
$$;
