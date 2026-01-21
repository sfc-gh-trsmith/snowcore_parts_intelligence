-- Unified Cortex Agent: SOURCING_ASSISTANT
-- Serves VP (strategic), Procurement Manager (operational), and R&D Engineer (technical) personas

USE DATABASE $DATABASE_NAME;

-- Note: Semantic View is created via deploy.sh using SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML

-- Create the unified Sourcing Assistant agent
CREATE OR REPLACE AGENT DATA_SCIENCE.SOURCING_ASSISTANT
  COMMENT = 'Sourcing Assistant for UPIP - serves VP, Procurement Manager, and R&D Engineer personas'
  FROM SPECIFICATION
$$
models:
  orchestration: auto

instructions:
  system: |
    You are a Sourcing Assistant for the Unified Parts Intelligence Platform (UPIP).
    Help users with parts inventory, supplier performance, procurement decisions, and regulatory compliance.
  response: |
    Provide clear, data-driven answers. When discussing costs, include currency (USD).
    For compliance questions, cite relevant regulatory standards.

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: UPIP_ANALYTICS
      description: >
        Query parts inventory, supplier performance, procurement spend, and KPIs using natural language.
        Use for questions about costs, quantities, inventory values, compliance rates, savings projections,
        maverick spend, supplier metrics, consolidation scenarios, and part reuse statistics.

  - tool_spec:
      type: cortex_search
      name: ENGINEERING_DOCS
      description: >
        Search engineering documents, compliance certifications, SOPs, and regulatory requirements.
        Use for questions about FDA compliance, ISO standards, biocompatibility requirements,
        technical specifications, audit trails, and regulatory frameworks.

  - tool_spec:
      type: generic
      name: CALCULATE_RETOOLING_COST
      description: >
        Estimate the one-time cost of switching suppliers based on geographic region and part type.
        Use when evaluating supplier consolidation scenarios or comparing switching costs.
      input_schema:
        type: object
        properties:
          CURRENT_REGION:
            type: string
            description: Current supplier region (NA, EU, APAC)
          TARGET_REGION:
            type: string
            description: Target supplier region (NA, EU, APAC)
          PART_FAMILY:
            type: string
            description: Part category (valve, motor, fastener, actuator, sensor, pump)
        required:
          - CURRENT_REGION
          - TARGET_REGION
          - PART_FAMILY

  - tool_spec:
      type: generic
      name: ASSESS_SUPPLIER_RISK
      description: >
        Get comprehensive risk assessment for a supplier including financial, delivery, and quality risk scores.
        Use when evaluating supplier alternatives or assessing supply chain risk.
      input_schema:
        type: object
        properties:
          SUPPLIER_ID_PARAM:
            type: string
            description: Supplier ID (e.g., SUP001)
        required:
          - SUPPLIER_ID_PARAM

  - tool_spec:
      type: generic
      name: GET_CONSOLIDATION_SCENARIO
      description: >
        Get details of a supplier consolidation scenario including projected savings, ROI, and implementation costs.
        Use when discussing strategic consolidation initiatives.
      input_schema:
        type: object
        properties:
          SCENARIO_ID_PARAM:
            type: string
            description: Scenario ID (e.g., CONS001)
        required:
          - SCENARIO_ID_PARAM

tool_resources:
  UPIP_ANALYTICS:
    semantic_view: "DATA_SCIENCE.UPIP_SEMANTIC_MODEL"
    execution_environment:
      type: warehouse
      warehouse: $WAREHOUSE_NAME
      query_timeout: 300
  ENGINEERING_DOCS:
    name: DATA_SCIENCE.ENGINEERING_DOCS_SEARCH
    id_column: DOC_ID
    title_column: PART_FAMILY
    max_results: 5
  CALCULATE_RETOOLING_COST:
    type: function
    identifier: DATA_SCIENCE.CALCULATE_RETOOLING_COST
  ASSESS_SUPPLIER_RISK:
    type: function
    identifier: DATA_SCIENCE.ASSESS_SUPPLIER_RISK
  GET_CONSOLIDATION_SCENARIO:
    type: function
    identifier: DATA_SCIENCE.GET_CONSOLIDATION_SCENARIO
$$;

-- Grant usage on the agent
GRANT USAGE ON AGENT DATA_SCIENCE.SOURCING_ASSISTANT TO ROLE PUBLIC;
