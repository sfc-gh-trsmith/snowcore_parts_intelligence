#!/bin/bash
set -e
set -o pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

error_exit() { echo -e "${RED}[ERROR] $1${NC}" >&2; exit 1; }
info() { echo -e "${GREEN}[INFO] $1${NC}"; }

CONNECTION_NAME="demo"
ENV_PREFIX=""
PROJECT_PREFIX="SNOWCORE_PARTS_INTELLIGENCE"
ONLY_COMPONENT=""

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--connection) CONNECTION_NAME="$2"; shift 2 ;;
        -p|--prefix) ENV_PREFIX="$2"; shift 2 ;;
        --only-sql) ONLY_COMPONENT="sql"; shift ;;
        --only-data) ONLY_COMPONENT="data"; shift ;;
        --only-streamlit) ONLY_COMPONENT="streamlit"; shift ;;
        --only-notebook) ONLY_COMPONENT="notebook"; shift ;;
        *) error_exit "Unknown argument: $1" ;;
    esac
done

SNOW_CONN="-c $CONNECTION_NAME"

if [ -n "$ENV_PREFIX" ]; then
    FULL_PREFIX="${ENV_PREFIX}_${PROJECT_PREFIX}"
else
    FULL_PREFIX="${PROJECT_PREFIX}"
fi

DATABASE="${FULL_PREFIX}"
ROLE="${FULL_PREFIX}_ROLE"
WAREHOUSE="${FULL_PREFIX}_WH"

CURRENT_USER=$(snow sql $SNOW_CONN -q "SELECT CURRENT_USER();" --format JSON 2>/dev/null | grep -o '"CURRENT_USER()": "[^"]*"' | cut -d'"' -f4)
[ -z "$CURRENT_USER" ] && error_exit "Could not determine current Snowflake user."

run_sql_file() {
    local file_path="$1"
    [ -f "$file_path" ] || error_exit "SQL file not found: $file_path"
    {
        echo "SET ROLE_NAME = '${ROLE}';"
        echo "SET WAREHOUSE_NAME = '${WAREHOUSE}';"
        echo "SET DATABASE_NAME = '${DATABASE}';"
        echo "SET MY_USER = '${CURRENT_USER}';"
        cat "$file_path"
    } | snow sql $SNOW_CONN -i
}

sql_exec() {
    local sql="$1"
    snow sql $SNOW_CONN -q "USE ROLE ${ROLE}; USE WAREHOUSE ${WAREHOUSE}; USE DATABASE ${DATABASE}; ${sql}"
}

if [[ -z "$ONLY_COMPONENT" || "$ONLY_COMPONENT" == "sql" ]]; then
    info "Deploying SQL objects..."
    run_sql_file "sql/01_setup.sql"
    run_sql_file "sql/02_tables.sql"
    run_sql_file "sql/03_cortex_search.sql"
    run_sql_file "sql/04_semantic_view.sql"
    run_sql_file "sql/05_cortex_agent.sql"
    run_sql_file "sql/06_synthetic_procedures.sql"
    
    info "Creating Semantic View from YAML..."
    # Create Semantic View using SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML
    # Write the full SQL to a temp file to preserve YAML formatting
    # Disable templating to avoid conflicts with YAML $ symbols
    # Note: verify_only=FALSE to actually create the view (TRUE is validation only)
    TEMP_SQL=$(mktemp)
    cat > "$TEMP_SQL" << EOSQL
CALL SYSTEM\$CREATE_SEMANTIC_VIEW_FROM_YAML(
  '${DATABASE}.DATA_SCIENCE',
  \$\$
$(cat "$SCRIPT_DIR/semantic_views/upip_semantic_model.yaml")
\$\$,
  FALSE
);
EOSQL
    snow sql $SNOW_CONN -f "$TEMP_SQL" --enable-templating NONE
    rm -f "$TEMP_SQL"
    
    info "Granting permissions on Semantic View..."
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; GRANT SELECT, REFERENCES ON SEMANTIC VIEW DATA_SCIENCE.UPIP_SEMANTIC_MODEL TO ROLE ${ROLE};"
    
    info "Deploying Cortex Agent..."
    # Agent SQL uses YAML with @ symbols that conflict with template rendering
    # Substitute variables manually and disable template processing
    AGENT_SQL=$(cat "sql/07_cortex_agent.sql" | \
        sed "s/\\\$DATABASE_NAME/${DATABASE}/g" | \
        sed "s/\\\$ROLE_NAME/${ROLE}/g" | \
        sed "s/\\\$WAREHOUSE_NAME/${WAREHOUSE}/g")
    echo "$AGENT_SQL" | snow sql $SNOW_CONN -i --enable-templating NONE
fi

if [[ -z "$ONLY_COMPONENT" || "$ONLY_COMPONENT" == "data" ]]; then
    info "Generating large synthetic datasets via stored procedures..."
    sql_exec "CALL DATA_SCIENCE.GENERATE_PLM_EXPORTS(20000);"
    sql_exec "CALL DATA_SCIENCE.GENERATE_PART_MASTER(20000);"
    sql_exec "CALL DATA_SCIENCE.GENERATE_SIMILARITY_SCORES();"

    info "Uploading small CSV datasets..."
    [ -d "$SCRIPT_DIR/data/synthetic" ] || error_exit "data/synthetic not found."
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; PUT file://$SCRIPT_DIR/data/synthetic/supplier_master.csv @RAW.DATA_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; PUT file://$SCRIPT_DIR/data/synthetic/engineering_docs.csv @RAW.DATA_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; PUT file://$SCRIPT_DIR/data/synthetic/purchase_orders.csv @RAW.DATA_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; PUT file://$SCRIPT_DIR/data/synthetic/supplier_risk_scores.csv @RAW.DATA_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; PUT file://$SCRIPT_DIR/data/synthetic/consolidation_scenarios.csv @RAW.DATA_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"

    info "Loading small CSV datasets..."
    sql_exec "TRUNCATE TABLE ATOMIC.SUPPLIER_MASTER;"
    sql_exec "COPY INTO ATOMIC.SUPPLIER_MASTER FROM @RAW.DATA_STAGE/supplier_master.csv.gz FILE_FORMAT=(FORMAT_NAME=RAW.CSV_FORMAT) ON_ERROR='ABORT_STATEMENT';"

    sql_exec "TRUNCATE TABLE RAW.ENGINEERING_DOCS;"
    sql_exec "COPY INTO RAW.ENGINEERING_DOCS (DOC_ID, PART_FAMILY, REGULATORY_STANDARD, DOC_TEXT, SOURCE_URI) FROM @RAW.DATA_STAGE/engineering_docs.csv.gz FILE_FORMAT=(FORMAT_NAME=RAW.CSV_FORMAT) ON_ERROR='ABORT_STATEMENT';"

    info "Loading procurement and analytics datasets..."
    sql_exec "TRUNCATE TABLE ATOMIC.PURCHASE_ORDERS;"
    sql_exec "COPY INTO ATOMIC.PURCHASE_ORDERS (PO_ID, PART_GLOBAL_ID, SUPPLIER_ID, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, PO_STATUS, CREATED_AT, APPROVED_AT, RECEIVED_AT, IS_MAVERICK) FROM @RAW.DATA_STAGE/purchase_orders.csv.gz FILE_FORMAT=(FORMAT_NAME=RAW.CSV_FORMAT) ON_ERROR='ABORT_STATEMENT';"

    sql_exec "TRUNCATE TABLE DATA_SCIENCE.SUPPLIER_RISK_SCORES;"
    sql_exec "COPY INTO DATA_SCIENCE.SUPPLIER_RISK_SCORES (SUPPLIER_ID, FINANCIAL_RISK, DELIVERY_RISK, QUALITY_RISK, COMPOSITE_RISK, SUPPLY_CONTINUITY) FROM @RAW.DATA_STAGE/supplier_risk_scores.csv.gz FILE_FORMAT=(FORMAT_NAME=RAW.CSV_FORMAT) ON_ERROR='ABORT_STATEMENT';"

    sql_exec "TRUNCATE TABLE DATA_SCIENCE.CONSOLIDATION_SCENARIOS;"
    sql_exec "COPY INTO DATA_SCIENCE.CONSOLIDATION_SCENARIOS (SCENARIO_ID, SCENARIO_NAME, SOURCE_SUPPLIERS, TARGET_SUPPLIER_ID, PARTS_AFFECTED, PROJECTED_SAVINGS, IMPLEMENTATION_COST, ROI_PCT, STATUS) FROM @RAW.DATA_STAGE/consolidation_scenarios.csv.gz FILE_FORMAT=(FORMAT_NAME=RAW.CSV_FORMAT) ON_ERROR='ABORT_STATEMENT';"

    info "Generating additional synthetic data..."
    sql_exec "CALL DATA_SCIENCE.GENERATE_PART_REUSE_EVENTS();"
fi

if [[ -z "$ONLY_COMPONENT" || "$ONLY_COMPONENT" == "streamlit" ]]; then
    info "Deploying Streamlit app..."
    cd streamlit
    snow streamlit deploy $SNOW_CONN --database "$DATABASE" --schema "DATA_SCIENCE" --role "$ROLE" --warehouse "$WAREHOUSE" --replace
    cd "$SCRIPT_DIR"
fi

if [[ -z "$ONLY_COMPONENT" || "$ONLY_COMPONENT" == "notebook" ]]; then
    info "Deploying notebook assets..."
    cd notebooks
    # Workaround: Upload notebook file manually before deploy (CLI bundling issue)
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; CREATE STAGE IF NOT EXISTS DATA_SCIENCE.notebooks;"
    snow sql $SNOW_CONN -q "USE DATABASE ${DATABASE}; PUT file://$SCRIPT_DIR/notebooks/part_similarity.ipynb @DATA_SCIENCE.notebooks/PART_SIMILARITY_NOTEBOOK AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
    snow notebook deploy $SNOW_CONN --database "$DATABASE" --schema "DATA_SCIENCE" --replace
    cd "$SCRIPT_DIR"
fi

info "Deploy completed."
