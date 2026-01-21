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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

COMMAND="${1:-main}"
shift || true

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--connection) CONNECTION_NAME="$2"; shift 2 ;;
        -p|--prefix) ENV_PREFIX="$2"; shift 2 ;;
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

sql_exec() {
    local sql="$1"
    snow sql $SNOW_CONN -q "USE ROLE ${ROLE}; USE WAREHOUSE ${WAREHOUSE}; USE DATABASE ${DATABASE}; ${sql}"
}

cmd_main() {
    info "Executing part similarity notebook..."
    snow notebook execute $SNOW_CONN PART_SIMILARITY_NOTEBOOK --database "$DATABASE" --schema "DATA_SCIENCE"
    info "Notebook execution completed."
}

cmd_test() {
    info "Running demo validation queries..."
    sql_exec "SELECT COUNT(*) AS PARTS FROM ATOMIC.PART_MASTER;"
    sql_exec "SELECT COUNT(*) AS SUPPLIERS FROM ATOMIC.SUPPLIER_MASTER;"
    sql_exec "SELECT COUNT(*) AS DOCS FROM RAW.ENGINEERING_DOCS;"
    sql_exec "SELECT COUNT(*) AS SIMILARITIES FROM DATA_SCIENCE.PART_SIMILARITY_SCORES;"
    sql_exec "SELECT BUSINESS_UNIT, SUM(INVENTORY_VALUE) AS TOTAL_INV_VALUE FROM DATA_SCIENCE.PARTS_ANALYTICS WHERE IS_DUPLICATE = TRUE AND MATERIAL = 'Stainless Steel' GROUP BY BUSINESS_UNIT;"
    info "Tests completed."
}

cmd_status() {
    info "Listing databases and warehouses..."
    snow sql $SNOW_CONN -q "SHOW DATABASES LIKE '${DATABASE}';"
    snow sql $SNOW_CONN -q "SHOW WAREHOUSES LIKE '${WAREHOUSE}';"
}

cmd_streamlit() {
    info "Getting Streamlit app URL..."
    snow streamlit get-url $SNOW_CONN UPIP_APP --database "$DATABASE" --schema "DATA_SCIENCE"
}

case "$COMMAND" in
    main) cmd_main ;;
    test) cmd_test ;;
    status) cmd_status ;;
    streamlit) cmd_streamlit ;;
    *) error_exit "Unknown command: $COMMAND" ;;
esac
