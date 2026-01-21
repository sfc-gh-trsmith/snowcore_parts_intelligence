#!/bin/bash
# Note: Not using set -e here since we want drops to continue even if some fail

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
FORCE=false

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--connection) CONNECTION_NAME="$2"; shift 2 ;;
        -p|--prefix) ENV_PREFIX="$2"; shift 2 ;;
        --force) FORCE=true; shift ;;
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

if [[ "$FORCE" != "true" ]]; then
    echo -e "${YELLOW}This will DROP warehouse, database, and role for ${DATABASE}.${NC}"
    read -r -p "Type '${DATABASE}' to confirm: " CONFIRM
    if [[ "$CONFIRM" != "${DATABASE}" ]]; then
        error_exit "Confirmation failed. Use --force to skip prompt."
    fi
fi

info "Tearing down resources..."
snow sql $SNOW_CONN -q "DROP WAREHOUSE IF EXISTS ${WAREHOUSE};" 2>/dev/null || true
snow sql $SNOW_CONN -q "DROP DATABASE IF EXISTS ${DATABASE};" 2>/dev/null || true
snow sql $SNOW_CONN -q "DROP ROLE IF EXISTS ${ROLE};" 2>/dev/null || true

info "Clean completed."
exit 0
