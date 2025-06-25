#!/bin/bash

# -------------------------
# EQX Daily Batch Executor
# -------------------------

# Activate virtual env if needed
# source /path/to/venv/bin/activate

# Set up paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/logs/daily_batch.log"
mkdir -p "$(dirname "$LOG_FILE")"

PYTHON_EXEC="/usr/bin/python3"  # Update if needed
RUNNER_SCRIPT="$SCRIPT_DIR/eqx_runner.py"

# Auto-set today's date (in YYYY-MM-DD)
TODAY=$(date +%F)

# Log function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"
}

log "Starting EQX batch job for $TODAY"

# Run the pipeline
$PYTHON_EXEC "$RUNNER_SCRIPT" \
    --steps run_all \
    --date "$TODAY" \
    >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    log "EQX batch job failed with exit code $EXIT_CODE"
    
    # Optionally notify via email, Slack, etc.
    # curl -X POST -H "Content-Type: application/json" -d '{"text":"EQX batch job failed for date: '"$TODAY"'"}' https://hooks.slack.com/services/XXX/YYY/ZZZ

    exit 1
else
    log "EQX batch job completed successfully"
    exit 0
fi
