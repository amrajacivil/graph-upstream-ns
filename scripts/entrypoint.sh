#!/bin/bash
set -euo pipefail


INIT_FLAG_FILE=${INIT_FLAG_FILE:-/data/.db_initialized}
NEO4J_HOST=${NEO4J_HOST:-neo4j}
NEO4J_PORT=${NEO4J_PORT:-7687}


# wait up to ~60s for Bolt (neo4j:7687) to be reachable using /dev/tcp
for i in {1..60}; do
  if (echo > /dev/tcp/"$NEO4J_HOST"/"$NEO4J_PORT") >/dev/null 2>&1; then
    echo "neo4j reachable"
    break
  fi
  echo "waiting for neo4j... ($i)"
  sleep 1
done


# if still not reachable, exit with failure
if ! (echo > /dev/tcp/"$NEO4J_HOST"/"$NEO4J_PORT") >/dev/null 2>&1; then
  echo "neo4j did not become reachable in time"
  exit 1
fi


# skip if already initialized
if [ -f "$INIT_FLAG_FILE" ]; then
  echo "DB already initialized, skipping"
  exit 0
fi


echo "Installing neo4j driver and running init script..."
python -m pip install --no-cache-dir neo4j tqdm pandas >/dev/null 2>&1 || true


if python /scripts/init_db.py; then
  touch "$INIT_FLAG_FILE"
  echo "Init succeeded"
  exit 0
else
  echo "Init failed"
  exit 1
fi
