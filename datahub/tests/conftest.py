import sys
from pathlib import Path

# The datahub-ingest image runs from /app/datahub; mirror that import root (`lineage`, `tools`).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
