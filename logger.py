"""
DIGITAL HUNTER - LOGGING UTILITY
--------------------------------
This module provides a centralized logging function to send structured events to Elasticsearch.

LOGGING STANDARDS:
- level: (String) Use one of the following:
    * 'INFO'  - General operational flow (e.g., "Received message from Kafka")
    * 'DEBUG' - Technical/Math details (e.g., "Haversine distance calculated")
    * 'ERROR' - Failures or invalid data (e.g., "Validation failed - routing to DLQ")
    * 'WARNING' - Unusual but non-critical events (e.g., "Target status updated")

- message: (String) A short, clear description of the event.

- extra_info: (Dict) Additional structured data for filtering (e.g., {"entity_id": "T-101", "distance": 5.4})

Note: If Elasticsearch is unreachable, the log will fallback to the console (Local Log).
"""

from elasticsearch import Elasticsearch
from datetime import datetime

# Initialize Elasticsearch client
# Ensure the host 'localhost' matches your docker-compose configuration
es = Elasticsearch(['http://localhost:9200'])


def log_event(level, message, extra_info=None):
    """
    Sends a structured log to Elasticsearch or falls back to console on failure.
    """

    # 1. Structure the mandatory fields
    document = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level.upper(),  # Ensuring levels are always uppercase for consistency
        "message": message
    }

    # 2. Integrate extra metadata if provided
    if extra_info:
        document.update(extra_info)

    try:
        # 3. Ship to Elasticsearch index 'intel-logs'
        es.index(index="intel-logs", document=document)
    except Exception as e:
        # 4. Fallback mechanism: Print to terminal if the connection fails
        print(f"⚠️  [LOCAL LOG - {level.upper()}] {message} | Connection Error: {e}")