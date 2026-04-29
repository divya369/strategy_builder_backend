"""
API Dependencies — public mode (no authentication).
A fixed system user UUID is used for all DB writes that require a user_id.
"""
import uuid

# Fixed system user UUID — used as owner for all screeners/backtests
SYSTEM_USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
