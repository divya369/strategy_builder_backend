"""
Auth API — kept as stub endpoints (no authentication enforced).
Signup/login still work if needed, but no endpoints require auth.
"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/me")
def read_current_user():
    """Public mode — returns system user info."""
    return {"id": "00000000-0000-0000-0000-000000000001", "email": "system@local", "full_name": "System"}
