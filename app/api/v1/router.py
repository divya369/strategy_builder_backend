from fastapi import APIRouter
from app.api.v1 import screeners, auth
from app.api.v1 import backtests, results
from app.api.v1 import zerodha_publisher

api_router = APIRouter()

api_router.include_router(screeners.router, prefix="/screeners", tags=["screeners"])
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(backtests.router, prefix="/backtests", tags=["backtests"])
api_router.include_router(results.router, prefix="/results", tags=["results"])
api_router.include_router(zerodha_publisher.router, prefix="/zerodha-publisher", tags=["Zerodha Publisher"])