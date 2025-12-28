from fastapi.routing import APIRouter
from .endpoints.us_bearu import router as us_bearu_router


router = APIRouter()  # Initialize the API router
router.include_router(us_bearu_router, prefix="/us_bearu")
