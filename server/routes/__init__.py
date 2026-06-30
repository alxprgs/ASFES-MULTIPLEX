from fastapi import APIRouter

from .admin import router as admin_router
from .auth import router as auth_router
from .health import router as health_router
from .oauth import oauth_router, well_known_router
from .api_keys import router as api_key_router
from .proxy import router as proxy_router
from .pypi import management_router as pypi_management_router
from .python_mirror import router as python_mirror_router


api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(auth_router)
api_router.include_router(admin_router)
api_router.include_router(oauth_router)
api_router.include_router(api_key_router)
api_router.include_router(proxy_router)
api_router.include_router(pypi_management_router)
api_router.include_router(python_mirror_router)

root_router = APIRouter()
root_router.include_router(well_known_router)
