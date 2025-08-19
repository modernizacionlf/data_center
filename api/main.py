from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from utils.endpoints import ENDPOINTS, ALLOWED_HOSTS, ALLOWED_ORIGINS
from utils.endpoints import get_available_entities, get_available_statistics


api = FastAPI(
    title="Centro de Datos - Las Flores",
    root_path="/api"
)

api.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=ALLOWED_HOSTS
)

api.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@api.get(ENDPOINTS.ENTITIES)
async def get_entities():
    entities = get_available_entities()
    return {"entidades": entities}

@api.get(ENDPOINTS.STATISTICS("{entity_name}"))
async def get_entity_statistics(entity_name: str):
    statistics = get_available_statistics(entity_name)
    return {"estadisticas": statistics}