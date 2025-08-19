from fastapi import FastAPI

from utils.endpoints import ENDPOINTS
from utils.endpoints import get_available_entities, get_available_statistics


api = FastAPI(title="Centro de Datos - Las Flores")

@api.get(ENDPOINTS.ENTITIES)
async def get_entities():
    entities = get_available_entities()
    return {"entidades": entities}

@api.get(ENDPOINTS.STATISTICS("{entity_name}"))
async def get_entity_statistics(entity_name: str):
    statistics = get_available_statistics(entity_name)
    return {"estadisticas": statistics}