from fastapi import FastAPI

from utils.endpoints import ENDPOINTS, get_available_entities


api = FastAPI(title="Centro de Datos - Las Flores")

@api.get(ENDPOINTS.ENTITIES)
async def get_entities():
    entities = get_available_entities()
    return {"entidades": entities}

@api.get(ENDPOINTS.ENTITY("{entity_name}"))
async def get_entity(entity_name: str):
    return {"entidad": entity_name}