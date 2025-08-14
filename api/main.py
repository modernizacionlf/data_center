from fastapi import FastAPI

from src.endpoints import ENDPOINTS


api = FastAPI(title="Centro de Datos - Las Flores")

@api.get(ENDPOINTS.BASE)
async def root():
    return "Hello world"

@api.get(ENDPOINTS.ENTITIES)
async def get_entities():
    return "Entidades disponibles"

@api.get(ENDPOINTS.ENTITY("{entity_name}"))
async def get_entity(entity_name: str):
    return {"Entity": entity_name}