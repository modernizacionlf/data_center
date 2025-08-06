from fastapi import FastAPI

from src.endpoints import ENDPOINTS


app = FastAPI(title="Centro de Datos - Las Flores")

@app.get(ENDPOINTS.BASE)
async def root():
    return "Hello world"

@app.get(ENDPOINTS.ENTITIES)
async def get_entities():
    return "Entidades disponibles"

@app.get(ENDPOINTS.ENTITY("{entity_name}"))
async def get_entity(entity_name: str):
    return {"Entity": entity_name}