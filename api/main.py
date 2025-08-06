from fastapi import FastAPI


app = FastAPI()


@app.get("/")
async def root():
    return "Hello world"

@app.get("/entidades")
async def get_entities():
    return "Entidades disponibles"

@app.get("/entidades/{entity_name}")
async def get_entity(entity_name: str):
    return {"Entity": entity_name}