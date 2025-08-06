class ENDPOINTS:
    BASE = "/api/v1"
    ENTITIES = f"{BASE}/entidades"
    
    @classmethod
    def ENTITY(cls, entity_name: str):
        return f"{cls.ENTITIES}/{entity_name}"

ENDPOINTS.ENTITIES