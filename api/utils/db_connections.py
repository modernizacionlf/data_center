import os 

from pathlib import Path

from dotenv import load_dotenv


class DBConnection:
    def __init__(self, env_path: Path, user_key: str, password_key: str, host_key: str, port_key: str, db_key: str) -> None:
        load_dotenv(env_path)
        self.user = os.getenv(user_key)
        self.password = os.getenv(password_key)
        self.host = os.getenv(host_key)
        self.port = os.getenv(port_key)
        self.db = os.getenv(db_key)

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
    

class Geonode(DBConnection):
    def __init__(
        self, 
        env_path: Path, 
        user_key: str = "DB_USER", 
        password_key: str = "DB_PASSWORD", 
        host_key: str = "DB_HOST", 
        port_key: str = "DB_PORT", 
        db_key: str = "DB_NAME"
    ) -> None:
        super().__init__(env_path, user_key, password_key, host_key, port_key, db_key)


class DataCenter(DBConnection):
    def __init__(self, 
        env_path: Path, 
        user_key: str = "POSTGRES_USER", 
        password_key: str = "POSTGRES_PASSWORD", 
        host_key: str = "POSTGRES_HOST", 
        port_key: str = "PGPORT", 
        db_key: str = "POSTGRES_DB"
    ) -> None:
        super().__init__(env_path, user_key, password_key, host_key, port_key, db_key)