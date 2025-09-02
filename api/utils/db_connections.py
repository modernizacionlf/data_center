import os
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from dotenv import load_dotenv

from utils.paths import BASE_UNICA_ENV_PATH, GEONODE_ENV_PATH


@dataclass
class QueryRequest:
    query: str
    params: Optional[Mapping[str, Any]] = None
    main_table: Optional[str] = None


type SourceConfig = dict[str, str]


class DBConnection:
    def __init__(
        self,
        env_path: Path,
        user_key: str,
        password_key: str,
        host_key: str,
        port_key: str,
        db_key: str,
    ) -> None:
        print(f"Loading env file: {env_path}")
        print(f"File exists: {env_path.exists()}")
        
        load_dotenv(env_path, override=True)
        
        self.user = os.getenv(user_key, "")
        self.password = os.getenv(password_key, "")
        self.host = os.getenv(host_key, "")
        self.port = os.getenv(port_key, "")
        self.db = os.getenv(db_key, "")
        
        print(f"Loaded values - User: {self.user}, Host: {self.host}, DB: {self.db}")

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    @property
    def source_config(self) -> SourceConfig:
        return {"name": self.db, "connection_string": self.connection_string}

    @property
    def queries(self) -> list[QueryRequest]:
        """Override this method in subclasses to provide specific queries"""
        raise NotImplementedError("Subclasses must implement queries property")


class Geonode(DBConnection):
    def __init__(
        self,
        env_path: Path = GEONODE_ENV_PATH,
        user_key: str = "DB_USER",
        password_key: str = "DB_PASSWORD",
        host_key: str = "DB_HOST",
        port_key: str = "DB_PORT",
        db_key: str = "DB_NAME",
    ) -> None:
        super().__init__(env_path, user_key, password_key, host_key, port_key, db_key)

    @property
    def queries(self) -> list[QueryRequest]:
        return [
            QueryRequest(
                query="""
                    SELECT r.id, re.estado, rn.nivel AS borde, r.pintura 
                    FROM global.rampas r
                    JOIN global.rampas_estados re ON r.estado_id = re.estado_id
                    JOIN global.rampas_niveles rn ON r.nivel_id = rn.nivel_id
                """,
                main_table="rampas"
            )
        ]


class BaseUnica(DBConnection):
    def __init__(
        self,
        env_path: Path = BASE_UNICA_ENV_PATH,
        user_key: str = "DB_USER",
        password_key: str = "DB_PASSWORD",
        host_key: str = "DB_HOST",
        port_key: str = "DB_PORT",
        db_key: str = "DB_NAME",
    ) -> None:
        super().__init__(env_path, user_key, password_key, host_key, port_key, db_key)

    @property
    def queries(self) -> list[QueryRequest]:
        return [
            QueryRequest(
                query="""
                    WITH primera_consulta AS (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY paciente ORDER BY id) AS rn
                        FROM consultas_medicas
                        WHERE edad > 0 
                        AND edad < 103 
                        AND obra_social IS NOT NULL
                    )
                    SELECT 
                        id,
                        edad,
                        CASE
                            WHEN obra_social NOT IN (
                                'PROGRAMA INCLUIR SALUD',
                                'PROGRAMA SUMAR',
                                'PROGRAMA FEDERAL DE SALUD'
                            ) THEN 'si'
                            ELSE 'no'
                        END AS tiene_obra_social,
                        CASE
                            WHEN sexo = 'M' THEN 'Masculino'
                            ELSE 'Femenino'
                        END AS sexo
                    FROM primera_consulta
                    WHERE rn = 1;
                """,
                main_table="centros_de_atencion_primaria"
            ),
            QueryRequest(
                query="""
                    SELECT 
                        i.id,
                        i.falta AS tipo_infraccion, 
                        a.tipo_vehiculo, 
                        COALESCE(a.retiene_licencia, false) AS retiene_licencia, 
                        COALESCE(a.retiene_vehiculo, false) AS retiene_vehiculo
                    FROM actas a 
                    INNER JOIN imputaciones i ON a.id = i.id_acta
                    WHERE i.falta IN (
                        SELECT i.falta
                        FROM imputaciones i
                        GROUP BY i.falta
                        ORDER BY COUNT(*) DESC
                        LIMIT 6
                    );
                """,
                main_table="infracciones"
            )
        ]



class DataCenter(DBConnection):
    def __init__(
        self,
        env_path: Path,
        user_key: str = "POSTGRES_USER",
        password_key: str = "POSTGRES_PASSWORD",
        host_key: str = "POSTGRES_HOST",
        port_key: str = "PGPORT",
        db_key: str = "POSTGRES_DB",
    ) -> None:
        super().__init__(env_path, user_key, password_key, host_key, port_key, db_key)
