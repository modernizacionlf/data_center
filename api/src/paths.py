from src.utils import get_project_root_path

ROOT_PATH = get_project_root_path()
GEONODE_ENV_PATH = ROOT_PATH / ".env.geonode"
DATA_CENTER_ENV_PATH = ROOT_PATH.parent / ".env"