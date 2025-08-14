from utils.rootpath import get_project_root_path

ROOT_PATH = get_project_root_path().parent
API_ROOT = ROOT_PATH / "api"
GEONODE_ENV_PATH = API_ROOT / ".env.geonode"
DATA_CENTER_DEVELOPMENT_PATH = ROOT_PATH / ".env.development"
DATA_CENTER_PRODUCTION_PATH = ROOT_PATH / ".env.production"