from utils.rootpath import get_project_root_path

ROOT_PATH = get_project_root_path().parent
API_PATH = ROOT_PATH / "api"
SECRETS = API_PATH / "secrets"
GEONODE_ENV_PATH = SECRETS / ".env.geonode"
BASE_UNICA_ENV_PATH = SECRETS / ".env.baseunica"
DATA_CENTER_DEVELOPMENT_PATH = API_PATH / ".env.development"
DATA_CENTER_PRODUCTION_PATH = API_PATH / ".env.production"
