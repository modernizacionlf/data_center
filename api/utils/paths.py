from utils.rootpath import get_project_root_path

ROOT_PATH = get_project_root_path()
GEONODE_ENV_PATH = ROOT_PATH / ".env.geonode"
DATA_CENTER_ENVIRONMENT_PATH = ROOT_PATH.parent / ".env.development"
DATA_CENTER_PRODUCTION_PATH = ROOT_PATH.parent / ".env.production"