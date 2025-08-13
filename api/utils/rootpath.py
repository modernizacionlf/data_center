from pathlib import Path

def get_project_root_path() -> Path:
    project_root_path = Path(__file__).parent.parent
    return project_root_path