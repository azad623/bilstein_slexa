"""bilstein_slexa."""

import os
import logging
import logging.config
from pathlib import Path
from typing import Optional
import yaml


def get_yaml_config(file_path: Path) -> Optional[dict]:
    """Fetch yaml config and return as dict if it exists."""
    if file_path.exists():
        with open(file_path, "rt") as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)


# Define project base directory
PROJECT_DIR = Path(__file__).resolve().parents[1]

# Google account secret file path
service_account_path = str(
    os.path.join(PROJECT_DIR, "secrets/azadsandbox-437909-ee45e051e930.json")
)

# Collect errors
global_vars = {"error_list": []}

# Define log output locations
log_output_path = str(Path(__file__).parent.resolve() / "logs/")

# define schema locations
source_schema_path = str(
    Path(__file__).parent.resolve() / "config/schemas/source_bilstein_schema_v1.json"
)

material_schema_path = str(
    Path(__file__).parent.resolve() / "config/schemas/material_grade_mapping.json"
)

g_sheet_schema_path = str(
    Path(__file__).parent.resolve() / "config/schemas/ref_gsheet_schema.json"
)

category_path = str(
    Path(__file__).parent.resolve() / "config/schemas/category_mapping.json"
)


# Read log config file
log_config_path = Path(__file__).parent.resolve() / "config/logging.yaml"

# Define module logger
logger = logging.getLogger(__name__)

# base/global config
_base_config_path = Path(__file__).parent.resolve() / "config/base.yaml"
config = get_yaml_config(_base_config_path)

# define input and output data locations
local_data_input_path = str(Path(__file__).resolve().parents[1] / "inputs/")
local_data_output_path = str(Path(__file__).resolve().parents[1] / "outputs/")

# finish repo path
finish_repo_path = Path(__file__).parent.resolve() / "config/bilstein_finish_repo.yaml"
