"""bilstein_slexa."""

import os
import logging
import logging.config
from pathlib import Path
from typing import Optional
from transformers import MarianMTModel, MarianTokenizer

import yaml


def load_translator(src_lang, tgt_lang):
    model_name = f"Helsinki-NLP/opus-mt-{src_lang}-{tgt_lang}"
    tokenizer = MarianTokenizer.from_pretrained(model_name)
    model = MarianMTModel.from_pretrained(model_name)
    return tokenizer, model


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


# Define log output locations
log_output_path = str(Path(__file__).parent.resolve() / "logs/")

# define schema locations
source_schema_path = str(
    Path(__file__).parent.resolve() / "config/schemas/source_bilstein_schema_v1.json"
)

g_sheet_schema_path = str(
    Path(__file__).parent.resolve() / "config/schemas/ref_gsheet_schema.json"
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

# load translator
# Load MarianMT model and tokenizer
src_lang = "de"  # Source language code (e.g., German)
tgt_lang = "en"  # Target language code (e.g., English)
tokenizer, model = load_translator(src_lang, tgt_lang)
