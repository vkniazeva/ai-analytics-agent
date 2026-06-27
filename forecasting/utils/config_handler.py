from pathlib import Path
import yaml

FORECASTING_PATH = Path(__file__).parent.parent
def return_config():

    config_path = FORECASTING_PATH / "configs" / "config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

