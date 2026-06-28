import json
from catboost import CatBoostClassifier, CatBoostRegressor
from forecasting.utils.config_handler import FORECASTING_PATH

REGISTRY_PATH = FORECASTING_PATH / "model_registry" / "catboost"
VERSIONS_FILE = REGISTRY_PATH / "versions.json"
MAX_VERSIONS = 2

def _read_versions() -> dict:
    if not VERSIONS_FILE.exists():
        return {"latest": None, "versions": []}
    with open(VERSIONS_FILE, "r") as f:
        return json.load(f)

def _write_versions(versions: dict) -> None:
    with open(VERSIONS_FILE, "w") as f:
        json.dump(versions, f, indent=2)

def save_model(classifier: CatBoostClassifier, regressor: CatBoostRegressor) -> None:
    import shutil
    versions = _read_versions()
    versions_list = versions["versions"]

    if len(versions_list) == 0:
        latest_version = "v1"
    else:
        last_version = versions_list[-1]
        new_number = int(last_version[1:]) + 1
        latest_version = f"v{new_number}"

    versions_list.append(latest_version)

    # removing old records after adding the new ones
    while len(versions_list) > MAX_VERSIONS:
        oldest = versions_list.pop(0)  # remove from the list and getting a name
        shutil.rmtree(REGISTRY_PATH / oldest)  # remove folder

    # store models
    version_path = REGISTRY_PATH / latest_version
    version_path.mkdir(parents=True, exist_ok=True)
    classifier.save_model(str(version_path / "classifier.cbm"))
    regressor.save_model(str(version_path / "regressor.cbm"))

    _write_versions({"latest": latest_version, "versions": versions_list})


def load_model() -> tuple[CatBoostClassifier, CatBoostRegressor]:
    versions = _read_versions()

    if versions["latest"] is None:
        raise FileNotFoundError("No models found in registry")

    version_path = REGISTRY_PATH / versions["latest"]

    classifier = CatBoostClassifier()
    classifier.load_model(str(version_path / "classifier.cbm"))

    regressor = CatBoostRegressor()
    regressor.load_model(str(version_path / "regressor.cbm"))

    return classifier, regressor
