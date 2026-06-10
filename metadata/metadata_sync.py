import json
from pathlib import Path
import pandas as pd
pd.set_option('display.max_columns', None)

from ingestion.load_raw import BASE_DIR

manifest_path = BASE_DIR / "analytics/target/manifest.json"

with open(manifest_path) as f:
    manifest = json.load(f)

models = []
dependencies = []

for unique_id, node in manifest["nodes"].items():
    if node["resource_type"] != "model":
        continue
    models.append(
        {
            "unique_id": node["unique_id"],
            "name": node["name"],
            "schema": node["schema"],
            "layer": node["original_file_path"].split("/")[1],
            "materialization": node["config"]["materialized"],
            "description": node["description"],
            "path": node["path"],
            "tags": node["tags"]
        }
    )

    for dependency in node["depends_on"]["nodes"]:
        dependencies.append({
            "from_node": dependency,
            "to_node": unique_id
        })

models = pd.DataFrame(models)
dependencies = pd.DataFrame(dependencies)




