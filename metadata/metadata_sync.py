import json
import pandas as pd
from dotenv import load_dotenv
import os

pd.set_option('display.max_columns', None)
from ingestion.load_raw import BASE_DIR
import psycopg2

def load_metadata():
    '''
    Extracts the data from dbt -> manifest.json to create 2 entries:
    models: DataFrame containing all models with their attributes
    dependencies: DataFrame with the lineage
    :return: models, dependencies
    '''

    manifest_path = BASE_DIR / "analytics_dbt/target/manifest.json"

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

    models_df = pd.DataFrame(models)
    dependencies_df = pd.DataFrame(dependencies)
    return models_df, dependencies_df


def init_metadata_schema(cur):
    '''
    Initialize metadata schema and tables if they don't exist
    '''
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS metadata;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata.models (
            unique_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            schema_name VARCHAR(100),
            layer VARCHAR(50),
            materialization VARCHAR(50),
            description TEXT,
            path VARCHAR(500),
            tags JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata.dependencies (
            from_node VARCHAR(255) NOT NULL,
            to_node VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (from_node, to_node)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_models_schema ON metadata.models(schema_name);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_models_layer ON metadata.models(layer);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dependencies_from ON metadata.dependencies(from_node);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dependencies_to ON metadata.dependencies(to_node);
    """)


def store_metadata(models_df, dependencies_df):
    '''Stores the models and dependencies in DB
    Fully handles connections, creates tables if they don't exist and stores
    data in respective tables'''

    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()

    # Initialize schema and tables
    init_metadata_schema(cur)
    conn.commit()

    for _, row in models_df.iterrows():
        cur.execute("""
                INSERT INTO metadata.models (
                    unique_id,
                    name,
                    schema_name,
                    layer,
                    materialization,
                    description,
                    path,
                    tags
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (unique_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    schema_name = EXCLUDED.schema_name,
                    layer = EXCLUDED.layer,
                    materialization = EXCLUDED.materialization,
                    description = EXCLUDED.description,
                    path = EXCLUDED.path,
                    tags = EXCLUDED.tags;
            """, (
            row["unique_id"],
            row["name"],
            row["schema"],
            row["layer"],
            row["materialization"],
            row["description"],
            row["path"],
            json.dumps(row["tags"])
        ))

    for _, row in dependencies_df.iterrows():
        cur.execute("""
                INSERT INTO metadata.dependencies (
                    from_node,
                    to_node
                )
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (
            row["from_node"],
            row["to_node"]
        ))

    conn.commit()
    cur.close()
    conn.close()


def main():
    models_df, dependencies_df = load_metadata()
    store_metadata(models_df, dependencies_df)

if __name__ == "__main__":
    main()

