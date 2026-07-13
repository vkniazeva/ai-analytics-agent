from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

def get_engine():
    host = os.getenv("AGENT_DB_HOST", "localhost")
    port = os.getenv("AGENT_DB_PORT", "5433")
    name = os.getenv("AGENT_DB_NAME")
    user = os.getenv("AGENT_DB_USER")
    password = os.getenv("AGENT_DB_PASSWORD")

    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}")
