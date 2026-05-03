import os

SUPERSET_SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")

_db_user = os.getenv("SUPERSET_DB_USER")
_db_password = os.getenv("SUPERSET_DB_PASSWORD")
_db_name = os.getenv("SUPERSET_DB_NAME")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{_db_user}:{_db_password}@superset_db:5432/{_db_name}"

DATA_DIR = "/app/superset_home"

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_HTTPONLY = True

PREVENT_UNSAFE_DB_CONNECTIONS = False
