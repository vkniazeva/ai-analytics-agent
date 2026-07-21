from unittest.mock import patch

from ai_analytics_agent.utils.database import get_engine


@patch("ai_analytics_agent.utils.database.create_engine")
def test_get_engine_builds_connection_string_from_env(mock_create_engine, monkeypatch):
    monkeypatch.setenv("AGENT_DB_HOST", "myhost")
    monkeypatch.setenv("AGENT_DB_PORT", "1234")
    monkeypatch.setenv("AGENT_DB_NAME", "mydb")
    monkeypatch.setenv("AGENT_DB_USER", "myuser")
    monkeypatch.setenv("AGENT_DB_PASSWORD", "mypass")

    get_engine()

    mock_create_engine.assert_called_once_with(
        "postgresql+psycopg2://myuser:mypass@myhost:1234/mydb"
    )


@patch("ai_analytics_agent.utils.database.create_engine")
def test_get_engine_defaults_host_and_port(mock_create_engine, monkeypatch):
    monkeypatch.delenv("AGENT_DB_HOST", raising=False)
    monkeypatch.delenv("AGENT_DB_PORT", raising=False)
    monkeypatch.setenv("AGENT_DB_NAME", "mydb")
    monkeypatch.setenv("AGENT_DB_USER", "myuser")
    monkeypatch.setenv("AGENT_DB_PASSWORD", "mypass")

    get_engine()

    mock_create_engine.assert_called_once_with(
        "postgresql+psycopg2://myuser:mypass@localhost:5433/mydb"
    )
