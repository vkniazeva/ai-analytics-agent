from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from ai_analytics_agent.api.app import app, CONVERSATIONS

client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_conversations():
    CONVERSATIONS.clear()
    yield
    CONVERSATIONS.clear()


@patch("ai_analytics_agent.api.app.run_agent")
def test_ask_new_conversation_returns_answer(mock_run_agent):
    mock_run_agent.return_value = ("Revenue is 100", [{"role": "user", "content": "what's revenue?"}])

    response = client.post("/ask", json={"question": "what's revenue?"})

    assert response.status_code == 200
    body = response.json()
    assert body["answer"] == "Revenue is 100"
    assert "conversation_id" in body


@patch("ai_analytics_agent.api.app.run_agent")
def test_ask_new_conversation_is_stored(mock_run_agent):
    updated_messages = [
        {"role": "user", "content": "what's revenue?"},
        {"role": "assistant", "content": "Revenue is 100"},
    ]
    mock_run_agent.return_value = ("Revenue is 100", updated_messages)

    response = client.post("/ask", json={"question": "what's revenue?"})
    conversation_id = response.json()["conversation_id"]

    assert CONVERSATIONS[conversation_id] == updated_messages


@patch("ai_analytics_agent.api.app.run_agent")
def test_ask_reuses_existing_conversation_history(mock_run_agent):
    conversation_id = "existing-conversation"
    CONVERSATIONS[conversation_id] = [{"role": "user", "content": "first question"}]
    mock_run_agent.return_value = ("second answer", [])

    response = client.post("/ask", json={"question": "second question", "conversation_id": conversation_id})

    assert response.json()["conversation_id"] == conversation_id
    passed_history = mock_run_agent.call_args[0][0]
    assert passed_history == [
        {"role": "user", "content": "first question"},
        {"role": "user", "content": "second question"},
    ]


@patch("ai_analytics_agent.api.app.run_agent")
def test_ask_unknown_conversation_id_starts_new(mock_run_agent):
    mock_run_agent.return_value = ("answer", [])

    response = client.post("/ask", json={"question": "hi", "conversation_id": "does-not-exist"})

    assert response.json()["conversation_id"] != "does-not-exist"


@patch("ai_analytics_agent.api.app.run_agent")
def test_ask_passes_only_user_question_for_new_conversation(mock_run_agent):
    mock_run_agent.return_value = ("answer", [])

    client.post("/ask", json={"question": "what's revenue?"})

    passed_history = mock_run_agent.call_args[0][0]
    assert passed_history == [{"role": "user", "content": "what's revenue?"}]
