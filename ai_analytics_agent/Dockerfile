FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ai_analytics_agent ./ai_analytics_agent
EXPOSE 8001
CMD ["uvicorn", "ai_analytics_agent.api.app:app", "--host", "0.0.0.0", "--port", "8001"]