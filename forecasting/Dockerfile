FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY forecasting ./forecasting
EXPOSE 8000
CMD ["uvicorn", "forecasting.api.app:app", "--host", "0.0.0.0", "--port", "8000"]