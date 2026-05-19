# data-service/Dockerfile
FROM python:3.12-slim

WORKDIR /app

# зависимости
COPY data-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# копируем код data-service
COPY data-service/ /app/

# копируем общий модуль с предикторами из корня репо
COPY ml_service/ /app/ml_service/

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8000"]