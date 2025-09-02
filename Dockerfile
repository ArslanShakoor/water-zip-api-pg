FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8000 HOST=0.0.0.0
WORKDIR /app
COPY requirements-prod.txt .
RUN pip install --no-cache-dir -r requirements-prod.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app_pg:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
