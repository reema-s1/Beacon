FROM python:3.11-slim
RUN pip install --no-cache-dir "fastapi==0.115.6" "uvicorn==0.32.1"
WORKDIR /app
COPY app.py .
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
