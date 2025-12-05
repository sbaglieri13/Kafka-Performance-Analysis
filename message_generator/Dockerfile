FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY message_generator.py .

CMD ["uvicorn", "message_generator:app", "--host", "0.0.0.0", "--port", "8000"]
