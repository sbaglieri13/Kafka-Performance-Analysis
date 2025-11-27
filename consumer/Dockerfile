# utlizza un'immagine base di Python 3.12 slim per ridurre le dimensioni dell'immagine
FROM python:3.12-slim

# imposta la directory di lavoro all'interno del container
WORKDIR /app
# copia il file consumer.py nella directory ./consumer del container
COPY consumer.py .
 # durante la build installa la libreria confluent-kafka
RUN pip install confluent-kafka

# comando di default per eseguire il producer Kafka
CMD ["python", "consumer.py"]
