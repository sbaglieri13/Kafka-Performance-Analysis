# utlizza un'immagine base di Python 3.12 slim per ridurre le dimensioni dell'immagine
FROM python:3.12-slim 

# imposta la directory di lavoro all'interno del container
WORKDIR /app 
# copia il file producer.py nella directory ./producer del container
COPY producer.py . 
 # durante la build installa la libreria confluent-kafka
RUN pip install confluent-kafka

# comando di default per eseguire il producer Kafka
CMD ["python", "producer.py"]