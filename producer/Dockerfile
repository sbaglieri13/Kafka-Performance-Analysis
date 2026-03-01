FROM python:3.12-slim

WORKDIR /app

# Install iproute2 for network configuration
RUN apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

COPY configure_routing.sh /configure_routing.sh
RUN chmod +x /configure_routing.sh

ENTRYPOINT ["/configure_routing.sh"]
