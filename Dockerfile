FROM python:3.11-slim

WORKDIR /app

RUN apt-get update -qq && apt-get install -y -qq curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY build_osm_graph.py osm_api.py osm_router.py realtime_sync.py ./
COPY index.html admin.html ./
COPY start.sh .
RUN chmod +x start.sh

ENV DB_PATH=/data/taiwan_osm.db
ENV AUTO_SYNC_INTERVAL=300

EXPOSE 8000

CMD ["./start.sh"]
