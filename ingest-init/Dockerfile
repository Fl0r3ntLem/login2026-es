FROM python:3.12-alpine

WORKDIR /app
COPY ingest-init/ingest.py /app/ingest.py
COPY ncedc-earthquakes-dataset/ /source/

RUN pip install requests

ENTRYPOINT ["python", "/app/ingest.py"]
