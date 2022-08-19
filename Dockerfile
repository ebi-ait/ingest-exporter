FROM quay.io/ebi-ait/ingest-base-images:python_3.7.13-alpine

RUN apk update && \
    apk add build-base && \
    apk add openssl-dev && \
    apk add libffi-dev && \
    apk add git

RUN mkdir /app
WORKDIR /app

RUN python -m venv venv
RUN source venv/bin/activate
RUN pip install --upgrade pip
RUN pip install pip-tools
COPY requirements.txt ./
RUN pip-sync requirements.txt

COPY exporter.py receiver.py ./
COPY ./exporter ./exporter
COPY ./manifest ./manifest

ENV INGEST_API=http://localhost:8080
ENV RABBIT_URL=amqp://localhost:5672
ENV SUBMISSION_QUEUE_NAME=ingest.envelope.submitted.queue
ENV STAGING_API=https://staging.staging.data.humancellatlas.org
ENV STAGING_API_VERSION=v1
ENV INGEST_API_KEY=key_not_set

ENTRYPOINT ["python"]
CMD ["exporter.py"]
