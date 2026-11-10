ARG NRL_IMAGE=nvcr.io/nvidia/nemo-microservices/nrl-service:26.8.2
FROM ${NRL_IMAGE}
USER 0:0
RUN apt-get update && apt-get install -y --no-install-recommends git libnss3 libatk1.0-0 libatk-bridge2.0-0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 libasound2 libcups2 libxkbcommon0 libpango-1.0-0 libcairo2 && rm -rf /var/lib/apt/lists/*
COPY build /app/build
RUN python /app/build/prepare_browser.py
COPY ingestion/patches /app/ingestion/patches
RUN python /app/build/prepare_library.py /opt/agentic-ingestion/library
RUN python -m pip install --no-cache-dir --require-hashes -r /app/build/application-requirements.txt -r /app/build/browser-requirements.txt -r /app/build/worker-requirements.txt && python -m pip check
COPY application /app/application
COPY ingestion /app/ingestion
COPY collection_control /app/collection_control
COPY runtime/src /app/runtime/src
COPY deployment /app/deployment
COPY tests /app/tests
COPY tools /app/tools
COPY config /app/config
ENV PYTHONPATH=/app:/app/runtime/src:/opt/agentic-ingestion/library PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 LANGCHAIN_TRACING_V2=false LANGSMITH_TRACING=false DURABLE_SOURCE_ROOT=/app/runtime
WORKDIR /app
USER 1000:1000
ENTRYPOINT ["python", "-m", "application.api"]
