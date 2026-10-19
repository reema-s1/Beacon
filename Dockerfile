# One image for beacon-api, beacon-slack, beacon-mcp and the chaos/eval tools.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/opt/hf

WORKDIR /app

# CPU-only torch keeps the image ~1.5 GB smaller than the default CUDA build.
RUN pip install "torch==2.5.1" --index-url https://download.pytorch.org/whl/cpu

# Dependencies first so code edits don't bust the layer cache.
COPY pyproject.toml ./
RUN mkdir -p beacon chaos eval && touch beacon/__init__.py chaos/__init__.py eval/__init__.py \
    && pip install ".[dev]" && pip uninstall -y beacon

# Bake the embedding model into the image so containers start offline. A copy saved by
# scripts/fetch_model.py into ./models is used when present; otherwise it is downloaded.
COPY models/ /opt/models/
RUN if [ ! -f /opt/models/all-MiniLM-L6-v2/config.json ]; then \
      python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2').save('/opt/models/all-MiniLM-L6-v2')"; \
    fi
ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 EMBEDDING_MODEL=/opt/models/all-MiniLM-L6-v2

COPY beacon/ beacon/
COPY beacon_slack/ beacon_slack/
COPY beacon_mcp/ beacon_mcp/
COPY chaos/ chaos/
COPY eval/ eval/
COPY runbooks/ runbooks/
COPY tests/ tests/
RUN pip install --no-deps -e .

EXPOSE 8000
CMD ["beacon-api"]
