# bookworm (not trixie) keeps Debian's python3 at 3.11, matching CI. The
# base image supplies both runtimes here, so changing the suite would move
# the backend's Python version too.
FROM node:24.20.0-bookworm-slim AS builder
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends nginx python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
RUN rm /usr/lib/python3.*/EXTERNALLY-MANAGED && python3 -m pip install --upgrade pip

FROM builder AS be
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app
WORKDIR /app/be
COPY ./be/requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY ./be/app ./app

COPY start.sh /app/start.sh
RUN chmod 755 /app/start.sh
COPY worker.sh /app/worker.sh
RUN chmod 755 /app/worker.sh
#USER lv

# Frontend is built here, at image build time, not in start.sh. Only the
# built bundle + production deps reach the runtime image, so build-only
# tooling (vite, esbuild's Go binaries, eslint, ...) is not shipped -- that
# tooling carried CVE-2025-68121 (Go stdlib in esbuild). The UI reads its
# config via $env/dynamic/public, i.e. at runtime, so a build-time build is safe.
FROM node:24.20.0-bookworm-slim AS fe-build
WORKDIR /fe
COPY ./fe/package*.json ./
RUN npm ci
COPY ./fe/ .
RUN npm run build && npm prune --omit=dev

FROM be
WORKDIR /app/fe
COPY --from=fe-build /fe/package.json ./
COPY --from=fe-build /fe/node_modules ./node_modules
COPY --from=fe-build /fe/build ./build

COPY nginx.conf /etc/nginx/

ARG ENABLE_SAMPLE_CATALOG=false

# Copy sample loader and helpers into the container
COPY scripts/load_sample_data.py /app/scripts/load_sample_data.py
COPY scripts/create_test_iceberg_tables.py /app/scripts/create_test_iceberg_tables.py

RUN if [ "$ENABLE_SAMPLE_CATALOG" = "true" ]; then \
      echo "🧪 Loading sample catalog for quickstart..." && \
      cd /app/be && \
      export PYICEBERG_CATALOG__DEFAULT__URI=sqlite:////app/warehouse/sql-catalog.db && \
      export PYICEBERG_CATALOG__DEFAULT__WAREHOUSE=file:///app/warehouse \
      export PYTHONPATH=app && \
      env && \
      python3 /app/scripts/load_sample_data.py; \
    fi

#fix later, this is for OCP
RUN chmod -R 777 /app/ && chmod -R 777 /var/lib/nginx/
EXPOSE 3000 8000 8081
ENV HOST=0.0.0.0
