# Runtime is Debian 13 (trixie). Python stays at 3.11 -- matching CI -- by taking
# it from the official python:3.11-slim-trixie image instead of Debian's python3
# (trixie ships 3.13). Node comes from the matching official node image.
FROM node:24.20.0-trixie-slim AS node
FROM python:3.11-slim-trixie AS builder
COPY --from=node /usr/local/bin/node /usr/local/bin/node
# Only the node binary: the runtime runs `node build`, never npm (npm's own
# bundled deps -- undici, tar, brace-expansion, ip-address -- were 10 of 14 node CVEs).
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends nginx libstdc++6 && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install --no-cache-dir --upgrade pip setuptools

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
# Runs on the build host's platform: its output is platform-independent JS (the
# pruned runtime deps have no native code), and esbuild's Go binary crashes under
# QEMU emulation ("fatal error: lfstack.push") when cross-building amd64 on arm64.
FROM --platform=$BUILDPLATFORM node:24.20.0-trixie-slim AS fe-build
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
