# Runtime is Red Hat UBI 10 minimal (freely redistributable, no subscription needed).
# UBI 10 ships Python 3.12 (there is no UBI 3.11 without the much larger s2i image), so
# CI runs 3.12 too. Node comes from the official node image (glibc 2.28 baseline, so it
# runs on UBI 10's glibc 2.39). openssl-libs and ca-certificates ship in ubi-minimal.
FROM node:24.20.0-trixie-slim AS node
FROM registry.access.redhat.com/ubi10/ubi-minimal:10.3 AS builder
COPY --from=node /usr/local/bin/node /usr/local/bin/node
# Only the node binary: the runtime runs `node build`, never npm (npm's own
# bundled deps -- undici, tar, brace-expansion, ip-address -- were 10 of 14 node CVEs).
RUN microdnf -y upgrade && \
    microdnf -y install python3.12 python3.12-pip nginx libstdc++ && \
    microdnf clean all && \
    python3 -m pip install --no-cache-dir --upgrade pip setuptools

FROM builder AS be
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app
WORKDIR /app/be
COPY ./be/requirements.txt ./
RUN python3 -m pip install --no-cache-dir --upgrade -r requirements.txt
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

# pip is only needed at build time. Removing it (the upgraded copy and the RPM) leaves no
# installer in the runtime image; pip's vendored SBOM (pip/_vendor/bom.cdx.json) also made
# scanners report a setuptools 70.3.0 that is not installed.
RUN python3 -m pip uninstall -y pip && microdnf -y remove python3.12-pip && microdnf clean all

#fix later, this is for OCP
RUN chmod -R 777 /app/ && chmod -R 777 /var/lib/nginx/
EXPOSE 3000 8000 8081
ENV HOST=0.0.0.0
