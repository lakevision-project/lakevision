FROM node:20.17.0-bookworm AS builder
RUN apt-get update && \
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

FROM be
WORKDIR /app/fe
COPY ./fe/package*.json ./
RUN npm ci
COPY ./fe/ .

COPY nginx.conf /etc/nginx/

ARG ENABLE_SAMPLE_CATALOG=false

# Copy sample loader into the container
COPY scripts/load_sample_data.py /app/scripts/load_sample_data.py

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
RUN chmod -R 777 /app/fe/ && chmod -R 777 /var/lib/nginx/
EXPOSE 3000 8000 8081
ENV HOST=0.0.0.0
