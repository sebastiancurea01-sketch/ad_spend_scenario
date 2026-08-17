FROM python:3.11-slim

WORKDIR /app

# Copy just this one file in first, before anything else.
# Why: Docker caches each step. If only your dbt models change
# later (not your dependencies), Docker can skip reinstalling
# everything and reuse the cached install step. Faster rebuilds.
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DATABRICKS_HOST=""
ENV DATABRICKS_TOKEN=""
ENV DATABRICKS_HTTP_PATH=""

CMD ["dbt", "--version"]