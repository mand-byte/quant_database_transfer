FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai

# Installation of necessary build tools (optional but safe for pandas/clickhouse extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and entry points
COPY src/ ./src/
COPY main.py .

# Trigger the transfer process at startup and block on scheduler
CMD ["python", "-u", "main.py"]
