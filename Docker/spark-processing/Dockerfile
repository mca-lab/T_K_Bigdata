FROM bitnami/spark:3.5

USER root

# Install Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy PySpark requirements
COPY docker/spark-processing/requirements.txt /app/requirements.txt
RUN pip3 install --upgrade pip && pip3 install -r /app/requirements.txt

# Code will be provided via volume, so no COPY . needed here
ENV PYTHONUNBUFFERED=1

