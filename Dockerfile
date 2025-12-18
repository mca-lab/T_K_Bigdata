FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install Java for PySpark
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and data
COPY src/ src/
COPY data/ data/

# Ensure raw data directory exists
RUN mkdir -p data/raw

# Default command (can be overridden)
CMD ["bash"]
