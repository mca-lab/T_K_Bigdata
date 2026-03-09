FROM python:3.10-slim

# Prevent python buffering issues
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app


# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

ENV PYSPARK_PYTHON=python3


# Copy requirements first (better Docker caching)
COPY requirements.txt .


# Upgrade pip and install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Copy project files
COPY src/ src/
COPY notebooks/ notebooks/
COPY data/ data/


# Ensure required directories exist
RUN mkdir -p data/raw data/processed


# Expose Jupyter port
EXPOSE 8888

COPY start.sh/ start.sh/
RUN chmod +x /start.sh

# Introduce entrypoint 
ENTRYPOINT ["/start.sh"]



# Default command (temporary)
CMD ["bash"]