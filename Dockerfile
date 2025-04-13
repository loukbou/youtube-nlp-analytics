FROM apache/airflow:2.10.5

# System dependencies (essential for transformers)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libopenblas-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install with Mamba (faster than pip for scientific packages)
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    torch==2.6.0 \
    transformers==4.40.0

# Verify
RUN python -c "from transformers import pipeline; print('Success!')"
