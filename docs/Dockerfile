FROM python:3.9-slim

# Install system dependencies, gsutil for GCS uploads
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    curl \
    gnupg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK for gsutil
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-sdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory to the project root
# Note: This Dockerfile should be built from the repo root with: 
# docker build -t doc-builder -f docs/Dockerfile .
WORKDIR /app

# Copy pyproject.toml first for dependency caching
COPY pyproject.toml README.md ./

# Install the package with docs dependencies
RUN pip install --no-cache-dir -e ".[docs]"

# Copy the documentation source to keep directory structure
COPY docs /app/docs

# Change to docs directory
WORKDIR /app/docs

# Set environment variables
ENV CONTAINER_ENV=1
ENV CLOUD_RUN=1

# Default command to build HTML docs and upload to GCS
CMD ["python", "build_docs.py", "--cloud"]

# For local development:
# Run from the repository root:
# docker build -t doc-builder -f docs/Dockerfile .
# docker run --rm -v $(pwd):/app -e GCS_BUCKET=your-bucket doc-builder