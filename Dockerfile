FROM python:3.9-slim

# Install system dependencies, gsutil for GCS uploads
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    curl \
    gnupg \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK for gsutil
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-sdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the entire repository content first
COPY . .
RUN rm -f setup.py

# Install the package with docs dependencies
RUN pip install --no-cache-dir -e ".[docs]"

# Set environment variables
ENV CONTAINER_ENV=1
ENV CLOUD_RUN=1

# Default command to build HTML docs and upload to GCS
CMD ["python", "docs/build_docs.py", "--cloud"]

# For local development:
# docker build -t doc-builder .
# docker run --rm -v $(pwd):/app -e GCS_BUCKET=your-bucket doc-builder
