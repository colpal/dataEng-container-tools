FROM python:3.9-slim

# Install system dependencies and Nginx
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    curl \
    gnupg \
    git \
    nginx \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the entire repository content first
COPY . .
RUN rm -f setup.py

# Install the package with docs dependencies
RUN pip install --no-cache-dir -e ".[docs]"

# Set environment variables
ENV CONTAINER_ENV=1

# Build HTML docs
RUN python docs/build_docs.py --html

# Set up Nginx to serve documentation
RUN mkdir -p /usr/share/nginx/html
COPY default.conf /etc/nginx/conf.d/default.conf
RUN cp -r docs/build/html/* /usr/share/nginx/html/

# Expose port 8080 for Cloud Run
EXPOSE 8080

# Start Nginx in the foreground
CMD ["nginx", "-g", "daemon off;"]

# For local development:
# docker build -t doc-builder .
# docker run --rm -p 8080:8080 doc-builder
