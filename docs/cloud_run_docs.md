# Using Cloud Run for Documentation Builds

This guide explains how to build HTML documentation using the provided Dockerfile with Google Cloud Run.

## Prerequisites

- Google Cloud SDK installed
- Access to a Google Cloud project
- Docker installed locally (for testing)

## Local Testing

Before deploying to Cloud Run, test the Docker container locally:

```bash
# Build the Docker image from the repository root
cd /path/to/dataEng-container-tools
docker build -t doc-builder .

# Run the container to build HTML docs
docker run --rm -v $(pwd):/app -e GCS_BUCKET=your-bucket-name doc-builder
```

## Deploying to Cloud Run

1. Set up your GCP project and enable required APIs:

```bash
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com storage.googleapis.com
```

2. Build and push the Docker image to Google Container Registry:

```bash
# Set your project ID
export PROJECT_ID=$(gcloud config get-value project)

# Build and push the image from the repository root
gcloud builds submit --tag gcr.io/$PROJECT_ID/doc-builder
```

3. Create a Cloud Storage bucket for the documentation output:

```bash
gsutil mb gs://$PROJECT_ID-docs
```

4. Deploy to Cloud Run with necessary environment variables:

```bash
gcloud run deploy doc-builder \
  --image gcr.io/$PROJECT_ID/doc-builder \
  --platform managed \
  --memory 1Gi \
  --timeout 600s \
  --set-env-vars="CLOUD_RUN=1,GCS_BUCKET=$PROJECT_ID-docs" \
  --command="python" \
  --args="docs/build_docs.py,--cloud"
```

5. Set up a Cloud Build trigger for automatic documentation builds:

```bash
gcloud builds triggers create github \
  --repo=your-repo-name \
  --branch-pattern="main" \
  --build-config="docs/cloudbuild.yaml" \
  --description="Build documentation"
```

## Accessing Documentation

After a successful build, your documentation will be available:

- HTML docs: https://storage.googleapis.com/$PROJECT_ID-docs/html/index.html

## Dependencies

The documentation build uses the dependencies specified in the `[project.optional-dependencies]` section of the pyproject.toml file, specifically the `docs` extra. This ensures that all documentation tools are consistently defined in a single place.