#!/usr/bin/env python3
"""Build documentation for DE Container Tools in various formats.

This script builds the Sphinx documentation in HTML and PDF formats.

Optional environment variables:
- GCS_BUCKET: The Cloud Storage bucket to upload built documentation to (for Cloud Run)
"""

import os
import subprocess
import sys
from pathlib import Path

# Import the environment loader
try:
    from load_env import load_env_file

    load_env_file()
except ImportError:
    print("Environment loader not found. Continuing without loading .env file.")

# Add a flag to detect if running in container
is_container = os.environ.get("CONTAINER_ENV", "0") == "1"
is_cloud_run = os.environ.get("CLOUD_RUN", "0") == "1"


def build_html_docs():
    """Build HTML documentation."""
    try:
        docs_dir = Path(__file__).parent
        subprocess.run(["sphinx-build", "-b", "html", "source", "build/html"], cwd=docs_dir, check=True)
        print("HTML documentation built successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error building HTML documentation: {e}")
        return False


def build_pdf_docs():
    """Build PDF documentation using LaTeX."""
    try:
        docs_dir = Path(__file__).parent

        # First build latex files
        subprocess.run(["sphinx-build", "-b", "latex", "source", "build/latex"], cwd=docs_dir, check=True)

        # Check if system has pdflatex
        try:
            subprocess.run(["pdflatex", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            has_pdflatex = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            has_pdflatex = False

        if has_pdflatex:
            # Then compile PDF
            subprocess.run(["make"], cwd=docs_dir / "build" / "latex", check=True)
            print("PDF documentation built successfully")
            return True
        else:
            print("pdflatex not found. PDF generation requires LaTeX to be installed on your system.")
            print(
                "On Ubuntu/Debian systems, you can install it with: sudo apt-get install texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk"
            )
            print("On Windows, you can install MiKTeX (https://miktex.org/)")
            print("On macOS, you can install MacTeX (https://www.tug.org/mactex/)")
            return False
    except subprocess.CalledProcessError as e:
        print(f"Error building PDF documentation: {e}")
        return False


def upload_to_gcs():
    """Upload built documentation to Google Cloud Storage."""
    if not os.environ.get("GCS_BUCKET"):
        print("Warning: GCS_BUCKET environment variable not set, skipping upload")
        return False

    try:
        docs_dir = Path(__file__).parent
        bucket = os.environ.get("GCS_BUCKET")

        # Upload HTML documentation
        print(f"Uploading HTML documentation to gs://{bucket}/html/")
        subprocess.run(["gsutil", "-m", "cp", "-r", str(docs_dir / "build" / "html"), f"gs://{bucket}/"], check=True)

        print(f"Documentation uploaded to gs://{bucket}/html/")
        print(f"Documentation viewable at: https://storage.googleapis.com/{bucket}/html/index.html")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error uploading to GCS: {e}")
        return False


def main():
    """Run the main build and publish process."""
    print("Building DE Container Tools documentation...")

    # Parse command line arguments
    build_pdf = "--pdf" in sys.argv
    build_all = "--all" in sys.argv
    build_cloud = "--cloud" in sys.argv or is_cloud_run

    # If --all is specified, build everything (except in Cloud Run mode)
    if build_all and not build_cloud:
        build_pdf = True

    # If running in Cloud Run mode, we only build HTML
    if build_cloud:
        build_pdf = False
        print("Running in Cloud Run mode - building HTML documentation only")

    # Require at least one output format for non-cloud builds
    if not build_cloud and not build_pdf and not build_all:
        print("Error: You must specify at least one output format:")
        print("  --pdf         Generate PDF documentation")
        print("  --all         Generate all formats (HTML and PDF)")
        print("  --cloud       Run in Cloud Run mode (HTML only)")
        print("\nExample: python build_docs.py --pdf")
        sys.exit(1)

    # Build HTML docs (always do this)
    html_success = build_html_docs()

    # Build PDF docs if requested and not in Cloud Run mode
    pdf_success = None
    if build_pdf:
        print("\nGenerating PDF documentation...")
        pdf_success = build_pdf_docs()

    # Upload to GCS if in container/cloud mode
    gcs_success = None
    if build_cloud and html_success:
        print("\nUploading documentation to Google Cloud Storage...")
        gcs_success = upload_to_gcs()

    # Print summary
    print("\n=== Documentation Build Summary ===")

    if html_success:
        docs_dir = Path(__file__).parent
        html_path = docs_dir / "build" / "html" / "index.html"
        print(f"✓ HTML documentation: {html_path.absolute()}")
    else:
        print("✗ HTML documentation build failed")

    if pdf_success is not None:
        if pdf_success:
            docs_dir = Path(__file__).parent
            pdf_path = list(docs_dir.glob("build/latex/*.pdf"))
            if pdf_path:
                print(f"✓ PDF documentation: {pdf_path[0].absolute()}")
            else:
                print("✓ PDF files generated in latex directory, but final PDF not found")
        else:
            print("✗ PDF documentation build failed")

    if gcs_success is not None:
        if gcs_success:
            bucket = os.environ.get("GCS_BUCKET")
            print(f"✓ Documentation uploaded to GCS: gs://{bucket}/html/")
        else:
            print("✗ GCS upload failed")


if __name__ == "__main__":
    main()
