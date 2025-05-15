#!/usr/bin/env python3
"""Build documentation for DE Container Tools in various formats.

This script builds the Sphinx documentation in HTML and PDF formats.
"""

import os
import subprocess
import sys
from pathlib import Path

# Add a flag to detect if running in container
is_container = os.environ.get("CONTAINER_ENV", "0") == "1"


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


def main():
    """Run the main build and publish process."""
    print("Building DE Container Tools documentation...")

    # Parse command line arguments
    build_pdf = "--pdf" in sys.argv
    build_all = "--all" in sys.argv
    build_html = "--html" in sys.argv or not (build_pdf or build_all)  # Default to HTML if no flags

    # If --all is specified, build everything
    if build_all:
        build_pdf = True
        build_html = True

    # Require at least one output format
    if not build_html and not build_pdf and not build_all:
        print("Error: You must specify at least one output format:")
        print("  --html        Generate HTML documentation (default)")
        print("  --pdf         Generate PDF documentation")
        print("  --all         Generate all formats (HTML and PDF)")
        print("\nExample: python build_docs.py --pdf")
        sys.exit(1)

    # Build HTML docs if requested
    html_success = None
    if build_html:
        html_success = build_html_docs()

    # Build PDF docs if requested
    pdf_success = None
    if build_pdf:
        print("\nGenerating PDF documentation...")
        pdf_success = build_pdf_docs()

    # Print summary
    print("\n=== Documentation Build Summary ===")

    if html_success is not None:
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


if __name__ == "__main__":
    main()
