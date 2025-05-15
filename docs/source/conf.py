"""Sphinx configuration for DE Container Tools documentation."""

import os
import sys
from datetime import date

# Add the project root to the path so we can import the package
sys.path.insert(0, os.path.abspath("../.."))

# Import package to get version info
import dataeng_container_tools

# Project information
project = "DE Container Tools"
copyright = f"{date.today().year}, Colgate-Palmolive"
author = "CP DE Team"
version = dataeng_container_tools.__version__
release = version

# General configuration
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx_autodoc_typehints",
    "myst_parser",
    "sphinx_copybutton",
]

# Confluence extension - only add if we're building for confluence
if os.environ.get("SPHINX_BUILDER") == "confluence":
    extensions.append("sphinxcontrib.confluencebuilder")
    confluence_publish = True
    confluence_space_name = os.environ.get("CONFLUENCE_SPACE", "DE")
    confluence_parent_page = os.environ.get("CONFLUENCE_PARENT", "Data Engineering Tools")
    confluence_server_url = os.environ.get("CONFLUENCE_URL", "https://your-confluence-url.atlassian.net/wiki")
    confluence_server_user = os.environ.get("CONFLUENCE_USER")
    confluence_server_pass = os.environ.get("CONFLUENCE_API_KEY")
    confluence_page_hierarchy = True
    confluence_publish_timeout = 30

templates_path = ["_templates"]
exclude_patterns = []
source_suffix = [".rst", ".md"]
master_doc = "index"

# HTML output configuration
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_title = f"DE Container Tools {version}"
html_logo = None  # Add path to logo if you have one
html_theme_options = {
    "navigation_depth": 4,
    "titles_only": False,
    "logo_only": True,
}

# Extension configuration
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
    "show-inheritance": True,
}

# Napoleon settings for Google-style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True

# Intersphinx mapping
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
}

# Copybutton configuration
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

# MyST configuration for markdown support
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_heading_anchors = 3
