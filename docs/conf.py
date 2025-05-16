"""Sphinx configuration for DE Container Tools documentation."""

import datetime

import dataeng_container_tools

# Project information
project = "DE Container Tools"
copyright = f"{datetime.datetime.now(tz=datetime.timezone.utc).year}, Colgate-Palmolive"  # noqa: A001
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
    "sphinx_togglebutton",
    "sphinx_tabs.tabs",
]

templates_path = ["_templates"]
exclude_patterns = []
source_suffix = [".rst", ".md"]
master_doc = "index"

# HTML output configuration
html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_title = f"DE Container Tools {version}"
html_logo = "_static/logo-light.svg"
html_favicon = "_static/favicon.ico"
html_theme_options = {
    "navigation_depth": 4,

    "logo": {
        "image_dark": "_static/logo-dark.svg",
    },
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
