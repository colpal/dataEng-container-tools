Contributing
===========

We welcome contributions to the DE Container Tools project! This page describes how to set up your development environment and submit changes.

Development Environment Setup
---------------------------

1. Clone the repository and set up a development environment:

   .. code-block:: bash

       git clone https://github.com/colpal/dataEng-container-tools.git
       cd dataEng-container-tools
       python -m venv .venv
       source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
       pip install -e ".[tests,docs]"

2. Run tests to verify your setup:

   .. code-block:: bash

       python -m unittest discover -s tests

Code Style Guidelines
------------------

This project uses ruff for linting and code quality enforcement:

1. Before submitting code, run ruff:

   .. code-block:: bash

       ruff check .

2. Follow these coding standards:
   
   - Use Google-style docstrings
   - Keep line length to 120 characters
   - Use type hints for all function signatures
   - Follow PEP 8 guidelines
   - Write comprehensive unit tests for new functionality

Making Changes
------------

1. Create a new branch for your changes:

   .. code-block:: bash

       git checkout -b feature/your-feature-name

2. Make your changes and ensure all tests pass
3. Update documentation if needed
4. Commit your changes with clear, descriptive commit messages

Submitting Changes
---------------

1. Push your changes to your branch:

   .. code-block:: bash

       git push origin feature/your-feature-name

2. Open a Pull Request against the appropriate branch
   - For new features or non-critical fixes, PR against `develop`
   - Follow the MST branching model described in the GitHub workflows

3. Ensure CI checks pass on your PR
4. Wait for code review

Documentation Guidelines
---------------------

When adding new features or changing existing features, please update the documentation:

1. Update or add docstrings to all public functions and classes
2. For significant changes, update the relevant usage example files
3. Add new features to the appropriate API documentation file
4. Update the changelog with your changes

When documenting code, follow this format:

.. code-block:: python

    def example_function(param1, param2=None):
        """Short description of what the function does.
        
        Longer description with more details if needed.
        
        Args:
            param1: Description of the first parameter.
            param2: Description of the second parameter. Defaults to None.
            
        Returns:
            Description of what the function returns.
            
        Raises:
            ExceptionType: When and why this exception is raised.
        """
        # Function implementation

Building Documentation
-------------------

To build the documentation locally:

.. code-block:: bash

    cd docs
    sphinx-build -b html source build/html

The documentation will be available in the `build/html` directory.