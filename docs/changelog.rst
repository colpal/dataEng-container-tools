Changelog
=========

This page tracks the major changes to the DE Container Tools package.

Version 1.0.0
-----------

Released: 2025-05-09

Major Changes
~~~~~~~~~~~~

- Removed BQ code and modules
- Added log utilities
- Added ``secret_manager.py``
  - Handles secret data for the entire tools library (passing relevant information to ``cla.py`` and ``safe_textio.py``)
- Added ``parse_known_args`` arg to CLA
- Refactored naming, function signatures, and more:
  - ``safe_stdout.py`` -> ``safe_textio.py``
    - Class: ``safe_stdout`` -> ``SafeTextIO`` (now accepts any TextIO)
    - Function: ``sys.stdout.add_words`` (instancemethod) -> ``SafeTextIO.add_words`` (classmethod)
    - Function: ``setup_stdout`` -> ``add_secrets_folder``
    - Function: ``setup_default_stdout`` -> ``setup_default_stdio`` (no longer parses default_secret_folder)
    - Functions now support both ``Path`` and ``str``
  - ``cla.py``
    - Class: ``custom_command_line_argument`` -> ``CustomCommandLineArgument`` (takes all of argparse.add_argument args)
    - Class: ``command_line_argument_type`` -> ``CommandLineArgumentType``
    - Class: ``command_line_secret`` -> ``CommandLineSecret``
    - Class: ``command_line_arguments`` -> ``CommandLineArguments``
    - ``CommandLineArguments.get_secret_locations`` now returns a ``list[Path]`` instead of ``list[str]`` when secret_locations is not present
    - Removed unused methods and parameters
  - ``gcs.py``
    - Class: ``gcs_file_io`` -> ``GCSFileIO``/``GCS``
    - Functions now support both ``Path`` and ``str``
    - Function: ``gcs_file_io.__get_parts`` (instancemethod) -> ``GCSFileIO.__get_parts`` (staticmethod)
  - ``db.py``
    - Class: ``Db`` -> ``DB``

Bug Fixes
~~~~~~~~

- Fixed several bugs regarding how ``gcs.py`` functions handle local vs non-local (return types, unsafe writes, etc.)