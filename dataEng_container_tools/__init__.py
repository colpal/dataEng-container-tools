#python3 setup.py sdist bdist_wheel
from .safe_stdout import setup_stdout
from .cla import default_gcs_secret_location

setup_stdout(default_gcs_secret_location)