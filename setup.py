# python3 -m build
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dataEng-container-tools",
    version="0.6.4",
    maintainer="CP DE Team",
    maintainer_email="git_data_science_engineering@colpal.com",
    description="A package containing tools for data engineering containers.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/colpal/dataEng-container-tools",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas',
        'google-cloud-storage',
        'google-cloud-bigquery',
        'google-cloud-datastore',
        'openpyxl',
        'pyarrow'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
