from setuptools import setup
from setuptools import find_packages

setup(
    name="helpers",
    version="0.0.1",
    author="Pulkit Chadha",
    author_email="",
    description="MLOps on Databricks and MLflow helpers",
    long_description_content_type="text/markdown",
    url="https://github.com/PulkitXChadha/mlops-on-databricks-mlflow.git",
    project_urls={
        "Bug Tracker": "https://github.com/PulkitXChadha/mlops-on-databricks-mlflow.git/issues",
    },
    install_requires=[],
    packages=find_packages(exclude=('tests*', 'testing*', 'docs')),
    entry_points={
        'console_scripts': [
            'post_comment_cli=helpers.mlflowAPIWrapper:postComment',
        ],
    }
)
