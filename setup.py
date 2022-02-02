from setuptools import setup

setup(
    name="dpa",
    version="1.0",
    author="alfredo.leon",
    description="This is the project for Udacity's Data Engineering Nanodegree course on Data Pipelines with Airflow.",
    install_requires=[
        "pandas",
        "jupyter",
        "psycopg2-binary",
        "tqdm",
        "apache-airflow==1.10.3",
        "boto3",
        "awscli",
        "WTForms==2.3.3",
        "marshmallow==2.21.0"
    ]
)