from setuptools import find_packages, setup

setup(
    name="duckpond",
    packages=find_packages(exclude=["duckpond_tests"]),
    install_requires=[
        "dagster",
        "duckdb",
        "pandas",
        "sqlescapy",
        "lxml",
        "html5lib"
    ],
    extras_require={"dev": ["dagit", "pytest", "localstack", "awscli", "awscli-local"]},
)
