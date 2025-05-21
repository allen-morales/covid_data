from setuptools import find_packages, setup

setup(
    name="dagster_covid_data",
    packages=find_packages(exclude=["covid_data_tests"]),
    install_requires=[
        "dagster", "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
