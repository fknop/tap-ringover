#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-ringover",
    version="0.1.0",
    description="Singer.io tap for extracting data from ringover",
    author="Florian Knop",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_ringover"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.9.0",
        "requests==2.24.0",
        "pendulum==2.1.2"
    ],
    entry_points="""
    [console_scripts]
    tap-ringover=tap_ringover:main
    """,
    packages=["tap_ringover"],
    package_data = {
        "schemas": ["tap_ringover/schemas/*.json"]
    },
    include_package_data=True,
)
