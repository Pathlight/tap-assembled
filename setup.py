#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-assembled",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Assembled API",
    author="Pathlight",
    url="http://pathlight.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_assembled"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "tap-framework==0.0.4",
    ],
    entry_points="""
    [console_scripts]
    tap-assembled=tap_assembled:main
    """,
    packages=find_packages(),
    package_data={"schemas": ["tap_assembled/schemas/*.json"]},
    include_package_data=True,
)
