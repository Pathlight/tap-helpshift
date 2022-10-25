#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-helpshift",
    version="1.2.32",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_helpshift"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "aiohttp",
        "python-dateutil",
    ],
    entry_points="""
    [console_scripts]
    tap-helpshift=tap_helpshift:main
    """,
    packages=["tap_helpshift"],
    package_data = {
        "schemas": ["tap_helpshift/schemas/*.json"]
    },
    include_package_data=True,
)
